package serialanalyzer

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	analyzerconfig "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/serialanalyzer/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/serialanalyzer/session"
)

var ErrStatusTooLong = errors.New("query result status exceeds 255 bytes")

type SerialAnalyzer struct {
	allTransactionsQueue middleware.Middleware
	allAccountsQueue     middleware.Middleware
	finalQueuePrefix     string
	connSettings         middleware.ConnSettings
	finalQueues          map[string]middleware.Middleware
	processor            *session.Processor
	running              atomic.Bool
	shutdownOnce         sync.Once
	shutdownCh           chan struct{}
	waitingGroup         sync.WaitGroup
	mu                   sync.Mutex
}

func NewSerialAnalyzer(config analyzerconfig.SerialAnalyzerConfig) (*SerialAnalyzer, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	allTransactionsQueue, err := middleware.CreateQueueMiddleware(config.AllTransactionsQueue, connSettings)
	if err != nil {
		return nil, err
	}

	allAccountsQueue, err := middleware.CreateQueueMiddleware(config.AllAccountsQueue, connSettings)
	if err != nil {
		_ = allTransactionsQueue.Close()
		return nil, err
	}

	serialAnalyzer := &SerialAnalyzer{
		allTransactionsQueue: allTransactionsQueue,
		allAccountsQueue:     allAccountsQueue,
		finalQueuePrefix:     config.FinalQueue,
		connSettings:         connSettings,
		finalQueues:          make(map[string]middleware.Middleware),
		processor:            session.NewProcessor(),
		shutdownCh:           make(chan struct{}),
	}
	serialAnalyzer.running.Store(true)

	return serialAnalyzer, nil
}

func (serialAnalyzer *SerialAnalyzer) Run() error {
	defer serialAnalyzer.shutdown()

	serialAnalyzer.waitingGroup.Add(1)
	go serialAnalyzer.handleSignals()

	serialAnalyzer.waitingGroup.Add(1)
	go serialAnalyzer.consumeTransactionsQueue()

	serialAnalyzer.waitingGroup.Add(1)
	go serialAnalyzer.consumeAccountsQueue()

	serialAnalyzer.waitingGroup.Wait()
	return nil
}

func (serialAnalyzer *SerialAnalyzer) handleSignals() {
	defer serialAnalyzer.waitingGroup.Done()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		slog.Info("Signal received, shutting down serial analyzer", "signal", sig.String())
		serialAnalyzer.shutdown()
	case <-serialAnalyzer.shutdownCh:
	}
}

func (serialAnalyzer *SerialAnalyzer) consumeTransactionsQueue() {
	defer serialAnalyzer.waitingGroup.Done()

	err := serialAnalyzer.allTransactionsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		serialAnalyzer.handleTransactionsMessage(msg, ack, nack)
	})
	if err != nil && serialAnalyzer.running.Load() {
		slog.Error("Transactions queue consumer stopped unexpectedly", "err", err)
		serialAnalyzer.shutdown()
	}
}

func (serialAnalyzer *SerialAnalyzer) consumeAccountsQueue() {
	defer serialAnalyzer.waitingGroup.Done()

	err := serialAnalyzer.allAccountsQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		serialAnalyzer.handleAccountsMessage(msg, ack, nack)
	})
	if err != nil && serialAnalyzer.running.Load() {
		slog.Error("Accounts queue consumer stopped unexpectedly", "err", err)
		serialAnalyzer.shutdown()
	}
}

func (serialAnalyzer *SerialAnalyzer) handleTransactionsMessage(message middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeEnvelope(&message)
	if err != nil {
		slog.Error("Malformed transaction queue message", "err", err)
		ack()
		return
	}

	serialAnalyzer.mu.Lock()
	defer serialAnalyzer.mu.Unlock()

	var outputs []session.QueryOutput
	switch envelope.Kind {
	case inner.AllTransactionsBatch:
		batch, err := external.DeserializeTransactionBatchPayload(envelope.Payload)
		if err != nil {
			slog.Error("Invalid transactions batch payload", "client_id", envelope.ClientID, "err", err)
			ack()
			return
		}
		outputs = serialAnalyzer.processor.AddTransactions(envelope.GatewayID, envelope.ClientID, batch)
	case inner.AllTransactionsEOF:
		outputs = serialAnalyzer.processor.MarkTransactionsEOF(envelope.GatewayID, envelope.ClientID, envelope.Total)
	default:
		slog.Warn("Unexpected message kind in transactions queue", "kind", envelope.Kind)
		ack()
		return
	}

	if err := serialAnalyzer.publishOutputs(envelope.GatewayID, envelope.ClientID, outputs); err != nil {
		if errors.Is(err, ErrStatusTooLong) {
			slog.Error("Dropping oversized query result row", "client_id", envelope.ClientID, "err", err)
			ack()
			return
		}
		slog.Error("While publishing query outputs", "client_id", envelope.ClientID, "err", err)
		nack()
		return
	}

	ack()
}

func (serialAnalyzer *SerialAnalyzer) handleAccountsMessage(message middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeEnvelope(&message)
	if err != nil {
		slog.Error("Malformed accounts queue message", "err", err)
		ack()
		return
	}

	serialAnalyzer.mu.Lock()
	defer serialAnalyzer.mu.Unlock()

	var outputs []session.QueryOutput
	switch envelope.Kind {
	case inner.AllAccountsBatch:
		batch, err := external.DeserializeAccountBatchPayload(envelope.Payload)
		if err != nil {
			slog.Error("Invalid accounts batch payload", "client_id", envelope.ClientID, "err", err)
			ack()
			return
		}
		outputs = serialAnalyzer.processor.AddAccounts(envelope.GatewayID, envelope.ClientID, batch)
	case inner.AllAccountsEOF:
		outputs = serialAnalyzer.processor.MarkAccountsEOF(envelope.GatewayID, envelope.ClientID, envelope.Total)
	default:
		slog.Warn("Unexpected message kind in accounts queue", "kind", envelope.Kind)
		ack()
		return
	}

	if err := serialAnalyzer.publishOutputs(envelope.GatewayID, envelope.ClientID, outputs); err != nil {
		if errors.Is(err, ErrStatusTooLong) {
			slog.Error("Dropping oversized query result row", "client_id", envelope.ClientID, "err", err)
			ack()
			return
		}
		slog.Error("While publishing query outputs", "client_id", envelope.ClientID, "err", err)
		nack()
		return
	}

	ack()
}

func (serialAnalyzer *SerialAnalyzer) publishOutputs(gatewayID inner.GatewayID, clientID inner.ClientID, outputs []session.QueryOutput) error {
	finalQueue, err := serialAnalyzer.getFinalQueueForGateway(gatewayID)
	if err != nil {
		return err
	}

	for _, output := range outputs {
		if len(output.Status) > 255 {
			return ErrStatusTooLong
		}

		message, err := inner.SerializeFinalQueryResult(gatewayID, clientID, output.QueryID, output.Status)
		if err != nil {
			return err
		}
		if err := finalQueue.Send(*message); err != nil {
			return err
		}
	}
	return nil
}

func (serialAnalyzer *SerialAnalyzer) getFinalQueueForGateway(gatewayID inner.GatewayID) (middleware.Middleware, error) {
	if gatewayID == "" {
		return nil, errors.New("gateway id is empty")
	}
	queueName := fmt.Sprintf("%s_%s", serialAnalyzer.finalQueuePrefix, gatewayID)

	queue, exists := serialAnalyzer.finalQueues[queueName]
	if exists {
		return queue, nil
	}

	queue, err := middleware.CreateQueueMiddleware(queueName, serialAnalyzer.connSettings)
	if err != nil {
		return nil, err
	}
	serialAnalyzer.finalQueues[queueName] = queue
	return queue, nil
}

func (serialAnalyzer *SerialAnalyzer) shutdown() {
	serialAnalyzer.shutdownOnce.Do(func() {
		serialAnalyzer.running.Store(false)
		close(serialAnalyzer.shutdownCh)

		_ = serialAnalyzer.allTransactionsQueue.StopConsuming()
		_ = serialAnalyzer.allAccountsQueue.StopConsuming()

		_ = serialAnalyzer.allTransactionsQueue.Close()
		_ = serialAnalyzer.allAccountsQueue.Close()

		serialAnalyzer.mu.Lock()
		for queueName, queue := range serialAnalyzer.finalQueues {
			if err := queue.Close(); err != nil {
				slog.Warn("While closing final queue middleware", "queue", queueName, "err", err)
			}
		}
		serialAnalyzer.mu.Unlock()
	})
}
