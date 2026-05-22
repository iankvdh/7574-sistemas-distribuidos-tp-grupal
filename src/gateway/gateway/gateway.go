package gateway

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/google/uuid"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/clientregistry"
	gatewayconfig "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/messagehandler"
)

const (
	requiredQueryEOFs = 5
	queryResultEOF    = "EOF"
)

type Gateway struct {
	registry             clientregistry.ClientRegistry
	allTransactionsQueue middleware.Middleware
	allAccountsQueue     middleware.Middleware
	finalQueue           middleware.Middleware
	gatewayID            inner.GatewayID
	resultBatchMaxBytes  int
	listener             net.Listener
	running              atomic.Bool
	shutdownOnce         sync.Once
	shutdownCh           chan struct{}
	waitingGroup         sync.WaitGroup
}

func NewGateway(config gatewayconfig.GatewayConfig) (*Gateway, error) {
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

	gatewayID := inner.GatewayID(config.GatewayID)

	finalQueueName := fmt.Sprintf("%s_%d", config.FinalQueue, gatewayID)
	finalQueue, err := middleware.CreateQueueMiddleware(finalQueueName, connSettings)
	if err != nil {
		_ = allTransactionsQueue.Close()
		_ = allAccountsQueue.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		_ = allTransactionsQueue.Close()
		_ = allAccountsQueue.Close()
		_ = finalQueue.Close()
		return nil, err
	}

	gateway := &Gateway{
		allTransactionsQueue: allTransactionsQueue,
		allAccountsQueue:     allAccountsQueue,
		finalQueue:           finalQueue,
		gatewayID:            gatewayID,
		resultBatchMaxBytes:  config.ResultBatchMaxBytes,
		listener:             listener,
		shutdownCh:           make(chan struct{}),
	}
	gateway.running.Store(true)

	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.shutdown()

	gateway.waitingGroup.Add(1)
	go gateway.consumeFinalQueue()

	gateway.waitingGroup.Add(1)
	go gateway.handleSignals()

	slog.Info("Gateway accepting client connections")

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		// TODO: implement idempotency/recovery strategy for reconnecting clients.
		clientID := inner.ClientID(uuid.NewString())
		handler := messagehandler.NewMessageHandler(gateway.gatewayID, clientID)
		state := gateway.registry.Add(clientID, conn)

		gateway.waitingGroup.Add(1)
		go gateway.handleClientSession(state, &handler)
	}

	gateway.waitingGroup.Wait()
	return nil
}

func (gateway *Gateway) handleSignals() {
	defer gateway.waitingGroup.Done()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		slog.Info("Signal received, shutting down gateway", "signal", sig.String())
		gateway.shutdown()
	case <-gateway.shutdownCh:
	}
}

func (gateway *Gateway) consumeFinalQueue() {
	defer gateway.waitingGroup.Done()

	err := gateway.finalQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		gateway.forwardFinalMessage(msg, ack, nack)
	})
	if err != nil && gateway.running.Load() {
		slog.Error("Final queue consumer stopped unexpectedly", "err", err)
		gateway.shutdown()
	}
}

func (gateway *Gateway) forwardFinalMessage(msg middleware.Message, ack func(), nack func()) {
	batch, err := messagehandler.DeserializeFinalBatch(&msg)
	if err != nil {
		slog.Error("Invalid message from FINAL queue", "err", err)
		ack()
		return
	}

	if batch.GatewayID != gateway.gatewayID {
		slog.Error(
			"Received FINAL batch addressed to another gateway — this is a routing bug, shutting down",
			"expected_gateway_id", gateway.gatewayID,
			"received_gateway_id", batch.GatewayID,
			"client_id", batch.ClientID,
		)
		ack()
		gateway.shutdown()
		return
	}

	state, ok := gateway.registry.Get(batch.ClientID)
	if !ok {
		slog.Warn("Received FINAL batch for unknown client", "client_id", batch.ClientID)
		ack()
		return
	}

	remaining := &atomic.Int32{}
	remaining.Store(int32(len(batch.Items)))
	once := &sync.Once{}
	itemAck := func() {
		if remaining.Add(-1) == 0 {
			once.Do(ack)
		}
	}
	itemNack := func() { once.Do(nack) }

	for _, item := range batch.Items {
		enqueued := state.EnqueueResult(clientregistry.ResultDelivery{
			QueryID: item.QueryID,
			Data:    item.Data,
			Ack:     itemAck,
			Nack:    itemNack,
		})
		if !enqueued {
			slog.Warn("Dropping FINAL batch because client session is closed", "client_id", batch.ClientID)
			once.Do(ack)
			return
		}
	}
}

func (gateway *Gateway) handleClientSession(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) {
	defer gateway.waitingGroup.Done()
	defer gateway.registry.Remove(handler.ClientID())
	defer state.Close()

	if err := gateway.handleHandshake(state, handler.ClientID()); err != nil {
		slog.Debug("Client handshake failed", "client_id", handler.ClientID(), "err", err)
		return
	}

	gateway.waitingGroup.Add(1)
	go gateway.handleClientResultOutput(state, handler.ClientID())

	for {
		msgType, err := external.ReadMsgType(state.Conn)
		if err != nil {
			if gateway.running.Load() {
				slog.Debug("While reading client message type", "client_id", handler.ClientID(), "err", err)
			}
			return
		}

		switch msgType {
		case external.TransactionBatch:
			if err := gateway.handleTransactionBatch(state, handler); err != nil {
				slog.Debug("While handling transaction batch", "client_id", handler.ClientID(), "err", err)
				return
			}
		case external.EndOfTransactions:
			if err := gateway.handleEndOfTransactions(state, handler); err != nil {
				slog.Debug("While handling transactions EOF", "client_id", handler.ClientID(), "err", err)
				return
			}
		case external.AccountBatch:
			if err := gateway.handleAccountBatch(state, handler); err != nil {
				slog.Debug("While handling account batch", "client_id", handler.ClientID(), "err", err)
				return
			}
		case external.EndOfAccounts:
			if err := gateway.handleEndOfAccounts(state, handler); err != nil {
				slog.Debug("While handling accounts EOF", "client_id", handler.ClientID(), "err", err)
				return
			}
		case external.ResultBatchAck:
			if !state.NotifyResultBatchAck() {
				return
			}
		default:
			slog.Warn("Client sent unexpected message type", "client_id", handler.ClientID(), "msg_type", msgType)
			return
		}
	}
}

func (gateway *Gateway) handleClientResultOutput(state *clientregistry.ClientState, clientID inner.ClientID) {
	defer gateway.waitingGroup.Done()

	pendingDeliveries := make([]clientregistry.ResultDelivery, 0, 16)
	pendingItems := make([]external.ResultBatchItem, 0, 16)
	currentBatchBytes := external.ResultBatchHeaderBytes()
	eofCountInBatch := 0

	nackPending := func() {
		for _, pending := range pendingDeliveries {
			pending.Nack()
		}
		pendingDeliveries = pendingDeliveries[:0]
		pendingItems = pendingItems[:0]
		currentBatchBytes = external.ResultBatchHeaderBytes()
		eofCountInBatch = 0
	}

	flush := func() error {
		if len(pendingItems) == 0 {
			return nil
		}

		err := state.WriteWithLock(func(conn net.Conn) error {
			return external.WriteResultBatch(conn, pendingItems)
		})
		if err != nil {
			nackPending()
			return err
		}

		if !state.WaitForResultBatchAck() {
			nackPending()
			return errors.New("client session closed while waiting ResultBatchAck")
		}

		for _, pending := range pendingDeliveries {
			pending.Ack()
		}
		pendingDeliveries = pendingDeliveries[:0]
		pendingItems = pendingItems[:0]
		currentBatchBytes = external.ResultBatchHeaderBytes()
		eofCountInBatch = 0
		return nil
	}

	appendDelivery := func(delivery clientregistry.ResultDelivery) error {
		item := external.ResultBatchItem{
			QueryID: delivery.QueryID,
			Data:    delivery.Data,
		}
		itemBytes, err := external.ResultBatchItemSize(item)
		if err != nil {
			return err
		}
		if itemBytes+external.ResultBatchHeaderBytes() > gateway.resultBatchMaxBytes {
			return fmt.Errorf("result row exceeds RESULT_BATCH_MAX_BYTES: row=%d max=%d", itemBytes+external.ResultBatchHeaderBytes(), gateway.resultBatchMaxBytes)
		}

		if len(pendingItems) > 0 && currentBatchBytes+itemBytes > gateway.resultBatchMaxBytes {
			if err := flush(); err != nil {
				return err
			}
		}

		pendingDeliveries = append(pendingDeliveries, delivery)
		pendingItems = append(pendingItems, item)
		currentBatchBytes += itemBytes
		if delivery.Data == queryResultEOF {
			eofCountInBatch++
		}
		return nil
	}

	abortWithError := func(d clientregistry.ResultDelivery, err error) {
		slog.Error("While building result batch for client", "client_id", clientID, "err", err)
		d.Nack()
		nackPending()
		gateway.registry.RemoveAndClose(clientID)
	}

	eofsSent := 0

	for {
		delivery, ok := state.DequeueResult()
		if !ok {
			nackPending()
			return
		}

		if err := appendDelivery(delivery); err != nil {
			abortWithError(delivery, err)
			return
		}

		if delivery.Data != queryResultEOF {
			for currentBatchBytes < gateway.resultBatchMaxBytes {
				nextDelivery, exists := state.TryDequeueResult()
				if !exists {
					select {
					case <-state.Closed():
						nackPending()
						return
					default:
					}
					break
				}

				if err := appendDelivery(nextDelivery); err != nil {
					abortWithError(nextDelivery, err)
					return
				}

				if nextDelivery.Data == queryResultEOF {
					break
				}
			}
		}

		batchEOFs := eofCountInBatch
		if err := flush(); err != nil {
			if gateway.running.Load() {
				slog.Warn("While flushing result batch to client", "client_id", clientID, "err", err)
			}
			gateway.registry.RemoveAndClose(clientID)
			return
		}

		eofsSent += batchEOFs
		if eofsSent >= requiredQueryEOFs {
			slog.Info("All query EOFs delivered to client, closing session", "client_id", clientID)
			gateway.registry.RemoveAndClose(clientID)
			return
		}
	}
}

func (gateway *Gateway) handleHandshake(state *clientregistry.ClientState, clientID inner.ClientID) error {
	msgType, err := external.ReadMsgType(state.Conn)
	if err != nil {
		return err
	}
	if msgType != external.ConnectRequest {
		return errors.New("expected CONNECT_REQUEST")
	}

	return state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteConnectAck(conn, string(clientID))
	})
}

func (gateway *Gateway) sendToQueueAndAck(state *clientregistry.ClientState, msg *middleware.Message, queue middleware.Middleware) error {
	if msg != nil {
		if err := queue.Send(*msg); err != nil {
			return err
		}
	}
	return state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteIngestAck(conn)
	})
}

func (gateway *Gateway) handleTransactionBatch(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	batch, err := external.ReadTransactionBatch(state.Conn)
	if err != nil {
		return err
	}
	msg, err := handler.SerializeTransactionBatch(batch)
	if err != nil {
		return err
	}
	return gateway.sendToQueueAndAck(state, msg, gateway.allTransactionsQueue)
}

func (gateway *Gateway) handleEndOfTransactions(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	msg, err := handler.SerializeTransactionEOFMessage()
	if err != nil {
		return err
	}
	return gateway.sendToQueueAndAck(state, msg, gateway.allTransactionsQueue)
}

func (gateway *Gateway) handleAccountBatch(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	batch, err := external.ReadAccountBatch(state.Conn)
	if err != nil {
		return err
	}
	msg, err := handler.SerializeAccountBatch(batch)
	if err != nil {
		return err
	}
	return gateway.sendToQueueAndAck(state, msg, gateway.allAccountsQueue)
}

func (gateway *Gateway) handleEndOfAccounts(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	msg, err := handler.SerializeAccountEOFMessage()
	if err != nil {
		return err
	}
	return gateway.sendToQueueAndAck(state, msg, gateway.allAccountsQueue)
}

func (gateway *Gateway) shutdown() {
	gateway.shutdownOnce.Do(func() {
		gateway.running.Store(false)
		close(gateway.shutdownCh)

		_ = gateway.listener.Close()
		gateway.registry.CloseAll()

		_ = gateway.finalQueue.StopConsuming()
		_ = gateway.finalQueue.Close()
		_ = gateway.allTransactionsQueue.Close()
		_ = gateway.allAccountsQueue.Close()
	})
}
