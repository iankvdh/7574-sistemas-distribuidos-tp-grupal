package gateway

import (
	"context"
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
	registry             *clientregistry.ClientRegistry
	allTransactionsQueue middleware.Middleware
	allAccountsQueue     middleware.Middleware
	finalQueue           middleware.Middleware
	gatewayID            inner.GatewayID
	resultBatchMaxBytes  int
	listener             net.Listener
	running              atomic.Bool
	ctx                  context.Context
	cancel               context.CancelFunc
	shutdownOnce         sync.Once
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

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		registry:             clientregistry.NewClientRegistry(),
		allTransactionsQueue: allTransactionsQueue,
		allAccountsQueue:     allAccountsQueue,
		finalQueue:           finalQueue,
		gatewayID:            gatewayID,
		resultBatchMaxBytes:  config.ResultBatchMaxBytes,
		listener:             listener,
		ctx:                  ctx,
		cancel:               cancel,
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
		state := gateway.registry.Add(gateway.ctx, clientID, conn)

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
	case <-gateway.ctx.Done():
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
	// NOTA: NO BORRAR
	// At-least-once delivery: the AMQP ack fires only when all items in this
	// batch have been individually acked by the client via ResultBatchAck.
	// If any item fails (e.g. client disconnects mid-flush), the whole AMQP
	// message is nacked and the broker redelivers it — including items already
	// sent. Today this is harmless because the client session is gone by the
	// time the nack fires, and redeliveries for unknown clients are dropped.
	// If reconnection with stable clientIDs is ever implemented, this becomes
	// an at-least-once gap: the reconnecting client could receive duplicate
	// results. Fix at that point: either deduplicate on the client side using
	// a sequence number, or ack + re-publish only the undelivered items.
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

	builder := newResultBatchBuilder(state, gateway.resultBatchMaxBytes)
	eofsSent := 0

	for {
		delivery, ok := state.DequeueResult()
		if !ok {
			builder.nackPending()
			return
		}

		if err := builder.append(delivery); err != nil {
			slog.Error("While building result batch for client", "client_id", clientID, "err", err)
			delivery.Nack()
			builder.nackPending()
			gateway.registry.RemoveAndClose(clientID)
			return
		}

		if delivery.Data != queryResultEOF {
			continue
		}

		eofs, err := builder.flush()
		if err != nil {
			if gateway.running.Load() {
				slog.Warn("While flushing result batch to client", "client_id", clientID, "err", err)
			}
			gateway.registry.RemoveAndClose(clientID)
			return
		}

		eofsSent += eofs
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
		gateway.cancel()

		_ = gateway.listener.Close()
		gateway.registry.CloseAll()

		_ = gateway.finalQueue.StopConsuming()
		_ = gateway.finalQueue.Close()
		_ = gateway.allTransactionsQueue.Close()
		_ = gateway.allAccountsQueue.Close()
	})
}
