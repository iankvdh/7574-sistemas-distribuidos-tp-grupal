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

type Gateway struct {
	registry             clientregistry.ClientRegistry
	allTransactionsQueue middleware.Middleware
	allAccountsQueue     middleware.Middleware
	finalQueue           middleware.Middleware
	gatewayID            inner.GatewayID
	listener             net.Listener
	running              atomic.Bool
	shutdownOnce         sync.Once
	shutdownCh           chan struct{}
	waiting_group        sync.WaitGroup
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
		listener:             listener,
		shutdownCh:           make(chan struct{}),
	}
	gateway.running.Store(true)

	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.shutdown()

	gateway.waiting_group.Add(1)
	go gateway.consumeFinalQueue()

	gateway.waiting_group.Add(1)
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

		gateway.waiting_group.Add(1)
		go gateway.handleClientSession(state, &handler)
	}

	gateway.waiting_group.Wait()
	return nil
}

func (gateway *Gateway) handleSignals() {
	defer gateway.waiting_group.Done()

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
	defer gateway.waiting_group.Done()

	err := gateway.finalQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		gateway.forwardFinalMessage(msg, ack, nack)
	})
	if err != nil && gateway.running.Load() {
		slog.Error("Final queue consumer stopped unexpectedly", "err", err)
		gateway.shutdown()
	}
}

func (gateway *Gateway) forwardFinalMessage(msg middleware.Message, ack func(), _ func()) {
	finalMsg, err := messagehandler.DeserializeFinalMessage(&msg)
	if err != nil {
		slog.Error("Invalid message from FINAL queue", "err", err)
		ack()
		return
	}

	state, ok := gateway.registry.Get(finalMsg.ClientID)
	if !ok {
		slog.Warn("Received FINAL message for unknown client", "client_id", finalMsg.ClientID)
		ack()
		return
	}

	err = state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteQueryResult(conn, finalMsg.QueryID, finalMsg.Status)
	})
	if err != nil {
		slog.Warn("Failed forwarding FINAL message to client", "client_id", finalMsg.ClientID, "err", err)
		gateway.registry.RemoveAndClose(finalMsg.ClientID)
		ack()
		return
	}

	ack()
}

func (gateway *Gateway) handleClientSession(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) {
	defer gateway.waiting_group.Done()
	defer gateway.registry.Remove(handler.ClientID())
	defer state.Conn.Close()

	if err := gateway.handleHandshake(state, handler.ClientID()); err != nil {
		slog.Debug("Client handshake failed", "client_id", handler.ClientID(), "err", err)
		return
	}

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
		default:
			slog.Warn("Client sent unexpected message type", "client_id", handler.ClientID(), "msg_type", msgType)
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

func (gateway *Gateway) handleTransactionBatch(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	batch, err := external.ReadTransactionBatch(state.Conn)
	if err != nil {
		return err
	}

	message, err := handler.SerializeTransactionBatchMessage(batch)
	if err != nil {
		return err
	}
	if err := gateway.allTransactionsQueue.Send(*message); err != nil {
		return err
	}

	return state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteAck(conn)
	})
}

func (gateway *Gateway) handleEndOfTransactions(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	message, err := handler.SerializeTransactionEOFMessage()
	if err != nil {
		return err
	}
	if err := gateway.allTransactionsQueue.Send(*message); err != nil {
		return err
	}

	return state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteAck(conn)
	})
}

func (gateway *Gateway) handleAccountBatch(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	batch, err := external.ReadAccountBatch(state.Conn)
	if err != nil {
		return err
	}

	message, err := handler.SerializeAccountBatchMessage(batch)
	if err != nil {
		return err
	}
	if err := gateway.allAccountsQueue.Send(*message); err != nil {
		return err
	}

	return state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteAck(conn)
	})
}

func (gateway *Gateway) handleEndOfAccounts(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	message, err := handler.SerializeAccountEOFMessage()
	if err != nil {
		return err
	}
	if err := gateway.allAccountsQueue.Send(*message); err != nil {
		return err
	}

	return state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteAck(conn)
	})
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
