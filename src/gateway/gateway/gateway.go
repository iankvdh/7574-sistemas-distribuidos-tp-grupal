package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/heartbeat"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/clientregistry"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/clientstore"
	gatewayconfig "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/messagehandler"
)

const (
	queryResultEOF = "EOF"
)

type Gateway struct {
	registry              *clientregistry.ClientRegistry
	txPublishers          *gatewayExchangePool
	acctPublishers        *gatewayExchangePool
	nTxShards             int
	nAcctShards           int
	finalQueue            middleware.Middleware
	gatewayID             inner.GatewayID
	maxExternalBatchBytes int
	expectedQueryEOFs     int
	listener              net.Listener
	ctx                   context.Context
	cancel                context.CancelFunc
	waitingGroup          sync.WaitGroup

	clientAborts middleware.Middleware
	dataDir      string
	clientsMu    sync.Mutex
	clients      map[inner.ClientID]struct{}

	heartbeatEnabled  bool
	containerName     string
	sentinelUDPAddrs  []string
	heartbeatInterval time.Duration
	heartbeatJitter   time.Duration
}

func NewGateway(config gatewayconfig.GatewayConfig) (*Gateway, error) {
	connSettings, err := middleware.NewConnSettings(config.MomHost, config.MomPort)
	if err != nil {
		return nil, err
	}

	txPublishers, err := newGatewayExchangePool(config.AllTransactionsExchange, shardKeys(config.NTxShards), connSettings, config.PublisherPoolSize)
	if err != nil {
		return nil, err
	}

	acctPublishers, err := newGatewayExchangePool(config.AllAccountsExchange, shardKeys(config.NAccountShards), connSettings, config.PublisherPoolSize)
	if err != nil {
		_ = txPublishers.Close()
		return nil, err
	}

	if err := declareShardQueues(config.AllTransactionsExchange, config.NTxShards, connSettings); err != nil {
		_ = txPublishers.Close()
		_ = acctPublishers.Close()
		return nil, err
	}
	if err := declareShardQueues(config.AllAccountsExchange, config.NAccountShards, connSettings); err != nil {
		_ = txPublishers.Close()
		_ = acctPublishers.Close()
		return nil, err
	}

	gatewayID := inner.GatewayID(config.GatewayID)

	finalQueueName := fmt.Sprintf("%s_%d", config.FinalQueue, gatewayID)
	finalQueue, err := middleware.CreateQueueMiddleware(finalQueueName, connSettings)
	if err != nil {
		_ = txPublishers.Close()
		_ = acctPublishers.Close()
		return nil, err
	}

	clientAborts, err := middleware.CreateBestEffortExchangeMiddleware(config.ClientAbortsExchange, []string{""}, connSettings)
	if err != nil {
		_ = txPublishers.Close()
		_ = acctPublishers.Close()
		_ = finalQueue.Close()
		return nil, err
	}

	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		_ = txPublishers.Close()
		_ = acctPublishers.Close()
		_ = finalQueue.Close()
		_ = clientAborts.Close()
		return nil, fmt.Errorf("create gateway data dir %q: %w", config.DataDir, err)
	}
	clients, err := clientstore.Load(config.DataDir)
	if err != nil {
		_ = txPublishers.Close()
		_ = acctPublishers.Close()
		_ = finalQueue.Close()
		_ = clientAborts.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		_ = txPublishers.Close()
		_ = acctPublishers.Close()
		_ = finalQueue.Close()
		_ = clientAborts.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		registry:              clientregistry.NewClientRegistry(),
		txPublishers:          txPublishers,
		acctPublishers:        acctPublishers,
		nTxShards:             config.NTxShards,
		nAcctShards:           config.NAccountShards,
		finalQueue:            finalQueue,
		gatewayID:             gatewayID,
		maxExternalBatchBytes: config.MaxExternalBatchBytes,
		expectedQueryEOFs:     config.ExpectedQueryEOFs,
		listener:              listener,
		ctx:                   ctx,
		cancel:                cancel,

		clientAborts: clientAborts,
		dataDir:      config.DataDir,
		clients:      clients,

		heartbeatEnabled:  config.HeartbeatEnabled,
		containerName:     config.ContainerName,
		sentinelUDPAddrs:  config.SentinelUDPAddrs,
		heartbeatInterval: config.HeartbeatInterval,
		heartbeatJitter:   config.HeartbeatJitter,
	}

	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.waitingGroup.Wait()
	defer gateway.cancel()

	gateway.waitingGroup.Add(1)
	go gateway.shutdownWatcher()

	gateway.waitingGroup.Add(1)
	go gateway.consumeFinalQueue()

	gateway.waitingGroup.Add(1)
	go gateway.handleSignals()

	gateway.startHeartbeat()

	if err := gateway.abortPreviousClients(); err != nil {
		slog.Error("Failed to abort previously active clients on revival; will retry next restart", "err", err)
	}

	slog.Info("Gateway accepting client connections")

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if gateway.ctx.Err() != nil {
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

	return nil
}

func (gateway *Gateway) startHeartbeat() {
	if !gateway.heartbeatEnabled || len(gateway.sentinelUDPAddrs) == 0 {
		slog.Debug("Gateway heartbeat disabled")
		return
	}
	emitter, err := heartbeat.New(gateway.containerName, gateway.sentinelUDPAddrs,
		gateway.heartbeatInterval, gateway.heartbeatJitter)
	if err != nil {
		slog.Error("Could not initialize gateway heartbeat emitter", "err", err)
		return
	}
	gateway.waitingGroup.Add(1)
	go func() {
		defer gateway.waitingGroup.Done()
		emitter.Run(gateway.ctx)
	}()
}

func (gateway *Gateway) handleSignals() {
	defer gateway.waitingGroup.Done()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		slog.Info("Signal received, shutting down gateway", "signal", sig.String())
		gateway.cancel()
	case <-gateway.ctx.Done():
	}
}

func (gateway *Gateway) consumeFinalQueue() {
	defer gateway.waitingGroup.Done()

	err := gateway.finalQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		gateway.forwardFinalMessage(msg, ack, nack)
	})
	if err != nil && gateway.ctx.Err() == nil {
		slog.Error("Final queue consumer stopped unexpectedly", "err", err)
		gateway.cancel()
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
		gateway.cancel()
		return
	}

	state, ok := gateway.registry.Get(batch.ClientID)
	if !ok {
		slog.Warn("Received FINAL batch for unknown client", "client_id", batch.ClientID)
		ack()
		return
	}

	if state.IsDuplicate(batch.IDSpace, batch.SeqID) {
		slog.Debug("Dropping duplicate result batch",
			"client_id", batch.ClientID,
			"seq_id", batch.SeqID,
			"id_space", batch.IDSpace,
		)
		ack()
		return
	}
	state.MarkReceived(batch.IDSpace, batch.SeqID)

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
	defer gateway.cleanupClientSession(state, handler.ClientID())

	if err := gateway.handleHandshake(state, handler.ClientID()); err != nil {
		slog.Debug("Client handshake failed", "client_id", handler.ClientID(), "err", err)
		return
	}

	if err := gateway.registerClient(handler.ClientID()); err != nil {
		slog.Error("Failed to persist client registration", "client_id", handler.ClientID(), "err", err)
	}

	slog.Info("Client connected", "client_id", handler.ClientID(), "remote_addr", state.Conn.RemoteAddr())
	defer slog.Info("Client disconnected", "client_id", handler.ClientID())

	gateway.waitingGroup.Add(1)
	go gateway.handleClientResultOutput(state, handler.ClientID())

	for {
		msgType, err := external.ReadMsgType(state.Conn)
		if err != nil {
			if gateway.ctx.Err() == nil {
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

	builder := newResultBatchBuilder(state, gateway.maxExternalBatchBytes)
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
			if state.HasPendingResults() {
				continue
			}
		}

		eofs, err := builder.flush()
		if err != nil {
			if gateway.ctx.Err() == nil {
				slog.Warn("While flushing result batch to client", "client_id", clientID, "err", err)
			}
			gateway.registry.RemoveAndClose(clientID)
			return
		}

		eofsSent += eofs
		if eofsSent >= gateway.expectedQueryEOFs {
			slog.Info("All query EOFs delivered to client, closing session",
				"client_id", clientID, "expected_query_eofs", gateway.expectedQueryEOFs)
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

func (gateway *Gateway) cleanupClientSession(state *clientregistry.ClientState, clientID inner.ClientID) {
	if gateway.ctx.Err() == nil && gateway.isClientRegistered(clientID) {
		if err := gateway.publishClientAborted(clientID); err != nil {
			slog.Error("Failed to publish ClientAborted; client state will be reclaimed on revival",
				"client_id", clientID, "err", err)
		} else if err := gateway.unregisterClient(clientID); err != nil {
			slog.Error("Failed to persist client deregistration", "client_id", clientID, "err", err)
		}
	}
	gateway.registry.Remove(clientID)
	state.Close()
}

func (gateway *Gateway) registerClient(clientID inner.ClientID) error {
	gateway.clientsMu.Lock()
	defer gateway.clientsMu.Unlock()
	gateway.clients[clientID] = struct{}{}
	return clientstore.Save(gateway.dataDir, gateway.clients)
}

func (gateway *Gateway) unregisterClient(clientID inner.ClientID) error {
	gateway.clientsMu.Lock()
	defer gateway.clientsMu.Unlock()
	delete(gateway.clients, clientID)
	return clientstore.Save(gateway.dataDir, gateway.clients)
}

func (gateway *Gateway) isClientRegistered(clientID inner.ClientID) bool {
	gateway.clientsMu.Lock()
	defer gateway.clientsMu.Unlock()
	_, ok := gateway.clients[clientID]
	return ok
}

func (gateway *Gateway) publishClientAborted(clientID inner.ClientID) error {
	msg, err := serializeClientAborted(gateway.gatewayID, clientID)
	if err != nil {
		return err
	}
	if err := gateway.clientAborts.SendWithKey(*msg, ""); err != nil {
		return err
	}
	return gateway.clientAborts.FlushPublisher()
}

func (gateway *Gateway) abortPreviousClients() error {
	gateway.clientsMu.Lock()
	defer gateway.clientsMu.Unlock()
	if len(gateway.clients) == 0 {
		return nil
	}
	for clientID := range gateway.clients {
		if err := gateway.publishClientAborted(clientID); err != nil {
			return fmt.Errorf("revival abort for client %s: %w", clientID, err)
		}
	}
	slog.Info("Revival: aborted previously active clients", "count", len(gateway.clients))
	gateway.clients = map[inner.ClientID]struct{}{}
	return clientstore.Save(gateway.dataDir, gateway.clients)
}

func serializeClientAborted(gatewayID inner.GatewayID, clientID inner.ClientID) (*middleware.Message, error) {
	aborted := &inner.ClientAbortedMessage{
		Header: inner.Header{
			GatewayID:       gatewayID,
			ClientID:        clientID,
			SeqID:           0,
			SenderStageType: inner.StageGateway,
			MinterStageType: inner.StageGateway,
		},
	}
	raw, err := aborted.Serialize()
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(raw)}, nil
}

func shardKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return keys
}

func declareShardQueues(exchange string, n int, connSettings middleware.ConnSettings) error {
	for i := 0; i < n; i++ {
		queueName := fmt.Sprintf("%s_%d", exchange, i)
		bound, err := middleware.CreateBoundQueueMiddleware(queueName, exchange, strconv.Itoa(i), connSettings)
		if err != nil {
			return fmt.Errorf("declare shard queue %s: %w", queueName, err)
		}
		_ = bound.Close()
	}
	return nil
}

func (gateway *Gateway) sendShardedAndAck(state *clientregistry.ClientState, msg *middleware.Message, exchange middleware.Middleware, routingKey string) error {
	if msg != nil {
		if err := exchange.SendWithKey(*msg, routingKey); err != nil {
			return err
		}
		if err := exchange.FlushPublisher(); err != nil {
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
	routingKey := ""
	if msg != nil {
		routingKey, err = messagehandler.TxShardForBatch(batch, gateway.nTxShards)
		if err != nil {
			return err
		}
	}
	return gateway.sendShardedAndAck(state, msg, gateway.txPublishers.ForClient(handler.ClientID()), routingKey)
}

func (gateway *Gateway) handleEndOfTransactions(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	msg, err := handler.SerializeTransactionEOFMessage()
	if err != nil {
		return err
	}
	return gateway.sendShardedAndAck(state, msg, gateway.txPublishers.ForClient(handler.ClientID()), "0")
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
	routingKey := ""
	if msg != nil {
		routingKey, err = messagehandler.AcctShardForBatch(batch, gateway.nAcctShards)
		if err != nil {
			return err
		}
	}
	return gateway.sendShardedAndAck(state, msg, gateway.acctPublishers.ForClient(handler.ClientID()), routingKey)
}

func (gateway *Gateway) handleEndOfAccounts(state *clientregistry.ClientState, handler *messagehandler.MessageHandler) error {
	msg, err := handler.SerializeAccountEOFMessage()
	if err != nil {
		return err
	}
	return gateway.sendShardedAndAck(state, msg, gateway.acctPublishers.ForClient(handler.ClientID()), "0")
}

func (gateway *Gateway) shutdownWatcher() {
	defer gateway.waitingGroup.Done()
	<-gateway.ctx.Done()

	_ = gateway.listener.Close()
	gateway.registry.CloseAll()

	_ = gateway.finalQueue.StopConsuming()
	_ = gateway.finalQueue.Close()
	_ = gateway.txPublishers.Close()
	_ = gateway.acctPublishers.Close()
	_ = gateway.clientAborts.Close()
}
