package runtime

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/builder"
)

type Worker struct {
	cfg                 config.WorkerConfig
	strategy            strategy.Strategy
	input               middleware.Middleware
	ringIn              middleware.Middleware
	ringOut             middleware.Middleware
	outputs             []OutputTarget
	outputTargetBuffers []*outputTargetBuffer
	ctx                 context.Context
	cancel              context.CancelFunc
	waitingGroup        sync.WaitGroup
	strategyMu          sync.Mutex
	upstreamEOFMu       sync.Mutex

	// Stores last upstream EOF message for each client, to allow re-enqueuing if
	// the strategy requests it. Cleared on successful publish of EOFs.
	upstreamEOFs map[inner.ClientID]middleware.Message
}

type outputTargetBuffer struct {
	target   OutputTarget
	perShard []map[batchKey]*pendingBatch
}

type batchKey struct {
	clientID   inner.ClientID
	routingKey string
	itemKind   inner.MsgKind
	queryID    uint8
}

type pendingBatch struct {
	gatewayID  inner.GatewayID
	clientID   inner.ClientID
	routingKey string
	itemKind   inner.MsgKind
	queryID    uint8
	items      [][]byte
	bytes      int
}

func New(cfg config.WorkerConfig) (*Worker, error) {
	strat, err := builder.Build(cfg.StrategyName)
	if err != nil {
		return nil, err
	}

	conn := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	input, err := BuildInputMiddleware(cfg.InputConfig, conn)
	if err != nil {
		return nil, err
	}

	outputTargets, err := BuildOutputTargets(cfg.Outputs, conn)
	if err != nil {
		_ = input.Close()
		return nil, err
	}

	var ringIn, ringOut middleware.Middleware
	if cfg.RingQueueIn != "" {
		ringIn, err = middleware.CreateQueueMiddleware(cfg.RingQueueIn, conn)
		if err != nil {
			closeAll(input, nil, nil, outputTargets)
			return nil, err
		}
	}
	if cfg.RingQueueOut != "" {
		ringOut, err = middleware.CreateQueueMiddleware(cfg.RingQueueOut, conn)
		if err != nil {
			closeAll(input, ringIn, nil, outputTargets)
			return nil, err
		}
	}

	err = initStrategy(outputTargets, cfg, strat, input, ringIn, ringOut)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	buffers := initOutputTargetBuffers(outputTargets)
	w := &Worker{
		cfg:                 cfg,
		strategy:            strat,
		input:               input,
		ringIn:              ringIn,
		ringOut:             ringOut,
		outputs:             outputTargets,
		outputTargetBuffers: buffers,
		ctx:                 runCtx,
		cancel:              cancel,
		upstreamEOFs:        map[inner.ClientID]middleware.Message{},
	}
	return w, nil
}

func initStrategy(outputTargets []OutputTarget, cfg config.WorkerConfig, strat strategy.Strategy, input middleware.Middleware, ringIn middleware.Middleware, ringOut middleware.Middleware) error {
	strategyCfg := strategy.StrategyConfig{
		OutputCount:  len(outputTargets),
		ReplicaID:    cfg.ReplicaID,
		NReplicas:    cfg.NReplicas,
		StrategyName: cfg.StrategyName,
		MatchCount:   cfg.OutputsMatchCount,
		RingQueueIn:  cfg.RingQueueIn,
		RingQueueOut: cfg.RingQueueOut,
	}
	if err := strat.Init(strategyCfg); err != nil {
		closeAll(input, ringIn, ringOut, outputTargets)
		return err
	}
	return nil
}

func initOutputTargetBuffers(outputTargets []OutputTarget) []*outputTargetBuffer {
	buffers := make([]*outputTargetBuffer, len(outputTargets))
	for i, target := range outputTargets {
		shards := 1
		if target.Kind == config.KindShardedQueues {
			shards = target.ShardCount
		}
		perShard := make([]map[batchKey]*pendingBatch, shards)
		for s := range perShard {
			perShard[s] = map[batchKey]*pendingBatch{}
		}
		buffers[i] = &outputTargetBuffer{target: target, perShard: perShard}
	}
	return buffers
}

func (w *Worker) Run() error {
	defer w.waitingGroup.Wait()
	defer w.cancel()

	w.waitingGroup.Add(1)
	go w.shutdownWatcher()

	w.waitingGroup.Add(1)
	go w.handleSignals()

	slog.Info(
		"Worker started",
		"strategy", w.cfg.StrategyName,
		"replica_id", w.cfg.ReplicaID,
		"n_replicas", w.cfg.NReplicas,
		"input", w.cfg.Input,
		"outputs", len(w.outputs),
		"ring_in", w.cfg.RingQueueIn,
		"ring_out", w.cfg.RingQueueOut,
	)

	if w.ringIn != nil {
		w.waitingGroup.Add(1)
		go w.consumeRing()
	}

	err := w.input.StartConsuming(w.handleInputMessage)
	if err != nil && w.ctx.Err() == nil {
		slog.Error("Input consumer stopped unexpectedly", "err", err)
	}
	return nil
}

func (w *Worker) consumeRing() {
	defer w.waitingGroup.Done()
	err := w.ringIn.StartConsuming(w.handleRingMessage)
	if err != nil && w.ctx.Err() == nil {
		slog.Error("Ring consumer stopped unexpectedly", "err", err)
	}
}

func (w *Worker) handleInputMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeEnvelope(&msg)
	if err != nil {
		slog.Error("Malformed envelope in input queue", "err", err)
		ack()
		return
	}

	if isEOFKind(envelope.Kind) {
		w.cacheUpstreamEOF(envelope.ClientID, msg)
		w.strategyMu.Lock()
		outcome, err := w.strategy.OnUpstreamEOF(envelope)
		if err != nil {
			w.strategyMu.Unlock()
			slog.Error("Strategy OnUpstreamEOF failed", "client_id", envelope.ClientID, "err", err)
			nack()
			return
		}
		if err := w.applyEOFOutcome(envelope, outcome); err != nil {
			w.strategyMu.Unlock()
			slog.Error("Applying EOF outcome failed", "client_id", envelope.ClientID, "err", err)
			nack()
			return
		}
		w.strategyMu.Unlock()
		ack()
		return
	}

	w.strategyMu.Lock()
	if envelope.Kind == inner.InnerBatch {
		itemKind, items, err := inner.DeserializeInnerBatch(envelope)
		if err != nil {
			w.strategyMu.Unlock()
			slog.Error("Malformed InnerBatch envelope", "err", err)
			ack()
			return
		}
		if err := w.processBatchItems(envelope, itemKind, items); err != nil {
			w.strategyMu.Unlock()
			slog.Error("Strategy ProcessMessage failed", "client_id", envelope.ClientID, "err", err)
			nack()
			return
		}
	} else {
		if err := w.processSingleItem(envelope); err != nil {
			w.strategyMu.Unlock()
			slog.Error("Strategy ProcessMessage failed", "client_id", envelope.ClientID, "err", err)
			nack()
			return
		}
	}
	w.strategyMu.Unlock()
	ack()
}

func (w *Worker) processBatchItems(parent *inner.Envelope, itemKind inner.MsgKind, items [][]byte) error {
	for _, payload := range items {
		item := &inner.Envelope{
			Kind:      itemKind,
			GatewayID: parent.GatewayID,
			ClientID:  parent.ClientID,
			QueryID:   parent.QueryID,
			Payload:   payload,
		}
		if err := w.processSingleItem(item); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) processSingleItem(env *inner.Envelope) error {
	outputMessages, _, err := w.strategy.ProcessMessage(env)
	if err != nil {
		return err
	}
	return w.appendOutputMessages(env.GatewayID, outputMessages)
}

func (w *Worker) handleRingMessage(msg middleware.Message, ack func(), nack func()) {
	ringMessageEnvelope, err := inner.DeserializeEnvelope(&msg)
	if err != nil {
		slog.Error("Malformed envelope in ring queue", "err", err)
		ack()
		return
	}
	if ringMessageEnvelope.Kind != inner.RingTokenMessage {
		slog.Warn("Unexpected kind on ring queue", "kind", ringMessageEnvelope.Kind)
		ack()
		return
	}

	token, err := eof.UnmarshalToken(ringMessageEnvelope.Payload)
	if err != nil {
		slog.Error("Malformed ring token", "err", err)
		ack()
		return
	}

	w.strategyMu.Lock()
	outcome, err := w.strategy.OnRingToken(token)
	if err != nil {
		w.strategyMu.Unlock()
		slog.Error("Strategy OnRingToken failed", "client_id", token.ClientID, "err", err)
		nack()
		return
	}

	resultEnvelope := &inner.Envelope{GatewayID: ringMessageEnvelope.GatewayID, ClientID: token.ClientID}
	if err := w.applyEOFOutcome(resultEnvelope, outcome); err != nil {
		w.strategyMu.Unlock()
		slog.Error("Applying ring outcome failed", "client_id", token.ClientID, "err", err)
		nack()
		return
	}
	w.strategyMu.Unlock()
	ack()
}

func (w *Worker) applyEOFOutcome(env *inner.Envelope, outcome strategy.EOFOutcome) error {
	switch outcome.Action.Kind {
	case eof.ActionNone:
		return nil
	case eof.ActionForwardToken:
		if err := w.flushClient(env.ClientID); err != nil {
			return err
		}
		return w.forwardToken(env, outcome.Action.Token)
	case eof.ActionEmitEOFs:
		w.clearUpstreamEOF(env.ClientID)
		return w.publishEOFs(env, outcome.EOFs)
	case eof.ActionEmitEOFsAndForwardToken:
		if err := w.flushClient(env.ClientID); err != nil {
			return err
		}
		w.clearUpstreamEOF(env.ClientID)
		if err := w.publishEOFs(env, outcome.EOFs); err != nil {
			return err
		}
		return w.forwardToken(env, outcome.Action.Token)
	case eof.ActionReenqueueUpstreamEOF:
		return w.reenqueueUpstreamEOF(env.ClientID)
	default:
		return errors.New("strategy returned unknown EOF action kind")
	}
}

func (w *Worker) forwardToken(env *inner.Envelope, token *eof.Token) error {
	if w.ringOut == nil {
		return errors.New("strategy asked to forward a ring token but RING_QUEUE_OUT is not configured")
	}
	if token == nil {
		return errors.New("strategy asked to forward a nil ring token")
	}
	payload, err := eof.MarshalToken(token)
	if err != nil {
		return err
	}
	msg, err := inner.SerializeRingToken(env.GatewayID, token.ClientID, payload)
	if err != nil {
		return err
	}
	return w.ringOut.Send(*msg)
}

func (w *Worker) appendOutputMessages(gatewayID inner.GatewayID, outputMessages []strategy.OutputMessage) error {
	for _, om := range outputMessages {
		for _, idx := range om.OutputIndices {
			if idx < 0 || idx >= len(w.outputTargetBuffers) {
				return errors.New("strategy returned invalid output index")
			}
			if err := w.appendToShard(idx, gatewayID, om); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) appendToShard(idx int, gatewayID inner.GatewayID, om strategy.OutputMessage) error {
	buf := w.outputTargetBuffers[idx]
	shard := w.shardFor(buf.target, om.ClientID)
	itemKind := om.BatchItemKind
	if itemKind == 0 {
		itemKind = inner.TransactionMessage
	}
	key := batchKey{
		clientID:   om.ClientID,
		routingKey: om.RoutingKey,
		itemKind:   itemKind,
		queryID:    om.BatchQueryID,
	}
	batches := buf.perShard[shard]
	pending, ok := batches[key]
	if !ok {
		pending = &pendingBatch{
			gatewayID:  gatewayID,
			clientID:   om.ClientID,
			routingKey: om.RoutingKey,
			itemKind:   itemKind,
			queryID:    om.BatchQueryID,
		}
		batches[key] = pending
	}
	pending.items = append(pending.items, om.Body)
	pending.bytes += len(om.Body)
	if pending.bytes >= w.cfg.MaxInternalBatchBytes {
		return w.flushBatch(idx, shard, key)
	}
	return nil
}

func (w *Worker) flushBatch(idx, shard int, key batchKey) error {
	buf := w.outputTargetBuffers[idx]
	pending := buf.perShard[shard][key]
	if pending == nil || len(pending.items) == 0 {
		return nil
	}
	msg, err := inner.SerializeInnerBatch(pending.queryID, pending.itemKind, pending.gatewayID, pending.clientID, pending.items)
	if err != nil {
		return err
	}
	if err := sendOnTarget(buf.target.Middlewares[shard], *msg, pending.routingKey); err != nil {
		return err
	}
	delete(buf.perShard[shard], key)
	return nil
}

func sendOnTarget(mw middleware.Middleware, msg middleware.Message, routingKey string) error {
	if routingKey != "" {
		return mw.SendWithKey(msg, routingKey)
	}
	return mw.Send(msg)
}

func (w *Worker) flushClient(clientID inner.ClientID) error {
	for idx, buf := range w.outputTargetBuffers {
		for shard := range buf.perShard {
			keys := make([]batchKey, 0, len(buf.perShard[shard]))
			for k := range buf.perShard[shard] {
				if k.clientID == clientID {
					keys = append(keys, k)
				}
			}
			for _, k := range keys {
				if err := w.flushBatch(idx, shard, k); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (w *Worker) flushAll() {
	for idx, buf := range w.outputTargetBuffers {
		for shard := range buf.perShard {
			keys := make([]batchKey, 0, len(buf.perShard[shard]))
			for k := range buf.perShard[shard] {
				keys = append(keys, k)
			}
			for _, k := range keys {
				if err := w.flushBatch(idx, shard, k); err != nil {
					slog.Error("Flush on shutdown failed", "client_id", k.clientID, "output", idx, "shard", shard, "err", err)
				}
			}
		}
	}
}

func (w *Worker) shardFor(target OutputTarget, clientID inner.ClientID) int {
	if target.Kind == config.KindShardedQueues {
		return hashing.Shard(string(clientID), target.ShardCount)
	}
	return 0
}

func (w *Worker) publishEOFs(env *inner.Envelope, emits []eof.EOFEmit) error {
	if err := w.flushClient(env.ClientID); err != nil {
		return err
	}
	for _, e := range emits {
		if e.OutputIndex < 0 || e.OutputIndex >= len(w.outputs) {
			return errors.New("strategy returned invalid output index for EOF")
		}
		msg, err := inner.SerializeInternalEOF(env.GatewayID, env.ClientID, e.Total)
		if err != nil {
			return err
		}
		target := w.outputs[e.OutputIndex]
		shard := w.shardFor(target, env.ClientID)
		if shard >= len(target.Middlewares) {
			return errors.New("misconfigured target: shard index out of range")
		}
		if err := sendOnTarget(target.Middlewares[shard], *msg, e.RoutingKey); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) reenqueueUpstreamEOF(clientID inner.ClientID) error {
	w.upstreamEOFMu.Lock()
	cached, ok := w.upstreamEOFs[clientID]
	w.upstreamEOFMu.Unlock()
	if !ok {
		return errors.New("no cached upstream EOF to re-enqueue")
	}
	w.clearUpstreamEOF(clientID)
	return w.input.Send(cached)
}

func (w *Worker) cacheUpstreamEOF(clientID inner.ClientID, msg middleware.Message) {
	w.upstreamEOFMu.Lock()
	w.upstreamEOFs[clientID] = msg
	w.upstreamEOFMu.Unlock()
}

func (w *Worker) clearUpstreamEOF(clientID inner.ClientID) {
	w.upstreamEOFMu.Lock()
	delete(w.upstreamEOFs, clientID)
	w.upstreamEOFMu.Unlock()
}

func (w *Worker) handleSignals() {
	defer w.waitingGroup.Done()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		slog.Info("Signal received, shutting down worker", "signal", sig.String())
		w.cancel()
	case <-w.ctx.Done():
	}
}

func (w *Worker) shutdownWatcher() {
	defer w.waitingGroup.Done()
	<-w.ctx.Done()

	w.strategyMu.Lock()
	w.flushAll()
	w.strategyMu.Unlock()

	_ = w.input.StopConsuming()
	_ = w.input.Close()
	if w.ringIn != nil {
		_ = w.ringIn.StopConsuming()
		_ = w.ringIn.Close()
	}
	if w.ringOut != nil {
		_ = w.ringOut.Close()
	}
	for _, target := range w.outputs {
		for _, mw := range target.Middlewares {
			_ = mw.Close()
		}
	}
}

func closeAll(input middleware.Middleware, ringIn middleware.Middleware, ringOut middleware.Middleware, outputs []OutputTarget) {
	if input != nil {
		_ = input.Close()
	}
	if ringIn != nil {
		_ = ringIn.Close()
	}
	if ringOut != nil {
		_ = ringOut.Close()
	}
	for _, target := range outputs {
		for _, mw := range target.Middlewares {
			_ = mw.Close()
		}
	}
}

func isEOFKind(kind inner.MsgKind) bool {
	switch kind {
	case inner.AllTransactionsEOF, inner.AllAccountsEOF, inner.InternalEOF:
		return true
	default:
		return false
	}
}
