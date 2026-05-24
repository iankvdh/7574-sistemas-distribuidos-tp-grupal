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
	perShard []map[inner.ClientID]*pendingBatch
}

type pendingBatch struct {
	gatewayID inner.GatewayID
	items     [][]byte
	bytes     int
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

	outputs, err := BuildOutputTargets(cfg.Outputs, conn)
	if err != nil {
		_ = input.Close()
		return nil, err
	}

	var ringIn, ringOut middleware.Middleware
	if cfg.RingQueueIn != "" {
		ringIn, err = middleware.CreateQueueMiddleware(cfg.RingQueueIn, conn)
		if err != nil {
			closeAll(input, nil, nil, outputs)
			return nil, err
		}
	}
	if cfg.RingQueueOut != "" {
		ringOut, err = middleware.CreateQueueMiddleware(cfg.RingQueueOut, conn)
		if err != nil {
			closeAll(input, ringIn, nil, outputs)
			return nil, err
		}
	}

	ctx := strategy.Context{
		OutputCount:  len(outputs),
		ReplicaID:    cfg.ReplicaID,
		NReplicas:    cfg.NReplicas,
		StrategyName: cfg.StrategyName,
		MatchCount:   cfg.OutputsMatchCount,
	}
	if err := strat.Init(ctx); err != nil {
		closeAll(input, ringIn, ringOut, outputs)
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())

	buffers := make([]*outputTargetBuffer, len(outputs))
	for i, target := range outputs {
		shards := 1
		if target.Kind == config.KindShardedQueues {
			shards = target.ShardCount
		}
		perShard := make([]map[inner.ClientID]*pendingBatch, shards)
		for s := range perShard {
			perShard[s] = map[inner.ClientID]*pendingBatch{}
		}
		buffers[i] = &outputTargetBuffer{target: target, perShard: perShard}
	}

	w := &Worker{
		cfg:                 cfg,
		strategy:            strat,
		input:               input,
		ringIn:              ringIn,
		ringOut:             ringOut,
		outputs:             outputs,
		outputTargetBuffers: buffers,
		ctx:                 runCtx,
		cancel:              cancel,
		upstreamEOFs:        map[inner.ClientID]middleware.Message{},
	}
	return w, nil
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
		if err := w.applyOutcome(envelope, outcome); err != nil {
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
		for _, payload := range items {
			item := &inner.Envelope{
				Kind:      itemKind,
				GatewayID: envelope.GatewayID,
				ClientID:  envelope.ClientID,
				Payload:   payload,
			}
			if err := w.processSingleItem(item); err != nil {
				w.strategyMu.Unlock()
				slog.Error("Strategy ProcessMessage failed", "client_id", envelope.ClientID, "err", err)
				nack()
				return
			}
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

func (w *Worker) processSingleItem(env *inner.Envelope) error {
	decisions, _, err := w.strategy.ProcessMessage(env)
	if err != nil {
		return err
	}
	return w.appendDecisions(env.GatewayID, decisions)
}

func (w *Worker) handleRingMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeEnvelope(&msg)
	if err != nil {
		slog.Error("Malformed envelope in ring queue", "err", err)
		ack()
		return
	}
	if envelope.Kind != inner.RingTokenMessage {
		slog.Warn("Unexpected kind on ring queue", "kind", envelope.Kind)
		ack()
		return
	}

	token, err := eof.UnmarshalToken(envelope.Payload)
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

	pseudoEnv := &inner.Envelope{GatewayID: envelope.GatewayID, ClientID: token.ClientID}
	if err := w.applyOutcome(pseudoEnv, outcome); err != nil {
		w.strategyMu.Unlock()
		slog.Error("Applying ring outcome failed", "client_id", token.ClientID, "err", err)
		nack()
		return
	}
	w.strategyMu.Unlock()
	ack()
}

func (w *Worker) applyOutcome(env *inner.Envelope, outcome strategy.EOFOutcome) error {
	switch outcome.Action.Kind {
	case eof.ActionNone:
		return nil
	case eof.ActionForwardToken:
		// Every replica must flush its pending output batches for this client
		// before its counts ride the ring token. Otherwise the aggregated total
		// the initiator computes will exceed what downstream actually received,
		// the ring fails the check, and re-enqueues the upstream EOF forever.
		if err := w.flushClient(env.ClientID); err != nil {
			return err
		}
		return w.forwardToken(env, outcome.Action.Token)
	case eof.ActionEmitEOFs:
		w.clearUpstreamEOF(env.ClientID)
		return w.publishEOFs(env, outcome.EOFs)
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

func (w *Worker) appendDecisions(gatewayID inner.GatewayID, decisions []strategy.Decision) error {
	for _, d := range decisions {
		for _, idx := range d.OutputIndices {
			if idx < 0 || idx >= len(w.outputTargetBuffers) {
				return errors.New("strategy returned invalid output index")
			}
			if err := w.appendToShard(idx, d.ClientID, gatewayID, []byte(d.Body)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) appendToShard(idx int, clientID inner.ClientID, gatewayID inner.GatewayID, payload []byte) error {
	buf := w.outputTargetBuffers[idx]
	shard := w.shardFor(buf.target, clientID)
	bucket := buf.perShard[shard]
	pending, ok := bucket[clientID]
	if !ok {
		pending = &pendingBatch{gatewayID: gatewayID}
		bucket[clientID] = pending
	}
	pending.items = append(pending.items, payload)
	pending.bytes += len(payload)
	if pending.bytes >= w.cfg.MaxInternalBatchBytes {
		return w.flushShard(idx, shard, clientID)
	}
	return nil
}

func (w *Worker) flushShard(idx, shard int, clientID inner.ClientID) error {
	buf := w.outputTargetBuffers[idx]
	pending := buf.perShard[shard][clientID]
	if pending == nil || len(pending.items) == 0 {
		return nil
	}
	msg, err := inner.SerializeInnerBatch(inner.TransactionMessage, pending.gatewayID, clientID, pending.items)
	if err != nil {
		return err
	}
	if err := buf.target.Middlewares[shard].Send(*msg); err != nil {
		return err
	}
	delete(buf.perShard[shard], clientID)
	return nil
}

// flushClient flushes every output buffer (across all targets and shards) that
// currently holds pending items for clientID. Called before publishing EOFs to
// keep downstream count==upstream_total invariants.
func (w *Worker) flushClient(clientID inner.ClientID) error {
	for idx, buf := range w.outputTargetBuffers {
		for shard := range buf.perShard {
			if _, ok := buf.perShard[shard][clientID]; !ok {
				continue
			}
			if err := w.flushShard(idx, shard, clientID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) flushAll() {
	for idx, buf := range w.outputTargetBuffers {
		for shard := range buf.perShard {
			for clientID := range buf.perShard[shard] {
				if err := w.flushShard(idx, shard, clientID); err != nil {
					slog.Error("Flush on shutdown failed", "client_id", clientID, "output", idx, "shard", shard, "err", err)
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
		if err := target.Middlewares[shard].Send(*msg); err != nil {
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
