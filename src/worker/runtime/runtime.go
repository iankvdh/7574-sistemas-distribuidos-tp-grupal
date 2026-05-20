package runtime

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/topology"
)

// Worker drives the lifecycle of a single STRATEGY: connects input/outputs/ring queues,
// consumes messages, dispatches them to the strategy, and publishes the results.
type Worker struct {
	cfg          config.WorkerConfig
	strategy     strategy.Strategy
	input        middleware.Middleware
	ringIn       middleware.Middleware // nil if no ring is configured
	ringOut      middleware.Middleware // nil if no ring is configured
	outputs      []topology.OutputSink
	logger       *slog.Logger
	running      atomic.Bool
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	wg           sync.WaitGroup

	// strategyMu serializes every call into the Strategy. The input consumer and
	// the ring consumer run on different goroutines, but neither the Strategy nor
	// its EOF coordinator are safe for concurrent access — keeping a single mutex
	// here is simpler than asking every strategy implementation to be thread-safe.
	strategyMu sync.Mutex

	// upstreamEOFs caches the upstream EOF envelope per clientID so we can re-enqueue it
	// when the ring fails to converge.
	mu           sync.Mutex
	upstreamEOFs map[inner.ClientID]middleware.Message
}

func New(cfg config.WorkerConfig, logger *slog.Logger) (*Worker, error) {
	strat, err := strategy.Build(cfg.StrategyName)
	if err != nil {
		return nil, err
	}

	bindings, err := topology.ParseOutputs()
	if err != nil {
		return nil, err
	}

	conn := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	inputBinding, err := topology.ParseInput(cfg.Input)
	if err != nil {
		return nil, err
	}
	input, err := topology.BuildInputMiddleware(inputBinding, conn)
	if err != nil {
		return nil, err
	}

	outputs, err := topology.BuildOutputSinks(bindings, conn)
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

	matchCount := cfg.OutputsMatchCount
	if matchCount == 0 {
		matchCount = len(outputs)
	}
	if matchCount > len(outputs) {
		closeAll(input, ringIn, ringOut, outputs)
		return nil, errors.New("OUTPUTS_MATCH_COUNT exceeds number of OUTPUT_* bindings")
	}

	ctx := strategy.Context{
		Logger:       logger,
		OutputCount:  len(outputs),
		ReplicaID:    cfg.ReplicaID,
		NReplicas:    cfg.NReplicas,
		StrategyName: cfg.StrategyName,
		MatchCount:   matchCount,
	}
	if err := strat.Init(ctx); err != nil {
		closeAll(input, ringIn, ringOut, outputs)
		return nil, err
	}

	w := &Worker{
		cfg:          cfg,
		strategy:     strat,
		input:        input,
		ringIn:       ringIn,
		ringOut:      ringOut,
		outputs:      outputs,
		logger:       logger,
		shutdownCh:   make(chan struct{}),
		upstreamEOFs: map[inner.ClientID]middleware.Message{},
	}
	w.running.Store(true)
	return w, nil
}

func (w *Worker) Run() error {
	defer w.shutdown()

	w.wg.Add(1)
	go w.handleSignals()

	w.logger.Info(
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
		w.wg.Add(1)
		go w.consumeRing()
	}

	err := w.input.StartConsuming(w.handleInputMessage)
	if err != nil && w.running.Load() {
		w.logger.Error("Input consumer stopped unexpectedly", "err", err)
	}
	w.wg.Wait()
	return nil
}

func (w *Worker) consumeRing() {
	defer w.wg.Done()
	err := w.ringIn.StartConsuming(w.handleRingMessage)
	if err != nil && w.running.Load() {
		w.logger.Error("Ring consumer stopped unexpectedly", "err", err)
	}
}

func (w *Worker) handleInputMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeEnvelope(&msg)
	if err != nil {
		w.logger.Error("Malformed envelope in input queue", "err", err)
		ack()
		return
	}

	if isEOFKind(envelope.Kind) {
		w.cacheUpstreamEOF(envelope.ClientID, msg)
		w.strategyMu.Lock()
		outcome, err := w.strategy.OnUpstreamEOF(envelope)
		w.strategyMu.Unlock()
		if err != nil {
			w.logger.Error("Strategy OnUpstreamEOF failed", "client_id", envelope.ClientID, "err", err)
			nack()
			return
		}
		if err := w.applyOutcome(envelope, outcome); err != nil {
			w.logger.Error("Applying EOF outcome failed", "client_id", envelope.ClientID, "err", err)
			nack()
			return
		}
		ack()
		return
	}

	w.strategyMu.Lock()
	decisions, _, err := w.strategy.ProcessMessage(envelope)
	w.strategyMu.Unlock()
	if err != nil {
		w.logger.Error("Strategy ProcessMessage failed", "client_id", envelope.ClientID, "err", err)
		nack()
		return
	}
	if err := w.publishDecisions(decisions); err != nil {
		w.logger.Error("Publishing decision failed", "client_id", envelope.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (w *Worker) handleRingMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeEnvelope(&msg)
	if err != nil {
		w.logger.Error("Malformed envelope in ring queue", "err", err)
		ack()
		return
	}
	if envelope.Kind != inner.RingTokenMessage {
		w.logger.Warn("Unexpected kind on ring queue", "kind", envelope.Kind)
		ack()
		return
	}

	token, err := eof.UnmarshalToken(envelope.Payload)
	if err != nil {
		w.logger.Error("Malformed ring token", "err", err)
		ack()
		return
	}

	w.strategyMu.Lock()
	outcome, err := w.strategy.OnRingToken(token)
	w.strategyMu.Unlock()
	if err != nil {
		w.logger.Error("Strategy OnRingToken failed", "client_id", token.ClientID, "err", err)
		nack()
		return
	}

	// Synthesize an envelope with the ClientID from the token (and the original
	// gateway id) so applyOutcome can publish EOFs without re-reading the upstream cache.
	pseudoEnv := &inner.Envelope{GatewayID: envelope.GatewayID, ClientID: token.ClientID}
	if err := w.applyOutcome(pseudoEnv, outcome); err != nil {
		w.logger.Error("Applying ring outcome failed", "client_id", token.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (w *Worker) applyOutcome(env *inner.Envelope, outcome strategy.EOFOutcome) error {
	switch outcome.Action.Kind {
	case eof.ActionNone:
		return nil
	case eof.ActionForwardToken:
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

func (w *Worker) publishDecisions(decisions []strategy.Decision) error {
	for _, d := range decisions {
		for _, idx := range d.OutputIndices {
			if idx < 0 || idx >= len(w.outputs) {
				return errors.New("strategy returned invalid output index")
			}
			if err := w.sendToSink(w.outputs[idx], d.ClientID, middleware.Message{Body: d.Body}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) publishEOFs(env *inner.Envelope, emits []eof.EOFEmit) error {
	for _, e := range emits {
		if e.OutputIndex < 0 || e.OutputIndex >= len(w.outputs) {
			return errors.New("strategy returned invalid output index for EOF")
		}
		msg, err := inner.SerializeInternalEOF(env.GatewayID, env.ClientID, e.Total)
		if err != nil {
			return err
		}
		if err := w.sendToSink(w.outputs[e.OutputIndex], env.ClientID, *msg); err != nil {
			return err
		}
	}
	return nil
}

// sendToSink dispatches a message to the right middleware inside a sink. For
// sharded sinks, the destination queue is chosen by hashing the client_id so
// the same client always lands on the same downstream replica.
func (w *Worker) sendToSink(sink topology.OutputSink, clientID inner.ClientID, msg middleware.Message) error {
	switch sink.Kind {
	case topology.KindShardedQueues:
		if sink.ShardCount <= 0 || len(sink.Middlewares) != sink.ShardCount {
			return errors.New("misconfigured sharded sink")
		}
		shard := hashing.Shard(string(clientID), sink.ShardCount)
		return sink.Middlewares[shard].Send(msg)
	default:
		if len(sink.Middlewares) != 1 {
			return errors.New("non-sharded sink expects exactly 1 middleware")
		}
		return sink.Middlewares[0].Send(msg)
	}
}

func (w *Worker) reenqueueUpstreamEOF(clientID inner.ClientID) error {
	w.mu.Lock()
	cached, ok := w.upstreamEOFs[clientID]
	w.mu.Unlock()
	if !ok {
		return errors.New("no cached upstream EOF to re-enqueue")
	}
	w.clearUpstreamEOF(clientID)
	return w.input.Send(cached)
}

func (w *Worker) cacheUpstreamEOF(clientID inner.ClientID, msg middleware.Message) {
	w.mu.Lock()
	w.upstreamEOFs[clientID] = msg
	w.mu.Unlock()
}

func (w *Worker) clearUpstreamEOF(clientID inner.ClientID) {
	w.mu.Lock()
	delete(w.upstreamEOFs, clientID)
	w.mu.Unlock()
}

func (w *Worker) handleSignals() {
	defer w.wg.Done()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		w.logger.Info("Signal received, shutting down worker", "signal", sig.String())
		w.shutdown()
	case <-w.shutdownCh:
	}
}

func (w *Worker) shutdown() {
	w.shutdownOnce.Do(func() {
		w.running.Store(false)
		close(w.shutdownCh)
		_ = w.input.StopConsuming()
		_ = w.input.Close()
		if w.ringIn != nil {
			_ = w.ringIn.StopConsuming()
			_ = w.ringIn.Close()
		}
		if w.ringOut != nil {
			_ = w.ringOut.Close()
		}
		for _, sink := range w.outputs {
			for _, mw := range sink.Middlewares {
				_ = mw.Close()
			}
		}
	})
}

func closeAll(input middleware.Middleware, ringIn middleware.Middleware, ringOut middleware.Middleware, outputs []topology.OutputSink) {
	if input != nil {
		_ = input.Close()
	}
	if ringIn != nil {
		_ = ringIn.Close()
	}
	if ringOut != nil {
		_ = ringOut.Close()
	}
	for _, sink := range outputs {
		for _, mw := range sink.Middlewares {
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
