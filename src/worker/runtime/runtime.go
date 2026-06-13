package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/heartbeat"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/builder"
)

type Worker struct {
	cfg                 config.WorkerConfig
	strategy            strategy.Strategy
	inputs              []middleware.Middleware
	ringIn              middleware.Middleware
	ringOut             middleware.Middleware
	outputs             []OutputTarget
	outputTargetBuffers []*outputTargetBuffer
	ctx                 context.Context
	cancel              context.CancelFunc
	waitingGroup        sync.WaitGroup
	strategyMu          sync.Mutex
	upstreamEOFMu       sync.Mutex

	// Stores last upstream EOF message for each client (and the input it came
	// from), to allow re-enqueuing to the right input if the strategy requests
	// it. Cleared on successful publish of EOFs.
	upstreamEOFs   map[inner.ClientID]cachedEOF
	inputWG        sync.WaitGroup
	inputStartMu   sync.Mutex
	startedInputs  []bool
	deferredInputs map[int]bool
}

type cachedEOF struct {
	inputIndex int
	msg        middleware.Message
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

	if len(cfg.InputConfigs) == 0 {
		return nil, errors.New("worker requires at least one input")
	}
	inputs := make([]middleware.Middleware, 0, len(cfg.InputConfigs))
	for i, ic := range cfg.InputConfigs {
		mw, err := BuildInputMiddleware(ic, conn)
		if err != nil {
			for _, opened := range inputs {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open input %d (%s): %w", i, ic.Name, err)
		}
		inputs = append(inputs, mw)
	}

	outputTargets, err := BuildOutputTargets(cfg.Outputs, conn)
	if err != nil {
		for _, mw := range inputs {
			_ = mw.Close()
		}
		return nil, err
	}

	var ringIn, ringOut middleware.Middleware
	if cfg.RingQueueIn != "" {
		ringIn, err = middleware.CreateQueueMiddleware(cfg.RingQueueIn, conn)
		if err != nil {
			closeAll(inputs, nil, nil, outputTargets)
			return nil, err
		}
	}
	if cfg.RingQueueOut != "" {
		ringOut, err = middleware.CreateQueueMiddleware(cfg.RingQueueOut, conn)
		if err != nil {
			closeAll(inputs, ringIn, nil, outputTargets)
			return nil, err
		}
	}

	err = initStrategy(outputTargets, cfg, strat, inputs, ringIn, ringOut)
	if err != nil {
		return nil, err
	}

	deferred := map[int]bool{}
	if def_input_provider, ok := strat.(strategy.DeferredInputProvider); ok {
		for _, idx := range def_input_provider.DeferredInputs() {
			deferred[idx] = true
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	buffers := initOutputTargetBuffers(outputTargets)
	w := &Worker{
		cfg:                 cfg,
		strategy:            strat,
		inputs:              inputs,
		ringIn:              ringIn,
		ringOut:             ringOut,
		outputs:             outputTargets,
		outputTargetBuffers: buffers,
		ctx:                 runCtx,
		cancel:              cancel,
		upstreamEOFs:        map[inner.ClientID]cachedEOF{},
		startedInputs:       make([]bool, len(inputs)),
		deferredInputs:      deferred,
	}
	return w, nil
}

func initStrategy(outputTargets []OutputTarget, cfg config.WorkerConfig, strat strategy.Strategy, inputs []middleware.Middleware, ringIn middleware.Middleware, ringOut middleware.Middleware) error {
	strategyCfg := strategy.StrategyConfig{
		OutputCount:  len(outputTargets),
		ReplicaID:    cfg.ReplicaID,
		NReplicas:    cfg.NReplicas,
		StrategyName: cfg.StrategyName,
		MatchCount:   cfg.OutputsMatchCount,
		NumInputs:    len(cfg.InputConfigs),
		RingQueueIn:  cfg.RingQueueIn,
		RingQueueOut: cfg.RingQueueOut,
	}
	if err := strat.Init(strategyCfg); err != nil {
		closeAll(inputs, ringIn, ringOut, outputTargets)
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

	inputNames := make([]string, 0, len(w.cfg.InputConfigs))
	for _, ic := range w.cfg.InputConfigs {
		inputNames = append(inputNames, ic.Name)
	}
	slog.Info(
		"Worker started",
		"strategy", w.cfg.StrategyName,
		"replica_id", w.cfg.ReplicaID,
		"n_replicas", w.cfg.NReplicas,
		"inputs", inputNames,
		"outputs", len(w.outputs),
		"ring_in", w.cfg.RingQueueIn,
		"ring_out", w.cfg.RingQueueOut,
	)

	if w.ringIn != nil {
		w.waitingGroup.Add(1)
		go w.consumeRing()
	}

	for i := range w.inputs {
		if w.deferredInputs[i] {
			continue
		}
		w.startInput(i)
	}

	w.startHeartbeat()

	w.inputWG.Wait()
	return nil
}

func (w *Worker) startHeartbeat() {
	if !w.cfg.HeartbeatEnabled || len(w.cfg.SentinelUDPAddrs) == 0 {
		slog.Debug("Heartbeat disabled")
		return
	}
	emitter, err := heartbeat.New(w.cfg.ContainerName, w.cfg.SentinelUDPAddrs,
		w.cfg.HeartbeatInterval, w.cfg.HeartbeatJitter)
	if err != nil {
		slog.Error("Could not initialize heartbeat emitter", "err", err)
		return
	}
	w.waitingGroup.Add(1)
	go func() {
		defer w.waitingGroup.Done()
		emitter.Run(w.ctx)
	}()
}

func (w *Worker) startInput(idx int) {
	w.inputStartMu.Lock()
	if w.startedInputs[idx] {
		w.inputStartMu.Unlock()
		return
	}
	w.startedInputs[idx] = true
	w.inputWG.Add(1)
	w.inputStartMu.Unlock()
	go func() {
		defer w.inputWG.Done()
		err := w.inputs[idx].StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
			w.handleInputMessage(idx, msg, ack, nack)
		})
		if err != nil && w.ctx.Err() == nil {
			slog.Error("Input consumer stopped unexpectedly", "input_index", idx, "err", err)
		}
	}()
}

func (w *Worker) consumeRing() {
	defer w.waitingGroup.Done()
	err := w.ringIn.StartConsuming(w.handleRingMessage)
	if err != nil && w.ctx.Err() == nil {
		slog.Error("Ring consumer stopped unexpectedly", "err", err)
	}
}

func (w *Worker) handleInputMessage(inputIndex int, msg middleware.Message, ack func(), nack func()) {
	parsed, err := inner.NewFromSerializedData([]byte(msg.Body))
	if err != nil {
		slog.Error("Malformed message in input queue", "input_index", inputIndex, "err", err)
		ack()
		return
	}

	switch typed := parsed.(type) {
	case *inner.EOFMessage:
		envelope := &inner.Envelope{
			GatewayID:  typed.GatewayID,
			ClientID:   typed.ClientID,
			Total:      typed.Total,
			QueryID:    typed.QueryID,
			InputIndex: inputIndex,
		}
		w.cacheUpstreamEOF(envelope.ClientID, inputIndex, msg)
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
	case *inner.BatchMessage:
		w.strategyMu.Lock()
		if err := w.processBatch(typed, inputIndex); err != nil {
			w.strategyMu.Unlock()
			slog.Error("Strategy ProcessMessage failed", "client_id", typed.ClientID, "err", err)
			nack()
			return
		}
		w.strategyMu.Unlock()
		ack()
	default:
		slog.Error("Unexpected message type in input queue", "input_index", inputIndex, "type", parsed.Type())
		ack()
	}
}

func (w *Worker) processBatch(batch *inner.BatchMessage, inputIndex int) error {
	for _, item := range batch.Items {
		envelope := &inner.Envelope{
			Kind:       batch.ItemKind,
			GatewayID:  batch.GatewayID,
			ClientID:   batch.ClientID,
			QueryID:    item.QueryID,
			Payload:    item.Payload,
			InputIndex: inputIndex,
		}
		if err := w.processSingleItem(envelope); err != nil {
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
	if err := w.appendOutputMessages(env.GatewayID, outputMessages); err != nil {
		return err
	}
	emitter, ok := w.strategy.(strategy.ReadyEOFEmitter)
	if !ok {
		return nil
	}
	outcome, ready := emitter.ReadyEOFs(env)
	if !ready {
		return nil
	}
	return w.applyEOFOutcome(env, outcome)
}

func (w *Worker) handleRingMessage(msg middleware.Message, ack func(), nack func()) {
	parsed, err := inner.NewFromSerializedData([]byte(msg.Body))
	if err != nil {
		slog.Error("Malformed message in ring queue", "err", err)
		ack()
		return
	}
	ringMessage, ok := parsed.(*inner.RingTokenMessage)
	if !ok {
		slog.Warn("Unexpected message type on ring queue", "type", parsed.Type())
		ack()
		return
	}

	token := &eof.Token{
		ClientID:      ringMessage.ClientID,
		InitiatorID:   int(ringMessage.InitiatorID),
		AggMatched:    ringMessage.AggMatched,
		AggNotMatched: ringMessage.AggNotMatched,
		Phase:         eof.TokenPhase(ringMessage.Phase),
	}

	w.strategyMu.Lock()
	outcome, err := w.strategy.OnRingToken(token)
	if err != nil {
		w.strategyMu.Unlock()
		slog.Error("Strategy OnRingToken failed", "client_id", token.ClientID, "err", err)
		nack()
		return
	}

	resultEnvelope := &inner.Envelope{GatewayID: ringMessage.GatewayID, ClientID: token.ClientID}
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
	if err := w.applyAction(env, outcome); err != nil {
		return err
	}
	for _, idx := range outcome.StartDeferredInputs {
		if idx < 0 || idx >= len(w.inputs) {
			return fmt.Errorf("strategy asked to start deferred input %d, out of range", idx)
		}
		w.startInput(idx)
	}
	return nil
}

func (w *Worker) applyAction(env *inner.Envelope, outcome strategy.EOFOutcome) error {
	switch outcome.Action.Kind {
	case eof.ActionNone:
		return nil
	case eof.ActionForwardToken:
		if err := w.flushClient(env.ClientID); err != nil {
			return err
		}
		return w.forwardToken(env, outcome.Action.Token)
	case eof.ActionEmitEOFs:
		if err := w.emitOutcomeOutputs(env.GatewayID, outcome); err != nil {
			return err
		}
		w.clearUpstreamEOF(env.ClientID)
		return w.publishEOFs(env, outcome.EOFs)
	case eof.ActionEmitEOFsAndForwardToken:
		if err := w.emitOutcomeOutputs(env.GatewayID, outcome); err != nil {
			return err
		}
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
	ringMessage := &inner.RingTokenMessage{
		Header:        inner.Header{GatewayID: env.GatewayID, ClientID: token.ClientID},
		InitiatorID:   int32(token.InitiatorID),
		AggMatched:    token.AggMatched,
		AggNotMatched: token.AggNotMatched,
		Phase:         uint8(token.Phase),
	}
	msg, err := serialize(ringMessage)
	if err != nil {
		return err
	}
	return w.ringOut.Send(*msg)
}

func (w *Worker) emitOutcomeOutputs(gatewayID inner.GatewayID, outcome strategy.EOFOutcome) error {
	if outcome.OutputsIterator != nil {
		for om := range outcome.OutputsIterator {
			if err := w.appendSingleOutputMessage(gatewayID, om); err != nil {
				return err
			}
		}
		return nil
	}
	return w.appendOutputMessages(gatewayID, outcome.Outputs)
}

func (w *Worker) appendSingleOutputMessage(gatewayID inner.GatewayID, om strategy.OutputMessage) error {
	for _, idx := range om.OutputIndices {
		if idx < 0 || idx >= len(w.outputTargetBuffers) {
			return errors.New("strategy returned invalid output index")
		}
		if err := w.appendToShard(idx, gatewayID, om); err != nil {
			return err
		}
	}
	return nil
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
	msg, err := w.serializePending(buf.target, pending)
	if err != nil {
		return err
	}
	if err := sendOnTarget(buf.target.Middlewares[shard], *msg, pending.routingKey); err != nil {
		return err
	}
	delete(buf.perShard[shard], key)
	return nil
}

// serializePending builds the on-wire Batch for a pending batch. Internal targets
// keep their item_kind so the downstream worker interprets each payload; final_queue
// targets use ResultRow (the gateway's final consumer reads each payload as a CSV
// row). Both shapes are the same BatchMessage type.
func (w *Worker) serializePending(target OutputTarget, pending *pendingBatch) (*middleware.Message, error) {
	itemKind := pending.itemKind
	if target.Kind == config.KindFinalQueue {
		itemKind = inner.ResultRow
	}
	items := make([]inner.BatchItem, 0, len(pending.items))
	for _, payload := range pending.items {
		items = append(items, inner.BatchItem{QueryID: pending.queryID, Payload: payload})
	}
	return serialize(&inner.BatchMessage{
		Header:   inner.Header{GatewayID: pending.gatewayID, ClientID: pending.clientID},
		ItemKind: itemKind,
		Items:    items,
	})
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
		target := w.outputs[e.OutputIndex]
		msg, err := w.serializeEOF(target, env, e)
		if err != nil {
			return err
		}
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

// serializeEOF picks the on-wire EOF shape based on the target kind. Internal
// pipeline targets get an EOFMessage (downstream workers count them); final_queue
// targets get a Batch with a single ResultRow item carrying "EOF" (the gateway's
// final consumer interprets that item as "this query is done for this client").
func (w *Worker) serializeEOF(target OutputTarget, env *inner.Envelope, e eof.EOFEmit) (*middleware.Message, error) {
	if target.Kind == config.KindFinalQueue {
		return serialize(&inner.BatchMessage{
			Header:   inner.Header{GatewayID: env.GatewayID, ClientID: env.ClientID},
			ItemKind: inner.ResultRow,
			Items:    []inner.BatchItem{{QueryID: e.QueryID, Payload: []byte("EOF")}},
		})
	}
	return serialize(&inner.EOFMessage{
		Header:  inner.Header{GatewayID: env.GatewayID, ClientID: env.ClientID},
		QueryID: e.QueryID,
		Total:   e.Total,
	})
}

func (w *Worker) reenqueueUpstreamEOF(clientID inner.ClientID) error {
	w.upstreamEOFMu.Lock()
	cached, ok := w.upstreamEOFs[clientID]
	w.upstreamEOFMu.Unlock()
	if !ok {
		return errors.New("no cached upstream EOF to re-enqueue")
	}
	if cached.inputIndex < 0 || cached.inputIndex >= len(w.inputs) {
		return errors.New("cached upstream EOF references invalid input index")
	}
	w.clearUpstreamEOF(clientID)
	return w.inputs[cached.inputIndex].Send(cached.msg)
}

func (w *Worker) cacheUpstreamEOF(clientID inner.ClientID, inputIndex int, msg middleware.Message) {
	w.upstreamEOFMu.Lock()
	w.upstreamEOFs[clientID] = cachedEOF{inputIndex: inputIndex, msg: msg}
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

	for _, mw := range w.inputs {
		_ = mw.StopConsuming()
		_ = mw.Close()
	}
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

func closeAll(inputs []middleware.Middleware, ringIn middleware.Middleware, ringOut middleware.Middleware, outputs []OutputTarget) {
	for _, mw := range inputs {
		if mw != nil {
			_ = mw.Close()
		}
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

// serialize wraps the bytes of an InternalMessage in a middleware.Message.
func serialize(msg inner.InternalMessage) (*middleware.Message, error) {
	raw, err := msg.Serialize()
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(raw)}, nil
}
