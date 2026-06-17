package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/checkpoint"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/heartbeat"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/builder"
)

const checkpointDirPermissions = 0o755
const checkpointNamePrefix = "client_"

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
	stateMu             sync.Mutex

	// Stores last upstream EOF message for each client (and the input it came
	// from), to allow re-enqueuing to the right input if the strategy requests
	// it. Cleared on successful EOF publish and on cleanupClientState.
	upstreamEOFs   map[inner.ClientID]cachedEOF
	inputWG        sync.WaitGroup
	inputStartMu   sync.Mutex
	startedInputs  []bool
	deferredInputs map[int]bool
	draining       bool
	pendingAcks    map[inner.ClientID][]deliveryRef
	totalPending   int
	lastSeen       map[inner.ClientID]time.Time
	tombstones     map[inner.ClientID]time.Time
	lastRecvSeqID  map[inner.SeqKey]uint64
	outSeqID       map[inner.ClientID]uint64
	roundRobinCtrs map[inner.ClientID]map[int]uint64 // clientID → outputIdx → round-robin counter
}

type deliveryRef struct {
	ack  func()
	nack func()
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

	conn, err := middleware.NewConnSettings(cfg.MomHost, cfg.MomPort)
	if err != nil {
		return nil, err
	}

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

	if err := os.MkdirAll(cfg.CheckpointDir, checkpointDirPermissions); err != nil {
		closeAll(inputs, ringIn, ringOut, outputTargets)
		return nil, fmt.Errorf("create checkpoint dir %q: %w", cfg.CheckpointDir, err)
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
		deferredInputs:      map[int]bool{},

		pendingAcks:    map[inner.ClientID][]deliveryRef{},
		lastSeen:       map[inner.ClientID]time.Time{},
		tombstones:     map[inner.ClientID]time.Time{},
		lastRecvSeqID:  map[inner.SeqKey]uint64{},
		outSeqID:       map[inner.ClientID]uint64{},
		roundRobinCtrs: map[inner.ClientID]map[int]uint64{},
	}

	w.loadCheckpoints()
	w.deferredInputs = deferredInputsForStrategy(strat)
	return w, nil
}

func deferredInputsForStrategy(strat strategy.Strategy) map[int]bool {
	deferred := map[int]bool{}
	if defInputProvider, ok := strat.(strategy.DeferredInputProvider); ok {
		for _, idx := range defInputProvider.DeferredInputs() {
			deferred[idx] = true
		}
	}
	return deferred
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
		if target.Kind == config.KindShardedQueues || target.Kind == config.KindRoundRobinQueues {
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

	w.waitingGroup.Add(1)
	go w.runMaintenanceLoop()

	inputNames := make([]string, 0, len(w.cfg.InputConfigs))
	for _, ic := range w.cfg.InputConfigs {
		inputNames = append(inputNames, ic.Name)
	}
	slog.Info(
		"Worker started",
		"strategy", w.cfg.StrategyName,
		"stage_type", w.cfg.StageType,
		"replica_id", w.cfg.ReplicaID,
		"n_replicas", w.cfg.NReplicas,
		"inputs", inputNames,
		"outputs", len(w.outputs),
		"ring_in", w.cfg.RingQueueIn,
		"ring_out", w.cfg.RingQueueOut,
		"checkpoint_dir", w.cfg.CheckpointDir,
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

func (w *Worker) flushPublishers() error {
	for _, mw := range w.inputs {
		if err := mw.FlushPublisher(); err != nil {
			return err
		}
	}
	if w.ringIn != nil {
		if err := w.ringIn.FlushPublisher(); err != nil {
			return err
		}
	}
	if w.ringOut != nil {
		if err := w.ringOut.FlushPublisher(); err != nil {
			return err
		}
	}
	for _, target := range w.outputs {
		for _, mw := range target.Middlewares {
			if err := mw.FlushPublisher(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) handleInputMessage(inputIndex int, msg middleware.Message, ack func(), nack func()) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if w.draining {
		nack()
		return
	}

	parsed, err := inner.NewFromSerializedData([]byte(msg.Body))
	if err != nil {
		slog.Error("Malformed message in input queue", "input_index", inputIndex, "err", err)
		ack()
		return
	}

	header := inner.HeaderOf(parsed)
	clientID := header.ClientID
	if _, dead := w.tombstones[clientID]; dead {
		ack()
		return
	}
	if w.isDuplicate(clientID, header) {
		ack()
		return
	}
	w.lastSeen[clientID] = time.Now()

	switch typed := parsed.(type) {
	case *inner.EOFMessage:
		w.handleUpstreamEOF(typed, msg, inputIndex, header, ack, nack)
	case *inner.BatchMessage:
		w.handleBatch(typed, []byte(msg.Body), inputIndex, header, ack, nack)
	default:
		slog.Error("Unexpected message type in input queue", "input_index", inputIndex, "type", parsed.Type())
		ack()
	}
}

func (w *Worker) handleRingMessage(msg middleware.Message, ack func(), nack func()) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if w.draining {
		nack()
		return
	}

	parsed, err := inner.NewFromSerializedData([]byte(msg.Body))
	if err != nil {
		slog.Error("Malformed message in ring queue (poison, discarding)", "err", err)
		ack()
		return
	}
	typed, ok := parsed.(*inner.RingTokenMessage)
	if !ok {
		slog.Warn("Unexpected message type on ring queue", "type", parsed.Type())
		ack()
		return
	}

	header := inner.HeaderOf(parsed)
	clientID := header.ClientID
	if _, dead := w.tombstones[clientID]; dead {
		ack()
		return
	}
	if w.isDuplicate(clientID, header) {
		ack()
		return
	}
	w.lastSeen[clientID] = time.Now()

	w.handleRingToken(typed, header, ack, nack)
}

func (w *Worker) isDuplicate(clientID inner.ClientID, header inner.Header) bool {
	if header.SeqID == 0 {
		return false
	}
	return header.SeqID <= w.lastRecvSeqID[seqKeyOf(clientID, header)]
}

func (w *Worker) handleBatch(batch *inner.BatchMessage, raw []byte, inputIndex int, header inner.Header, ack, nack func()) {
	completed, err := w.processBatch(batch, raw, inputIndex)
	if err != nil {
		slog.Error("processBatch failed, exiting for redelivery", "client_id", batch.ClientID, "err", err)
		os.Exit(1)
	}
	w.lastRecvSeqID[seqKeyOf(header.ClientID, header)] = header.SeqID
	if completed {
		w.completeClient(header.ClientID, deliveryRef{ack: ack, nack: nack})
		return
	}
	w.enqueuePending(header.ClientID, deliveryRef{ack: ack, nack: nack})
	if len(w.pendingAcks[header.ClientID]) >= w.cfg.CheckpointInterval {
		w.checkpointAndAck(header.ClientID)
	} else if w.totalPending >= middleware.PrefetchCount {
		w.flushAllPending()
	}
}

func (w *Worker) handleUpstreamEOF(typed *inner.EOFMessage, msg middleware.Message, inputIndex int, header inner.Header, ack, nack func()) {
	envelope := &inner.Envelope{
		GatewayID:       typed.GatewayID,
		ClientID:        typed.ClientID,
		Total:           typed.Total,
		QueryID:         typed.QueryID,
		InputIndex:      inputIndex,
		SenderStageType: typed.SenderStageType,
		SenderReplicaID: typed.SenderReplicaID,
	}
	w.cacheUpstreamEOF(envelope.ClientID, inputIndex, msg)
	outcome, err := w.strategy.OnUpstreamEOF(envelope)
	if err != nil {
		slog.Error("Strategy OnUpstreamEOF failed, exiting for redelivery", "client_id", envelope.ClientID, "err", err)
		os.Exit(1)
	}
	if err := w.applyEOFOutcome(envelope, outcome); err != nil {
		slog.Error("Applying EOF outcome failed, exiting for redelivery", "client_id", envelope.ClientID, "err", err)
		os.Exit(1)
	}
	w.lastRecvSeqID[seqKeyOf(header.ClientID, header)] = header.SeqID
	if outcome.ClientCompleted {
		w.completeClient(envelope.ClientID, deliveryRef{ack: ack, nack: nack})
		return
	}
	w.enqueuePending(envelope.ClientID, deliveryRef{ack: ack, nack: nack})
	w.checkpointAndAck(envelope.ClientID)
}

func (w *Worker) handleRingToken(typed *inner.RingTokenMessage, header inner.Header, ack, nack func()) {
	token := tokenFromMessage(typed)
	outcome, err := w.strategy.OnRingToken(token)
	if err != nil {
		slog.Error("Strategy OnRingToken failed, exiting for redelivery", "client_id", token.ClientID, "err", err)
		os.Exit(1)
	}
	resultEnv := &inner.Envelope{GatewayID: header.GatewayID, ClientID: token.ClientID}
	if err := w.applyEOFOutcome(resultEnv, outcome); err != nil {
		slog.Error("Applying ring outcome failed, exiting for redelivery", "client_id", token.ClientID, "err", err)
		os.Exit(1)
	}
	w.lastRecvSeqID[seqKeyOf(token.ClientID, header)] = header.SeqID
	if outcome.ClientCompleted {
		w.completeClient(token.ClientID, deliveryRef{ack: ack, nack: nack})
		return
	}
	w.enqueuePending(token.ClientID, deliveryRef{ack: ack, nack: nack})
	w.checkpointAndAck(token.ClientID)
}

func tokenFromMessage(m *inner.RingTokenMessage) *eof.Token {
	return &eof.Token{
		ClientID:      m.ClientID,
		InitiatorID:   int(m.InitiatorID),
		AggMatched:    m.AggMatched,
		AggNotMatched: m.AggNotMatched,
		Phase:         eof.TokenPhase(m.Phase),
	}
}

func (w *Worker) processBatch(batch *inner.BatchMessage, rawBatch []byte, inputIndex int) (bool, error) {
	if rawStrat, ok := w.strategy.(strategy.RawBatchStrategy); ok {
		outputs, _, handled, err := rawStrat.ProcessRawBatch(batch, rawBatch, inputIndex)
		if err != nil {
			if errors.Is(err, strategy.ErrInvalidData) {
				slog.Warn("RawBatchStrategy: invalid batch, discarding", "client_id", batch.ClientID, "err", err)
				return false, nil
			}
			return false, err
		}
		if err := w.appendOutputMessages(batch.GatewayID, outputs); err != nil {
			return false, err
		}
		if handled {
			return false, nil
		}
	}
	clientCompleted := false
	for _, item := range batch.Items {
		envelope := &inner.Envelope{
			Kind:            batch.ItemKind,
			GatewayID:       batch.GatewayID,
			ClientID:        batch.ClientID,
			QueryID:         item.QueryID,
			Payload:         item.Payload,
			InputIndex:      inputIndex,
			SenderStageType: batch.SenderStageType,
			SenderReplicaID: batch.SenderReplicaID,
		}
		itemCompleted, err := w.processSingleItem(envelope)
		if err != nil {
			return false, err
		}
		if itemCompleted {
			clientCompleted = true
		}
	}
	return clientCompleted, nil
}

func (w *Worker) processSingleItem(env *inner.Envelope) (bool, error) {
	if err := w.strategy.Validate(env); err != nil {
		if errors.Is(err, strategy.ErrInvalidData) {
			slog.Warn("Strategy.Validate: invalid data, discarding", "client_id", env.ClientID, "err", err)
			return false, nil
		}
		return false, err
	}
	outputMessages, _, err := w.strategy.ProcessMessage(env)
	if err != nil {
		return false, err
	}
	if err := w.appendOutputMessages(env.GatewayID, outputMessages); err != nil {
		return false, err
	}
	emitter, ok := w.strategy.(strategy.ReadyEOFEmitter)
	if !ok {
		return false, nil
	}
	outcome, ready := emitter.ReadyEOFs(env)
	if !ready {
		return false, nil
	}
	if err := w.applyEOFOutcome(env, outcome); err != nil {
		return false, err
	}
	return outcome.ClientCompleted, nil
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
		if err := w.flushClientBuffers(env.ClientID); err != nil {
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
		if err := w.flushClientBuffers(env.ClientID); err != nil {
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
		Header:        w.outHeader(env.GatewayID, token.ClientID),
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
	shard := w.dataShard(idx, buf.target, om.ClientID)
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
		Header:   w.outHeader(pending.gatewayID, pending.clientID),
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

func (w *Worker) flushClientBuffers(clientID inner.ClientID) error {
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

func (w *Worker) shardFor(target OutputTarget, clientID inner.ClientID) int {
	if target.Kind == config.KindShardedQueues || target.Kind == config.KindRoundRobinQueues {
		return hashing.Shard(string(clientID), target.ShardCount)
	}
	return 0
}

func (w *Worker) dataShard(idx int, target OutputTarget, clientID inner.ClientID) int {
	switch target.Kind {
	case config.KindShardedQueues:
		return hashing.Shard(string(clientID), target.ShardCount)
	case config.KindRoundRobinQueues:
		ctrs := w.roundRobinCtrs[clientID]
		if ctrs == nil {
			ctrs = map[int]uint64{}
			w.roundRobinCtrs[clientID] = ctrs
		}
		shard := int(ctrs[idx] % uint64(target.ShardCount))
		ctrs[idx]++
		return shard
	default:
		return 0
	}
}

func (w *Worker) publishEOFs(env *inner.Envelope, emits []eof.EOFEmit) error {
	if err := w.flushClientBuffers(env.ClientID); err != nil {
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

func (w *Worker) serializeEOF(target OutputTarget, env *inner.Envelope, e eof.EOFEmit) (*middleware.Message, error) {
	if target.Kind == config.KindFinalQueue {
		return serialize(&inner.BatchMessage{
			Header:   w.outHeader(env.GatewayID, env.ClientID),
			ItemKind: inner.ResultRow,
			Items:    []inner.BatchItem{{QueryID: e.QueryID, Payload: []byte("EOF")}},
		})
	}
	return serialize(&inner.EOFMessage{
		Header:  w.outHeader(env.GatewayID, env.ClientID),
		QueryID: e.QueryID,
		Total:   e.Total,
	})
}

func (w *Worker) reenqueueUpstreamEOF(clientID inner.ClientID) error {
	cached, ok := w.upstreamEOFs[clientID]
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
	w.upstreamEOFs[clientID] = cachedEOF{inputIndex: inputIndex, msg: msg}
}

func (w *Worker) clearUpstreamEOF(clientID inner.ClientID) {
	delete(w.upstreamEOFs, clientID)
}

func (w *Worker) outHeader(gatewayID inner.GatewayID, clientID inner.ClientID) inner.Header {
	return inner.Header{
		GatewayID:       gatewayID,
		ClientID:        clientID,
		SeqID:           w.nextSeqID(clientID),
		SenderStageType: w.cfg.StageType,
		SenderReplicaID: uint16(w.cfg.ReplicaID),
	}
}

func (w *Worker) nextSeqID(clientID inner.ClientID) uint64 {
	w.outSeqID[clientID]++
	return w.outSeqID[clientID]
}

func (w *Worker) enqueuePending(clientID inner.ClientID, ref deliveryRef) {
	w.pendingAcks[clientID] = append(w.pendingAcks[clientID], ref)
	w.totalPending++
}

func (w *Worker) flushAllPending() {
	for clientID := range w.pendingAcks {
		w.checkpointAndAck(clientID)
	}
}

func (w *Worker) checkpointAndAck(clientID inner.ClientID) {
	if len(w.pendingAcks[clientID]) == 0 {
		return
	}
	if err := w.flushClientBuffers(clientID); err != nil {
		slog.Error("flushClientBuffers failed, exiting for redelivery", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	if err := w.flushPublishers(); err != nil {
		slog.Error("FlushPublisher failed, exiting for redelivery", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	cp := w.buildClientCheckpoint(clientID)
	if err := checkpoint.WriteClientCheckpoint(w.cfg.CheckpointDir, cp); err != nil {
		slog.Error("WriteClientCheckpoint failed, exiting before ACK", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	w.ackAllPending(clientID)
}

func (w *Worker) ackAllPending(clientID inner.ClientID) {
	refs := w.pendingAcks[clientID]
	for _, ref := range refs {
		ref.ack()
	}
	w.totalPending -= len(refs)
	delete(w.pendingAcks, clientID)
}

func (w *Worker) completeClient(clientID inner.ClientID, finalRef deliveryRef) {
	if err := w.flushClientBuffers(clientID); err != nil {
		slog.Error("flushClientBuffers failed on complete, exiting", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	if err := w.flushPublishers(); err != nil {
		slog.Error("FlushPublisher failed on complete, exiting", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	clientCheckpoint := w.buildClientCheckpoint(clientID)
	if err := checkpoint.WriteClientCheckpoint(w.cfg.CheckpointDir, clientCheckpoint); err != nil {
		slog.Error("WriteClientCheckpoint failed on complete, exiting before ACK", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	w.tombstones[clientID] = time.Now()
	w.flushMetaCheckpoint()
	w.ackAllPending(clientID)
	finalRef.ack()
	w.cleanupClientState(clientID)
	_ = os.Remove(checkpoint.ClientCheckpointPath(w.cfg.CheckpointDir, clientID))
}

func (w *Worker) abortClient(clientID inner.ClientID) {
	w.tombstones[clientID] = time.Now()
	w.flushMetaCheckpoint()
	w.ackAllPending(clientID)
	w.cleanupClientState(clientID)
	_ = os.Remove(checkpoint.ClientCheckpointPath(w.cfg.CheckpointDir, clientID))
}

func (w *Worker) cleanupClientState(clientID inner.ClientID) {
	if recoverableClientState, ok := w.strategy.(strategy.RecoverableStrategy); ok {
		recoverableClientState.CleanupClient(clientID)
	}
	for sequenceKey := range w.lastRecvSeqID {
		if sequenceKey.ClientID == clientID {
			delete(w.lastRecvSeqID, sequenceKey)
		}
	}
	delete(w.outSeqID, clientID)
	delete(w.roundRobinCtrs, clientID)
	delete(w.lastSeen, clientID)
	delete(w.upstreamEOFs, clientID)
}

func (w *Worker) flushMetaCheckpoint() {
	metaCheckpoint := &checkpoint.MetaCheckpoint{Tombstones: make(map[string]int64, len(w.tombstones))}
	for clientID, deathTimestamp := range w.tombstones {
		metaCheckpoint.Tombstones[string(clientID)] = deathTimestamp.Unix()
	}
	if err := checkpoint.WriteMetaCheckpoint(w.cfg.CheckpointDir, metaCheckpoint); err != nil {
		slog.Warn("WriteMetaCheckpoint failed", "err", err)
	}
}

func (w *Worker) runMaintenanceLoop() {
	defer w.waitingGroup.Done()
	ticker := time.NewTicker(w.cfg.MaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.stateMu.Lock()
			if !w.draining {
				w.runMaintenance()
			}
			w.stateMu.Unlock()
		}
	}
}

func (w *Worker) runMaintenance() {
	now := time.Now()
	for clientID, timestamp := range w.lastSeen {
		if now.Sub(timestamp) > w.cfg.ClientTTL {
			slog.Warn("CLIENT_TTL expired, aborting client", "client_id", clientID, "idle", now.Sub(timestamp))
			w.abortClient(clientID)
		}
	}
	changed := false
	for clientID, deathTimestamp := range w.tombstones {
		if now.Sub(deathTimestamp) > w.cfg.TombstoneTTL {
			delete(w.tombstones, clientID)
			changed = true
		}
	}
	if changed {
		w.flushMetaCheckpoint()
	}
}

func (w *Worker) loadCheckpoints() {
	metaCheckpoint, err := checkpoint.ReadMetaCheckpoint(w.cfg.CheckpointDir)
	if err != nil {
		slog.Warn("ReadMetaCheckpoint failed", "err", err)
	} else {
		for clientId, deathTimestamp := range metaCheckpoint.Tombstones {
			w.tombstones[inner.ClientID(clientId)] = time.Unix(deathTimestamp, 0)
		}
	}

	checkpoints, _ := os.ReadDir(w.cfg.CheckpointDir)
	for _, e := range checkpoints {
		name := e.Name()

		if !strings.HasPrefix(name, checkpointNamePrefix) || !strings.HasSuffix(name, ".json") {
			continue
		}
		checkpointPath := filepath.Join(w.cfg.CheckpointDir, name)
		clientCheckpoint, err := checkpoint.ReadClientCheckpoint(checkpointPath)
		if err != nil {
			slog.Warn("skip corrupt checkpoint", "file", name, "err", err)
			continue
		}
		clientID := inner.ClientID(clientCheckpoint.ClientID)
		if _, dead := w.tombstones[clientID]; dead {
			_ = os.Remove(checkpointPath)
			continue
		}
		for upstreamKey, sequenceId := range clientCheckpoint.LastRecvSeqID {
			sequenceKey, err := parseSeqKey(clientID, upstreamKey)
			if err != nil {
				slog.Warn("bad seq key in checkpoint", "file", name, "key", upstreamKey, "err", err)
				continue
			}
			w.lastRecvSeqID[sequenceKey] = sequenceId
		}
		w.outSeqID[clientID] = clientCheckpoint.OutSeqID
		if len(clientCheckpoint.RoundRobin) > 0 {
			ctrs := make(map[int]uint64, len(clientCheckpoint.RoundRobin))
			for k, v := range clientCheckpoint.RoundRobin {
				if idx, err := strconv.Atoi(k); err == nil {
					ctrs[idx] = v
				}
			}
			w.roundRobinCtrs[clientID] = ctrs
		}
		if recoverableStrategy, ok := w.strategy.(strategy.RecoverableStrategy); ok {
			if err := recoverableStrategy.UnmarshalClientState(clientID, clientCheckpoint.StrategyState); err != nil {
				slog.Warn("UnmarshalClientState failed", "client_id", clientID, "err", err)
			}
		}
		w.lastSeen[clientID] = time.Now()
	}
	if booster, ok := w.strategy.(strategy.SeqIDRecoverer); ok {
		for sk, maxSeq := range booster.BoostSeqIDs() {
			if maxSeq > w.lastRecvSeqID[sk] {
				w.lastRecvSeqID[sk] = maxSeq
			}
		}
	}
}

func (w *Worker) buildClientCheckpoint(clientID inner.ClientID) *checkpoint.ClientCheckpoint {
	clientCheckpoint := &checkpoint.ClientCheckpoint{
		ClientID:      string(clientID),
		OutSeqID:      w.outSeqID[clientID],
		LastRecvSeqID: w.serializeSeqMap(clientID),
	}
	if ctrs, ok := w.roundRobinCtrs[clientID]; ok && len(ctrs) > 0 {
		rr := make(map[string]uint64, len(ctrs))
		for outIdx, cnt := range ctrs {
			rr[strconv.Itoa(outIdx)] = cnt
		}
		clientCheckpoint.RoundRobin = rr
	}
	if recoverableStrategy, ok := w.strategy.(strategy.RecoverableStrategy); ok {
		data, err := recoverableStrategy.MarshalClientState(clientID)
		if err != nil {
			slog.Error("MarshalClientState failed, exiting before checkpoint", "client_id", clientID, "err", err)
			os.Exit(1)
		}
		clientCheckpoint.StrategyState = data
	}
	return clientCheckpoint
}

func seqKeyOf(clientID inner.ClientID, h inner.Header) inner.SeqKey {
	return inner.SeqKey{ClientID: clientID, StageType: h.SenderStageType, ReplicaID: h.SenderReplicaID}
}

func (w *Worker) serializeSeqMap(clientID inner.ClientID) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range w.lastRecvSeqID {
		if k.ClientID == clientID {
			out[fmt.Sprintf("%d:%d", k.StageType, k.ReplicaID)] = v
		}
	}
	return out
}

func parseSeqKey(clientID inner.ClientID, s string) (inner.SeqKey, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return inner.SeqKey{}, fmt.Errorf("malformed seq key %q", s)
	}
	st, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil {
		return inner.SeqKey{}, fmt.Errorf("malformed stage type in seq key %q: %w", s, err)
	}
	rid, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return inner.SeqKey{}, fmt.Errorf("malformed replica id in seq key %q: %w", s, err)
	}
	return inner.SeqKey{ClientID: clientID, StageType: uint8(st), ReplicaID: uint16(rid)}, nil
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

	w.stateMu.Lock()
	w.draining = true
	for clientID := range w.pendingAcks {
		w.checkpointAndAck(clientID)
	}
	w.flushMetaCheckpoint()
	if flusher, ok := w.strategy.(strategy.BufferFlusher); ok {
		flusher.CloseAllBuffers()
	}
	w.stateMu.Unlock()

	done := make(chan struct{})
	go func() {
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
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(w.cfg.AMQPCloseTimeout):
		slog.Warn("AMQP close timed out, forcing exit")
		os.Exit(0)
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
