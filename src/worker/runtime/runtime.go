package runtime

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/checkpoint"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/dedup"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/heartbeat"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/wal"
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
	firstPendingAt map[inner.ClientID]time.Time
	totalPending   int
	lastSeen       map[inner.ClientID]time.Time
	tombstones     map[inner.ClientID]time.Time
	lastRecvSeqID map[inner.SeqKey]uint64
	dedupSeen map[inner.DedupKey]*dedup.IntervalSet
	processedItems map[inner.ClientID]uint64
	outSeqID map[inner.ClientID]uint64

	deltaCP       strategy.DeltaCheckpointer
	wals          map[inner.ClientID]*wal.Log
	walGen        map[inner.ClientID]uint64
	pendingDeltas map[inner.ClientID][][]byte
	lastSnapSize  map[inner.ClientID]int64
}

const walCompactionFloorBytes = 1 << 20

type walDelta struct {
	IDSpace   string `json:"is"`
	SeqID     uint64 `json:"s"`
	ItemCount uint32 `json:"n"`
	Delta     []byte `json:"d,omitempty"`
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
		firstPendingAt: map[inner.ClientID]time.Time{},
		lastSeen:       map[inner.ClientID]time.Time{},
		tombstones:     map[inner.ClientID]time.Time{},
		lastRecvSeqID:  map[inner.SeqKey]uint64{},
		dedupSeen:      map[inner.DedupKey]*dedup.IntervalSet{},
		processedItems: map[inner.ClientID]uint64{},
		outSeqID:       map[inner.ClientID]uint64{},
		wals:           map[inner.ClientID]*wal.Log{},
		walGen:         map[inner.ClientID]uint64{},
		pendingDeltas:  map[inner.ClientID][][]byte{},
		lastSnapSize:   map[inner.ClientID]int64{},
	}
	if dcp, ok := strat.(strategy.DeltaCheckpointer); ok && cfg.WALEnabled {
		w.deltaCP = dcp
	}

	foundCheckpoints := w.loadCheckpoints()
	w.deferredInputs = deferredInputsForStrategy(strat)
	if foundCheckpoints {
		w.deferredInputs = map[int]bool{}
	}
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
		if target.Kind == config.KindShardedQueues || target.Kind == config.KindBatchQueues {
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

	switch typed := parsed.(type) {
	case *inner.EOFMessage:
		if w.isDuplicateEOF(clientID, header) {
			ack()
			return
		}
		w.lastSeen[clientID] = time.Now()
		w.handleUpstreamEOF(typed, msg, inputIndex, header, ack, nack)
	case *inner.BatchMessage:
		if w.isDuplicateData(header) {
			ack()
			return
		}
		w.lastSeen[clientID] = time.Now()
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
	if w.isDuplicateEOF(clientID, header) {
		ack()
		return
	}
	w.lastSeen[clientID] = time.Now()

	w.handleRingToken(typed, header, ack, nack)
}

func (w *Worker) isDuplicateEOF(clientID inner.ClientID, header inner.Header) bool {
	if header.SeqID == 0 {
		return false
	}
	return header.SeqID <= w.lastRecvSeqID[seqKeyOf(clientID, header)]
}

func (w *Worker) isDuplicateData(header inner.Header) bool {
	set := w.dedupSeen[inner.DedupKeyOf(header)]
	return set != nil && header.SeqID != 0 && set.Contains(header.SeqID)
}

func (w *Worker) dedupAdd(header inner.Header) {
	if header.SeqID == 0 {
		return
	}
	key := inner.DedupKeyOf(header)
	set := w.dedupSeen[key]
	if set == nil {
		set = &dedup.IntervalSet{}
		w.dedupSeen[key] = set
	}
	set.Add(header.SeqID)
}

func (w *Worker) handleBatch(batch *inner.BatchMessage, raw []byte, inputIndex int, header inner.Header, ack, nack func()) {
	completed, err := w.processBatch(batch, raw, inputIndex)
	if err != nil {
		slog.Error("processBatch failed, exiting for redelivery", "client_id", batch.ClientID, "err", err)
		os.Exit(1)
	}
	w.dedupAdd(header)
	w.processedItems[header.ClientID] += uint64(len(batch.Items))
	if w.deltaCP != nil {
		w.bufferDelta(header, len(batch.Items))
	}
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
		LocalCount:      w.processedItems[typed.ClientID],
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
	outcome, err := w.strategy.OnRingToken(token, w.processedItems[token.ClientID])
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
	var dataOutputs []strategy.OutputMessage
	if rawStrat, ok := w.strategy.(strategy.RawBatchStrategy); ok {
		outputs, _, handled, err := rawStrat.ProcessRawBatch(batch, rawBatch, inputIndex)
		if err != nil {
			if errors.Is(err, strategy.ErrInvalidData) {
				slog.Warn("RawBatchStrategy: invalid batch, discarding", "client_id", batch.ClientID, "err", err)
				return false, nil
			}
			return false, err
		}
		dataOutputs = append(dataOutputs, outputs...)
		if handled {
			return false, w.emitDataOutputs(batch.Header, dataOutputs)
		}
	}

	emitter, hasEmitter := w.strategy.(strategy.ReadyEOFEmitter)
	var readyEnv *inner.Envelope
	var readyOutcome strategy.EOFOutcome
	for _, item := range batch.Items {
		env := &inner.Envelope{
			Kind:            batch.ItemKind,
			GatewayID:       batch.GatewayID,
			ClientID:        batch.ClientID,
			QueryID:         item.QueryID,
			Payload:         item.Payload,
			InputIndex:      inputIndex,
			SenderStageType: batch.SenderStageType,
			SenderReplicaID: batch.SenderReplicaID,
		}
		if err := w.strategy.Validate(env); err != nil {
			if errors.Is(err, strategy.ErrInvalidData) {
				slog.Warn("Strategy.Validate: invalid data, discarding", "client_id", env.ClientID, "err", err)
				continue
			}
			return false, err
		}
		outputs, _, err := w.strategy.ProcessMessage(env)
		if err != nil {
			return false, err
		}
		dataOutputs = append(dataOutputs, outputs...)
		if hasEmitter && readyEnv == nil {
			if outcome, ready := emitter.ReadyEOFs(env); ready {
				readyEnv = env
				readyOutcome = outcome
			}
		}
	}

	if err := w.emitDataOutputs(batch.Header, dataOutputs); err != nil {
		return false, err
	}
	if readyEnv != nil {
		if err := w.applyEOFOutcome(readyEnv, readyOutcome); err != nil {
			return false, err
		}
		return readyOutcome.ClientCompleted, nil
	}
	return false, nil
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
	shard := w.dataShard(buf.target, om, 0)
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
			// Orden determinístico: el SeqID se asigna en este orden, y un replay
			// tras crash debe reproducir exactamente la misma asignación para que
			// el dedup high-water-mark downstream reconozca los duplicados.
			slices.SortFunc(keys, func(a, b batchKey) int {
				if a.itemKind != b.itemKind {
					return cmp.Compare(a.itemKind, b.itemKind)
				}
				if a.queryID != b.queryID {
					return cmp.Compare(a.queryID, b.queryID)
				}
				return cmp.Compare(a.routingKey, b.routingKey)
			})
			for _, k := range keys {
				if err := w.flushBatch(idx, shard, k); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (w *Worker) emitDataOutputs(src inner.Header, outputs []strategy.OutputMessage) error {
	if len(outputs) == 0 {
		return nil
	}
	type groupKey struct {
		outputIdx int
		dest      string
		itemKind  inner.MsgKind
	}
	type dataGroup struct {
		mwIndex    int
		routingKey string
		itemKind   inner.MsgKind
		outputIdx  int
		items      []inner.BatchItem
	}
	groups := map[groupKey]*dataGroup{}
	var order []groupKey
	for _, om := range outputs {
		for _, idx := range om.OutputIndices {
			if idx < 0 || idx >= len(w.outputs) {
				return errors.New("strategy returned invalid output index")
			}
			target := w.outputs[idx]
			shard := w.dataShard(target, om, src.SeqID)
			dest, routingKey, mwIndex := destinationFor(target, shard, om.RoutingKey)
			itemKind := om.BatchItemKind
			if itemKind == 0 {
				itemKind = inner.TransactionMessage
			}
			if target.Kind == config.KindFinalQueue {
				itemKind = inner.ResultRow
			}
			k := groupKey{outputIdx: idx, dest: dest, itemKind: itemKind}
			g := groups[k]
			if g == nil {
				g = &dataGroup{mwIndex: mwIndex, routingKey: routingKey, itemKind: itemKind, outputIdx: idx}
				groups[k] = g
				order = append(order, k)
			}
			g.items = append(g.items, inner.BatchItem{QueryID: om.BatchQueryID, Payload: om.Body})
		}
	}
	multiOutput := len(w.outputs) > 1
	for _, k := range order {
		g := groups[k]
		branchPath := src.BranchPath
		if multiOutput {
			branchPath = inner.WithBranch(src.BranchPath, g.outputIdx)
		}
		msg, err := serialize(&inner.BatchMessage{
			Header: inner.Header{
				GatewayID:       src.GatewayID,
				ClientID:        src.ClientID,
				SeqID:           src.SeqID,
				SenderStageType: w.cfg.StageType,
				SenderReplicaID: uint16(w.cfg.ReplicaID),
				MinterStageType: src.MinterStageType,
				MinterReplicaID: src.MinterReplicaID,
				BranchPath:      branchPath,
			},
			ItemKind: g.itemKind,
			Items:    g.items,
		})
		if err != nil {
			return err
		}
		target := w.outputs[g.outputIdx]
		if g.mwIndex < 0 || g.mwIndex >= len(target.Middlewares) {
			return errors.New("data emission: shard index out of range")
		}
		if err := sendOnTarget(target.Middlewares[g.mwIndex], *msg, g.routingKey); err != nil {
			return err
		}
	}
	return nil
}

func destinationFor(target OutputTarget, shard int, omRoutingKey string) (dest, routingKey string, mwIndex int) {
	switch target.Kind {
	case config.KindShardedQueues, config.KindBatchQueues:
		return strconv.Itoa(shard), "", shard
	default:
		return omRoutingKey, omRoutingKey, 0
	}
}

func (w *Worker) shardFor(target OutputTarget, clientID inner.ClientID) int {
	if target.Kind == config.KindShardedQueues || target.Kind == config.KindBatchQueues {
		return hashing.Shard(string(clientID), target.ShardCount)
	}
	return 0
}

func (w *Worker) dataShard(target OutputTarget, om strategy.OutputMessage, seqID uint64) int {
	switch target.Kind {
	case config.KindShardedQueues:
		return hashing.Shard(string(om.ClientID), target.ShardCount)
	case config.KindBatchQueues:
		return hashing.Shard(string(om.ClientID)+"|"+strconv.FormatUint(seqID, 10), target.ShardCount)
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
	msg, err := w.retryUpstreamEOFMessage(clientID, cached.msg)
	if err != nil {
		return err
	}
	return w.inputs[cached.inputIndex].Send(msg)
}

func (w *Worker) retryUpstreamEOFMessage(clientID inner.ClientID, msg middleware.Message) (middleware.Message, error) {
	parsed, err := inner.NewFromSerializedData([]byte(msg.Body))
	if err != nil {
		return middleware.Message{}, fmt.Errorf("parse cached upstream EOF: %w", err)
	}
	eofMessage, ok := parsed.(*inner.EOFMessage)
	if !ok {
		return middleware.Message{}, errors.New("cached upstream EOF is not an EOF message")
	}
	if eofMessage.ClientID != clientID {
		return middleware.Message{}, fmt.Errorf("cached upstream EOF client mismatch: cached=%s expected=%s", eofMessage.ClientID, clientID)
	}

	key := seqKeyOf(clientID, eofMessage.Header)
	// Re-enqueued EOFs are internal retries. The original EOF may already be in
	// lastRecvSeqID, so the retry needs a fresh seq while preserving sender ID.
	nextSeqID := eofMessage.SeqID + 1
	if nextSeqID == 0 {
		return middleware.Message{}, errors.New("cached upstream EOF seq id overflow")
	}
	if last := w.lastRecvSeqID[key]; last >= nextSeqID {
		if last == ^uint64(0) {
			return middleware.Message{}, errors.New("last received seq id overflow")
		}
		nextSeqID = last + 1
	}
	eofMessage.SeqID = nextSeqID

	retry, err := serialize(eofMessage)
	if err != nil {
		return middleware.Message{}, err
	}
	return *retry, nil
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
		MinterStageType: w.cfg.StageType,
		MinterReplicaID: uint16(w.cfg.ReplicaID),
	}
}

func (w *Worker) nextSeqID(clientID inner.ClientID) uint64 {
	w.outSeqID[clientID]++
	return w.outSeqID[clientID]
}

func (w *Worker) enqueuePending(clientID inner.ClientID, ref deliveryRef) {
	if _, ok := w.firstPendingAt[clientID]; !ok {
		w.firstPendingAt[clientID] = time.Now()
	}
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
	if w.deltaCP != nil {
		w.walCheckpoint(clientID)
	} else {
		cp := w.buildClientCheckpoint(clientID)
		if err := checkpoint.WriteClientCheckpoint(w.cfg.CheckpointDir, cp); err != nil {
			slog.Error("WriteClientCheckpoint failed, exiting before ACK", "client_id", clientID, "err", err)
			os.Exit(1)
		}
		w.writeGlobalState()
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
	delete(w.firstPendingAt, clientID)
}

func (w *Worker) writeGlobalState() {
	if gsp, ok := w.strategy.(strategy.GlobalStateProvider); ok {
		data, err := gsp.MarshalGlobalState()
		if err != nil {
			slog.Error("MarshalGlobalState failed, exiting before ACK", "err", err)
			os.Exit(1)
		}
		if err := checkpoint.WriteGlobalState(w.cfg.CheckpointDir, data); err != nil {
			slog.Error("WriteGlobalState failed, exiting before ACK", "err", err)
			os.Exit(1)
		}
	}
}


func walBaseFor(clientID inner.ClientID) string {
	return checkpointNamePrefix + string(clientID)
}

func (w *Worker) bufferDelta(header inner.Header, itemCount int) {
	rec := walDelta{
		IDSpace:   inner.IDSpaceOf(header),
		SeqID:     header.SeqID,
		ItemCount: uint32(itemCount),
		Delta:     w.deltaCP.TakeDelta(header.ClientID),
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		slog.Error("marshal wal delta failed, exiting", "client_id", header.ClientID, "err", err)
		os.Exit(1)
	}
	w.pendingDeltas[header.ClientID] = append(w.pendingDeltas[header.ClientID], payload)
}

func (w *Worker) walFor(clientID inner.ClientID) *wal.Log {
	if l, ok := w.wals[clientID]; ok {
		return l
	}
	l, err := wal.Open(w.cfg.CheckpointDir, walBaseFor(clientID), w.walGen[clientID])
	if err != nil {
		slog.Error("wal open failed, exiting", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	w.wals[clientID] = l
	return l
}

func (w *Worker) walCheckpoint(clientID inner.ClientID) {
	log := w.walFor(clientID)
	if w.lastSnapSize[clientID] == 0 {
		w.writeWALSnapshot(clientID, log.Gen())
		w.pendingDeltas[clientID] = nil
		return
	}
	if recs := w.pendingDeltas[clientID]; len(recs) > 0 {
		if err := log.AppendBatch(recs); err != nil {
			slog.Error("wal append failed, exiting before ACK", "client_id", clientID, "err", err)
			os.Exit(1)
		}
		w.pendingDeltas[clientID] = nil
	}
	threshold := int64(w.cfg.WALCompactionFactor) * w.lastSnapSize[clientID]
	if threshold < walCompactionFloorBytes {
		threshold = walCompactionFloorBytes
	}
	if log.Size() >= threshold {
		w.compactWAL(clientID, log)
	}
}

func (w *Worker) writeWALSnapshot(clientID inner.ClientID, gen uint64) {
	w.walGen[clientID] = gen
	cp := w.buildClientCheckpoint(clientID)
	data, err := json.Marshal(cp)
	if err != nil {
		slog.Error("marshal wal snapshot failed, exiting", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	if err := checkpoint.WriteClientCheckpoint(w.cfg.CheckpointDir, cp); err != nil {
		slog.Error("WriteClientCheckpoint (wal snapshot) failed, exiting before ACK", "client_id", clientID, "err", err)
		os.Exit(1)
	}
	w.writeGlobalState()
	w.lastSnapSize[clientID] = int64(len(data))
}

func (w *Worker) compactWAL(clientID inner.ClientID, log *wal.Log) {
	w.writeWALSnapshot(clientID, log.Gen()+1)
	if _, err := log.Rotate(); err != nil {
		slog.Error("wal rotate failed, exiting", "client_id", clientID, "err", err)
		os.Exit(1)
	}
}

func (w *Worker) replayWAL(clientID inner.ClientID, gen uint64) {
	err := wal.Replay(w.cfg.CheckpointDir, walBaseFor(clientID), gen, func(payload []byte) error {
		var rec walDelta
		if err := json.Unmarshal(payload, &rec); err != nil {
			return err
		}
		if len(rec.Delta) > 0 {
			if err := w.deltaCP.ApplyDelta(clientID, rec.Delta); err != nil {
				return err
			}
		}
		w.dedupAddKeyed(clientID, rec.IDSpace, rec.SeqID)
		w.processedItems[clientID] += uint64(rec.ItemCount)
		return nil
	})
	if err != nil {
		slog.Error("wal replay failed, exiting", "client_id", clientID, "err", err)
		os.Exit(1)
	}
}

func (w *Worker) dedupAddKeyed(clientID inner.ClientID, idSpace string, seqID uint64) {
	if seqID == 0 {
		return
	}
	key := inner.DedupKey{ClientID: clientID, IDSpace: idSpace}
	set := w.dedupSeen[key]
	if set == nil {
		set = &dedup.IntervalSet{}
		w.dedupSeen[key] = set
	}
	set.Add(seqID)
}

func (w *Worker) closeWAL(clientID inner.ClientID) {
	if w.deltaCP == nil {
		return
	}
	if l, ok := w.wals[clientID]; ok {
		_ = l.Close()
		delete(w.wals, clientID)
	}
	_ = wal.Remove(w.cfg.CheckpointDir, walBaseFor(clientID))
	delete(w.walGen, clientID)
	delete(w.pendingDeltas, clientID)
	delete(w.lastSnapSize, clientID)
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
	w.writeGlobalState()
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
	w.closeWAL(clientID)
	if recoverableClientState, ok := w.strategy.(strategy.RecoverableStrategy); ok {
		recoverableClientState.CleanupClient(clientID)
	}
	for sequenceKey := range w.lastRecvSeqID {
		if sequenceKey.ClientID == clientID {
			delete(w.lastRecvSeqID, sequenceKey)
		}
	}
	for dedupKey := range w.dedupSeen {
		if dedupKey.ClientID == clientID {
			delete(w.dedupSeen, dedupKey)
		}
	}
	delete(w.outSeqID, clientID)
	delete(w.processedItems, clientID)
	delete(w.lastSeen, clientID)
	delete(w.upstreamEOFs, clientID)
	delete(w.firstPendingAt, clientID)
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
	for clientID, timestamp := range w.firstPendingAt {
		if now.Sub(timestamp) > w.cfg.MaxPendingAckAge {
			w.checkpointAndAck(clientID)
		}
	}
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

func (w *Worker) loadCheckpoints() bool {
	metaCheckpoint, err := checkpoint.ReadMetaCheckpoint(w.cfg.CheckpointDir)
	if err != nil {
		slog.Warn("ReadMetaCheckpoint failed", "err", err)
	} else {
		for clientId, deathTimestamp := range metaCheckpoint.Tombstones {
			w.tombstones[inner.ClientID(clientId)] = time.Unix(deathTimestamp, 0)
		}
	}

	foundAnyCheckpoint := false
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
		for idSpace, set := range clientCheckpoint.DedupSeen {
			if set == nil {
				continue
			}
			w.dedupSeen[inner.DedupKey{ClientID: clientID, IDSpace: idSpace}] = set
		}
		w.outSeqID[clientID] = clientCheckpoint.OutSeqID
		w.processedItems[clientID] = clientCheckpoint.ProcessedItems
		if recoverableStrategy, ok := w.strategy.(strategy.RecoverableStrategy); ok {
			if err := recoverableStrategy.UnmarshalClientState(clientID, clientCheckpoint.StrategyState); err != nil {
				slog.Warn("UnmarshalClientState failed", "client_id", clientID, "err", err)
			}
		}
		if w.deltaCP != nil {
			w.walGen[clientID] = clientCheckpoint.WALGen
			w.replayWAL(clientID, clientCheckpoint.WALGen)
			if info, err := os.Stat(checkpointPath); err == nil {
				w.lastSnapSize[clientID] = info.Size()
			}
		}
		if len(clientCheckpoint.PendingEOFBody) > 0 {
			w.upstreamEOFs[clientID] = cachedEOF{
				inputIndex: clientCheckpoint.PendingEOFInputIdx,
				msg:        middleware.Message{Body: string(clientCheckpoint.PendingEOFBody)},
			}
		}
		w.lastSeen[clientID] = time.Now()
		foundAnyCheckpoint = true
	}
	if recoverer, ok := w.strategy.(strategy.DedupRecoverer); ok {
		for _, h := range recoverer.RecoveredDedupHeaders() {
			w.dedupAdd(h)
		}
	}

	// Restore worker-level global state (e.g., exchange-rate tables).
	if gsp, ok := w.strategy.(strategy.GlobalStateProvider); ok {
		data, err := checkpoint.ReadGlobalState(w.cfg.CheckpointDir)
		if err != nil {
			slog.Warn("ReadGlobalState failed", "err", err)
		} else if data != nil {
			if err := gsp.UnmarshalGlobalState(data); err != nil {
				slog.Warn("UnmarshalGlobalState failed", "err", err)
			}
		}
	}

	return foundAnyCheckpoint
}

func (w *Worker) buildClientCheckpoint(clientID inner.ClientID) *checkpoint.ClientCheckpoint {
	clientCheckpoint := &checkpoint.ClientCheckpoint{
		ClientID:       string(clientID),
		OutSeqID:       w.outSeqID[clientID],
		LastRecvSeqID:  w.serializeSeqMap(clientID),
		DedupSeen:      w.serializeDedup(clientID),
		ProcessedItems: w.processedItems[clientID],
		WALGen:         w.walGen[clientID],
	}
	if recoverableStrategy, ok := w.strategy.(strategy.RecoverableStrategy); ok {
		data, err := recoverableStrategy.MarshalClientState(clientID)
		if err != nil {
			slog.Error("MarshalClientState failed, exiting before checkpoint", "client_id", clientID, "err", err)
			os.Exit(1)
		}
		clientCheckpoint.StrategyState = data
	}
	if cached, ok := w.upstreamEOFs[clientID]; ok {
		clientCheckpoint.PendingEOFBody = []byte(cached.msg.Body)
		clientCheckpoint.PendingEOFInputIdx = cached.inputIndex
	}
	return clientCheckpoint
}

func seqKeyOf(clientID inner.ClientID, h inner.Header) inner.SeqKey {
	return inner.SeqKey{ClientID: clientID, StageType: h.SenderStageType, ReplicaID: h.SenderReplicaID}
}

func (w *Worker) serializeDedup(clientID inner.ClientID) map[string]*dedup.IntervalSet {
	out := map[string]*dedup.IntervalSet{}
	for k, set := range w.dedupSeen {
		if k.ClientID == clientID {
			out[k.IDSpace] = set
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
