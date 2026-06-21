package runtime

import (
	"fmt"
	"sort"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/dedup"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type capturingMiddleware struct {
	batches []*inner.BatchMessage
	keys    []string
}

func (m *capturingMiddleware) StartConsuming(func(middleware.Message, func(), func())) error {
	return nil
}
func (m *capturingMiddleware) StopConsuming() error              { return nil }
func (m *capturingMiddleware) FlushPublisher() error             { return nil }
func (m *capturingMiddleware) Close() error                      { return nil }
func (m *capturingMiddleware) Send(msg middleware.Message) error { return m.capture("", msg) }
func (m *capturingMiddleware) SendWithKey(msg middleware.Message, rk string) error {
	return m.capture(rk, msg)
}
func (m *capturingMiddleware) capture(rk string, msg middleware.Message) error {
	parsed, err := inner.NewFromSerializedData([]byte(msg.Body))
	if err != nil {
		return err
	}
	b, ok := parsed.(*inner.BatchMessage)
	if !ok {
		return fmt.Errorf("captured non-batch message %T", parsed)
	}
	m.batches = append(m.batches, b)
	m.keys = append(m.keys, rk)
	return nil
}

func TestEmitDataOutputsCarriesOriginIDAndBranch(t *testing.T) {
	rec0 := &capturingMiddleware{}
	rec1 := &capturingMiddleware{}
	w := &Worker{
		cfg: config.WorkerConfig{StageType: inner.StageFilterPeriod1, ReplicaID: 2},
		outputs: []OutputTarget{
			{Kind: config.KindBatchQueues, ShardCount: 1, Middlewares: []middleware.Middleware{rec0}},
			{Kind: config.KindBatchQueues, ShardCount: 1, Middlewares: []middleware.Middleware{rec1}},
		},
	}
	src := inner.Header{
		GatewayID:       1,
		ClientID:        inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd"),
		SeqID:           77,
		SenderStageType: inner.StageGateway,
		MinterStageType: inner.StageGateway,
	}
	outputs := []strategy.OutputMessage{
		{OutputIndices: []int{0}, Body: []byte("a"), ClientID: src.ClientID},
		{OutputIndices: []int{0}, Body: []byte("b"), ClientID: src.ClientID},
		{OutputIndices: []int{1}, Body: []byte("c"), ClientID: src.ClientID},
	}
	if err := w.emitDataOutputs(src, outputs); err != nil {
		t.Fatalf("emitDataOutputs: %v", err)
	}

	if len(rec0.batches) != 1 {
		t.Fatalf("output 0: %d batches, want 1", len(rec0.batches))
	}
	b0 := rec0.batches[0]
	if b0.SeqID != 77 {
		t.Fatalf("origin id re-minted: got %d want 77", b0.SeqID)
	}
	if len(b0.Items) != 2 {
		t.Fatalf("output 0: %d items, want 2 (merged)", len(b0.Items))
	}
	if len(b0.BranchPath) != 1 || b0.BranchPath[0] != 0 {
		t.Fatalf("output 0 branchPath=%v want [0]", b0.BranchPath)
	}
	if b0.SenderStageType != inner.StageFilterPeriod1 || b0.SenderReplicaID != 2 {
		t.Fatalf("sender not stamped: %d/%d", b0.SenderStageType, b0.SenderReplicaID)
	}
	if b0.MinterStageType != inner.StageGateway {
		t.Fatalf("minter root not carried: %d", b0.MinterStageType)
	}

	if len(rec1.batches) != 1 {
		t.Fatalf("output 1: %d batches, want 1", len(rec1.batches))
	}
	b1 := rec1.batches[0]
	if b1.SeqID != 77 || len(b1.BranchPath) != 1 || b1.BranchPath[0] != 1 {
		t.Fatalf("output 1: seq=%d branch=%v want seq=77 branch=[1]", b1.SeqID, b1.BranchPath)
	}
	if inner.IDSpaceOf(b0.Header) == inner.IDSpaceOf(b1.Header) {
		t.Fatal("las dos ramas no deben compartir idSpace")
	}
}

func TestDataDedupByIDSpace(t *testing.T) {
	w := &Worker{dedupSeen: map[inner.DedupKey]*dedup.IntervalSet{}}
	h := inner.Header{
		ClientID:        inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd"),
		SeqID:           5,
		MinterStageType: inner.StageGateway,
		BranchPath:      []uint8{0},
	}
	if w.isDuplicateData(h) {
		t.Fatal("set vacío no debe ser duplicado")
	}
	w.dedupAdd(h)
	if !w.isDuplicateData(h) {
		t.Fatal("debe ser duplicado tras dedupAdd")
	}

	otherBranch := h
	otherBranch.BranchPath = []uint8{1}
	if w.isDuplicateData(otherBranch) {
		t.Fatal("mismo origin id, otra rama → idSpace distinto → no duplicado")
	}

	otherSeq := h
	otherSeq.SeqID = 6
	if w.isDuplicateData(otherSeq) {
		t.Fatal("otro origin id → no duplicado")
	}
}

// recordingMiddleware captura los publishes (routing key + SeqID del header) para
// inspeccionar el orden de emisión.
type recordingMiddleware struct {
	sends []recordedSend
}

type recordedSend struct {
	routingKey string
	seqID      uint64
}

func (m *recordingMiddleware) StartConsuming(func(middleware.Message, func(), func())) error {
	return nil
}
func (m *recordingMiddleware) StopConsuming() error  { return nil }
func (m *recordingMiddleware) FlushPublisher() error { return nil }
func (m *recordingMiddleware) Close() error          { return nil }
func (m *recordingMiddleware) Send(msg middleware.Message) error {
	return m.record("", msg)
}
func (m *recordingMiddleware) SendWithKey(msg middleware.Message, routingKey string) error {
	return m.record(routingKey, msg)
}
func (m *recordingMiddleware) record(routingKey string, msg middleware.Message) error {
	parsed, err := inner.NewFromSerializedData([]byte(msg.Body))
	if err != nil {
		return err
	}
	m.sends = append(m.sends, recordedSend{routingKey: routingKey, seqID: inner.HeaderOf(parsed).SeqID})
	return nil
}

// TestFlushClientBuffersAssignsSeqIDsDeterministically verifica el invariante del
// hallazgo #1: el SeqID de cada batch debe asignarse en un orden determinístico
// (independiente de la iteración aleatoria de maps de Go), para que un replay tras
// crash reproduzca exactamente la misma asignación y el dedup downstream funcione.
func TestFlushClientBuffersAssignsSeqIDsDeterministically(t *testing.T) {
	clientID := inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd")
	routingKeys := []string{"k7", "k3", "k9", "k0", "k5", "k1", "k8", "k2", "k6", "k4"}

	build := func() (*Worker, *recordingMiddleware) {
		rec := &recordingMiddleware{}
		target := OutputTarget{
			Kind:        config.KindShardedQueues,
			ShardCount:  1,
			Middlewares: []middleware.Middleware{rec},
		}
		perShard := []map[batchKey]*pendingBatch{{}}
		for _, rk := range routingKeys {
			k := batchKey{clientID: clientID, routingKey: rk, itemKind: inner.TransactionMessage, queryID: 1}
			perShard[0][k] = &pendingBatch{
				clientID:   clientID,
				routingKey: rk,
				itemKind:   inner.TransactionMessage,
				queryID:    1,
				items:      [][]byte{[]byte("x")},
				bytes:      1,
			}
		}
		w := &Worker{
			cfg:                 config.WorkerConfig{StageType: 1, ReplicaID: 0},
			outSeqID:            map[inner.ClientID]uint64{},
			outputTargetBuffers: []*outputTargetBuffer{{target: target, perShard: perShard}},
		}
		return w, rec
	}

	sortedRK := append([]string(nil), routingKeys...)
	sort.Strings(sortedRK)

	w1, rec1 := build()
	if err := w1.flushClientBuffers(clientID); err != nil {
		t.Fatalf("flushClientBuffers run 1: %v", err)
	}
	if len(rec1.sends) != len(routingKeys) {
		t.Fatalf("run 1 emitió %d batches, want %d", len(rec1.sends), len(routingKeys))
	}
	for i, s := range rec1.sends {
		if s.routingKey != sortedRK[i] {
			t.Fatalf("emisión %d: routingKey=%q, want %q (no se emitió en orden ordenado)", i, s.routingKey, sortedRK[i])
		}
		if s.seqID != uint64(i+1) {
			t.Fatalf("emisión %d (rk=%q): seqID=%d, want %d", i, s.routingKey, s.seqID, i+1)
		}
	}

	// Una segunda corrida independiente debe reproducir la misma asignación
	// routingKey→SeqID, pese a la aleatoriedad de iteración de maps.
	w2, rec2 := build()
	if err := w2.flushClientBuffers(clientID); err != nil {
		t.Fatalf("flushClientBuffers run 2: %v", err)
	}
	for i := range rec1.sends {
		if rec1.sends[i] != rec2.sends[i] {
			t.Fatalf("la corrida 2 difiere en %d: %+v vs %+v", i, rec2.sends[i], rec1.sends[i])
		}
	}
}

func TestRetryUpstreamEOFMessageBumpsSeqPastLastReceived(t *testing.T) {
	clientID := inner.ClientID("ef87cae8-a5dd-416e-9fe0-f9f4c9e8c279")
	original := &inner.EOFMessage{
		Header: inner.Header{
			GatewayID:       3,
			ClientID:        clientID,
			SeqID:           7,
			SenderStageType: inner.StageFilterCurrencyUsdP2,
			SenderReplicaID: 1,
		},
		QueryID: 3,
		Total:   42,
	}
	msg, err := serialize(original)
	if err != nil {
		t.Fatalf("serialize original EOF: %v", err)
	}

	worker := &Worker{
		lastRecvSeqID: map[inner.SeqKey]uint64{
			{
				ClientID:  clientID,
				StageType: inner.StageFilterCurrencyUsdP2,
				ReplicaID: 1,
			}: 9,
		},
	}

	retry, err := worker.retryUpstreamEOFMessage(clientID, *msg)
	if err != nil {
		t.Fatalf("retryUpstreamEOFMessage: %v", err)
	}
	parsed, err := inner.NewFromSerializedData([]byte(retry.Body))
	if err != nil {
		t.Fatalf("parse retry EOF: %v", err)
	}
	eofMessage, ok := parsed.(*inner.EOFMessage)
	if !ok {
		t.Fatalf("retry message type = %T, want *inner.EOFMessage", parsed)
	}

	if eofMessage.SeqID != 10 {
		t.Fatalf("retry SeqID = %d, want 10", eofMessage.SeqID)
	}
	if eofMessage.ClientID != clientID ||
		eofMessage.GatewayID != original.GatewayID ||
		eofMessage.SenderStageType != original.SenderStageType ||
		eofMessage.SenderReplicaID != original.SenderReplicaID ||
		eofMessage.QueryID != original.QueryID ||
		eofMessage.Total != original.Total {
		t.Fatalf("retry EOF changed fields: got %+v want original %+v with bumped SeqID", eofMessage, original)
	}
}

func TestDirectExchangeInputQueueNameIsStable(t *testing.T) {
	got := directExchangeInputQueueName("results", "0")
	if got != "direct_results_0" {
		t.Fatalf("directExchangeInputQueueName() = %q, want %q", got, "direct_results_0")
	}
}

func TestBatchDataShardStableBySeqID(t *testing.T) {
	clientID := inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd")
	worker := &Worker{}
	target := OutputTarget{
		Kind:       config.KindBatchQueues,
		ShardCount: 3,
	}
	output := strategy.OutputMessage{
		ClientID:      clientID,
		Body:          []byte("tx|account-a|account-b|123.45"),
		RoutingKey:    "matched",
		BatchItemKind: inner.TransactionMessage,
		BatchQueryID:  1,
	}

	firstShard := worker.dataShard(target, output, 77)
	for i := 0; i < 10; i++ {
		if got := worker.dataShard(target, output, 77); got != firstShard {
			t.Fatalf("dataShard cambió de %d a %d para el mismo batch (SeqID=77)", firstShard, got)
		}
	}
	other := output
	other.Body = []byte("contenido totalmente distinto")
	if got := worker.dataShard(target, other, 77); got != firstShard {
		t.Fatalf("el shard por-batch no debe depender del contenido: %d != %d", got, firstShard)
	}
}
