package runtime

import (
	"sort"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

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

func TestContentHashDataShardIsStableForSameMessage(t *testing.T) {
	clientID := inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd")
	worker := &Worker{}
	target := OutputTarget{
		Kind:       config.KindContentHashQueues,
		ShardCount: 3,
	}
	output := strategy.OutputMessage{
		ClientID:      clientID,
		Body:          []byte("tx|account-a|account-b|123.45"),
		RoutingKey:    "matched",
		BatchItemKind: inner.TransactionMessage,
		BatchQueryID:  1,
	}

	firstShard := worker.dataShard(2, target, output)
	for i := 0; i < 10; i++ {
		if got := worker.dataShard(2, target, output); got != firstShard {
			t.Fatalf("dataShard() changed from %d to %d for the same message", firstShard, got)
		}
	}
}
