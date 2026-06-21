package pathfinder

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const pfClient = inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd")

func inTx(t *testing.T, a, m string) *inner.Envelope  { return shardedTx(t, a, m, false) }
func outTx(t *testing.T, m, b string) *inner.Envelope { return shardedTx(t, m, b, true) }

func shardedTx(t *testing.T, from, to string, bySource bool) *inner.Envelope {
	t.Helper()
	b, err := inner.SerializeShardedTx(&inner.ShardedTx{
		FromBank: 1, FromAccount: from, ToBank: 1, ToAccount: to, ShardedBySource: bySource,
	})
	if err != nil {
		t.Fatalf("serialize sharded tx: %v", err)
	}
	return &inner.Envelope{Kind: inner.ShardedTxMessage, ClientID: pfClient, Payload: b}
}

func runPathfinder(t *testing.T, txs []*inner.Envelope) []string {
	t.Helper()
	t.Setenv("K_COUNTERS", "1")
	t.Setenv("N_SUSPICIOUS_FILTERS", "1")
	p := New()
	if err := p.Init(strategy.StrategyConfig{OutputCount: 1}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for _, e := range txs {
		if _, _, err := p.ProcessMessage(e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	out, err := p.OnUpstreamEOF(&inner.Envelope{ClientID: pfClient, SenderStageType: 11, SenderReplicaID: 0})
	if err != nil {
		t.Fatalf("eof: %v", err)
	}
	if out.OutputsIterator == nil {
		return nil
	}
	var triples []string
	for om := range out.OutputsIterator {
		sp, err := inner.DeserializeSuspiciousPath(om.Body)
		if err != nil {
			t.Fatalf("deserialize triple: %v", err)
		}
		triples = append(triples, fmt.Sprintf("%s->%s->%s", sp.SourceAccount, sp.IntermediateAccount, sp.DestAccount))
	}
	return triples
}

func TestPathfinderEmissionIsOrderIndependent(t *testing.T) {
	order1 := []*inner.Envelope{inTx(t, "A1", "M"), outTx(t, "M", "B1"), inTx(t, "A2", "M"), outTx(t, "M", "B2")}
	order2 := []*inner.Envelope{outTx(t, "M", "B2"), inTx(t, "A2", "M"), outTx(t, "M", "B1"), inTx(t, "A1", "M")}
	order3 := []*inner.Envelope{inTx(t, "A1", "M"), inTx(t, "A1", "M"), outTx(t, "M", "B1"), outTx(t, "M", "B2"), inTx(t, "A2", "M"), outTx(t, "M", "B1")}

	r1 := runPathfinder(t, order1)
	r2 := runPathfinder(t, order2)
	r3 := runPathfinder(t, order3)

	if len(r1) != 4 {
		t.Fatalf("esperaba 4 triples, got %d (%v)", len(r1), r1)
	}
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("emisión depende del orden:\n order1=%v\n order2=%v", r1, r2)
	}
	if !reflect.DeepEqual(r1, r3) {
		t.Fatalf("emisión depende de duplicados:\n order1=%v\n order3=%v", r1, r3)
	}
}
