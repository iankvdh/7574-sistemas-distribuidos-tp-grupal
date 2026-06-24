package counter

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const ctrClient = inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd")

func suspiciousPath(t *testing.T, src, mid, dst string) *inner.Envelope {
	t.Helper()
	b, err := inner.SerializeSuspiciousPath(&inner.SuspiciousPath{
		SourceBank: 1, SourceAccount: src,
		IntermediateBank: 1, IntermediateAccount: mid,
		DestBank: 1, DestAccount: dst,
	})
	if err != nil {
		t.Fatalf("serialize suspicious path: %v", err)
	}
	return &inner.Envelope{Kind: inner.SuspiciousPathMessage, ClientID: ctrClient, Payload: b}
}

func runCounter(t *testing.T, paths []*inner.Envelope) []string {
	t.Helper()
	t.Setenv("N_PATH_FINDERS", "1")
	t.Setenv("N_FINAL_JOINERS", "1")
	t.Setenv("MIN_INTERMEDIATES", "2")
	c := New()
	if err := c.Init(strategy.StrategyConfig{OutputCount: 1}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for _, e := range paths {
		if _, _, err := c.ProcessMessage(e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	out, err := c.OnUpstreamEOF(&inner.Envelope{ClientID: ctrClient, SenderStageType: 21, SenderReplicaID: 0})
	if err != nil {
		t.Fatalf("eof: %v", err)
	}
	var pairs []string
	for _, om := range out.Outputs {
		p, err := inner.DeserializeQuery4Pair(om.Body)
		if err != nil {
			t.Fatalf("deserialize pair: %v", err)
		}
		pairs = append(pairs, fmt.Sprintf("%s->%s", p.SourceAccount, p.DestAccount))
	}
	return pairs
}

func TestCounterEmissionIsOrderIndependent(t *testing.T) {
	order1 := []*inner.Envelope{
		suspiciousPath(t, "A", "M1", "B"),
		suspiciousPath(t, "A", "M2", "B"),
		suspiciousPath(t, "A", "M1", "C"),
	}
	order2 := []*inner.Envelope{
		suspiciousPath(t, "A", "M1", "C"),
		suspiciousPath(t, "A", "M2", "B"),
		suspiciousPath(t, "A", "M1", "B"),
		suspiciousPath(t, "A", "M2", "B"),
	}

	r1 := runCounter(t, order1)
	r2 := runCounter(t, order2)

	if !reflect.DeepEqual(r1, []string{"A->B"}) {
		t.Fatalf("esperaba [A->B], got %v", r1)
	}
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("emisión depende del orden:\n order1=%v\n order2=%v", r1, r2)
	}
}
