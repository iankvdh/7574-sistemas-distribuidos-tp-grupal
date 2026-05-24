package pathfinder

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func newWithKN(t *testing.T, k, n int) *PathFinder {
	t.Helper()
	t.Setenv("K_COUNTERS", itoa(k))
	t.Setenv("N_SHARDERS", itoa(n))
	pf := New()
	if err := pf.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	return pf
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func mkShardedTx(bySource bool, fromBank uint32, fromAcc string, toBank uint32, toAcc string) []byte {
	body, err := inner.SerializeShardedTx(&inner.ShardedTx{
		FromBank: fromBank, FromAccount: fromAcc,
		ToBank: toBank, ToAccount: toAcc,
		ShardedBySource: bySource,
	})
	if err != nil {
		panic(err)
	}
	return body
}

func feed(t *testing.T, pf *PathFinder, bySource bool, fromBank uint32, fromAcc string, toBank uint32, toAcc string) []strategy.OutputMessage {
	t.Helper()
	out, _, err := pf.ProcessMessage(&inner.Envelope{
		Kind:     inner.ShardedTxMessage,
		ClientID: "client-x",
		Payload:  mkShardedTx(bySource, fromBank, fromAcc, toBank, toAcc),
	})
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	return out
}

func TestPathFinderBuildsCombinationsOnNewAdditions(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// Estado inicial para cuenta A: in={B}, out={D}.
	// Llegan dos mensajes preparatorios.
	if out := feed(t, pf, false /*by dest*/, 1, "B", 9, "A"); len(out) != 0 {
		t.Fatalf("first incoming B→A should not emit (out set empty), got %d", len(out))
	}
	if out := feed(t, pf, true /*by source*/, 9, "A", 2, "D"); len(out) != 1 {
		t.Fatalf("second message A→D with in={B} must emit 1 triplet (B,A,D), got %d", len(out))
	}

	// Llega "G,A,true" — by source: añadimos D nuevamente a out(A); ya existe, NO emite.
	if out := feed(t, pf, true, 9, "A", 2, "D"); len(out) != 0 {
		t.Fatalf("repeated destination should not emit, got %d", len(out))
	}

	// Llega G→A (in): nuevo. in={B,G}, out={D}. Por G nuevo en in, generamos (G,A,D).
	out := feed(t, pf, false, 7, "G", 9, "A")
	if len(out) != 1 {
		t.Fatalf("new in account G with out={D} must emit 1 triplet, got %d", len(out))
	}
	triplet, err := inner.DeserializeSuspiciousPath(out[0].Body)
	if err != nil {
		t.Fatalf("deserialize triplet: %v", err)
	}
	if triplet.SourceAccount != "G" || triplet.IntermediateAccount != "A" || triplet.DestAccount != "D" {
		t.Fatalf("expected (G,A,D), got (%s,%s,%s)", triplet.SourceAccount, triplet.IntermediateAccount, triplet.DestAccount)
	}
}

func TestPathFinderEmitsAcrossWholeCrossProductOnFirstAddition(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// out(A) = {D, E, F} (3 mensajes A→D, A→E, A→F; in vacío → no emite).
	for _, acc := range []string{"D", "E", "F"} {
		if out := feed(t, pf, true, 9, "A", 2, acc); len(out) != 0 {
			t.Fatalf("seeding out(A)={D,E,F}: emit expected 0, got %d", len(out))
		}
	}
	// in(A) = {B, C}, ambos producen 3 triplets cada uno (cross-product con {D,E,F}).
	for _, acc := range []string{"B", "C"} {
		out := feed(t, pf, false, 1, acc, 9, "A")
		if len(out) != 3 {
			t.Fatalf("new in %s with out={D,E,F} must emit 3, got %d", acc, len(out))
		}
	}
	// Una entrada repetida (B) no debe emitir nada nuevo.
	if out := feed(t, pf, false, 1, "B", 9, "A"); len(out) != 0 {
		t.Fatalf("repeated in addition must not emit, got %d", len(out))
	}
}

func TestPathFinderEmitsEOFsAcrossAllCounters(t *testing.T) {
	pf := newWithKN(t, 3, 2)

	// Primer EOF: aún esperando uno más, ActionNone.
	outcome, err := pf.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x", Total: 0})
	if err != nil {
		t.Fatalf("OnUpstreamEOF: %v", err)
	}
	if len(outcome.EOFs) != 0 {
		t.Fatalf("first EOF must not emit downstream, got %d", len(outcome.EOFs))
	}

	// Segundo EOF: completa, debe emitir K_COUNTERS=3 EOFs.
	outcome, err = pf.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x", Total: 0})
	if err != nil {
		t.Fatalf("OnUpstreamEOF #2: %v", err)
	}
	if len(outcome.EOFs) != 3 {
		t.Fatalf("expected 3 EOFEmits, got %d", len(outcome.EOFs))
	}
	for i, e := range outcome.EOFs {
		if e.OutputIndex != 0 {
			t.Fatalf("emit %d expected OutputIndex=0, got %d", i, e.OutputIndex)
		}
		if e.RoutingKey == "" {
			t.Fatalf("emit %d must have a routing key", i)
		}
	}
}
