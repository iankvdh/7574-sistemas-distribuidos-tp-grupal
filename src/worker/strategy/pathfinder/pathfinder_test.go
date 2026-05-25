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

func feed(t *testing.T, pf *PathFinder, bySource bool, fromBank uint32, fromAcc string, toBank uint32, toAcc string) []strategy.OutputMessage {
	t.Helper()
	body, err := inner.SerializeShardedTx(&inner.ShardedTx{
		FromBank: fromBank, FromAccount: fromAcc,
		ToBank: toBank, ToAccount: toAcc,
		ShardedBySource: bySource,
	})
	if err != nil {
		t.Fatalf("SerializeShardedTx: %v", err)
	}
	out, _, err := pf.ProcessMessage(&inner.Envelope{
		Kind:     inner.ShardedTxMessage,
		ClientID: "client-x",
		Payload:  body,
	})
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	return out
}

func triggerEOF(t *testing.T, pf *PathFinder, nSharders int) strategy.EOFOutcome {
	t.Helper()
	var outcome strategy.EOFOutcome
	for i := 0; i < nSharders; i++ {
		var err error
		outcome, err = pf.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x", Total: 0})
		if err != nil {
			t.Fatalf("OnUpstreamEOF #%d: %v", i+1, err)
		}
	}
	return outcome
}

// pathsIn extracts SuspiciousPath triples from outputs, keyed by "src|interm|dst".
func pathsIn(t *testing.T, outputs []strategy.OutputMessage) map[string]struct{} {
	t.Helper()
	seen := map[string]struct{}{}
	for _, om := range outputs {
		if om.BatchItemKind != inner.SuspiciousPathMessage {
			t.Fatalf("expected SuspiciousPathMessage, got kind=%d", om.BatchItemKind)
		}
		p, err := inner.DeserializeSuspiciousPath(om.Body)
		if err != nil {
			t.Fatalf("DeserializeSuspiciousPath: %v", err)
		}
		key := p.SourceAccount + "|" + p.IntermediateAccount + "|" + p.DestAccount
		seen[key] = struct{}{}
	}
	return seen
}

// TestPathFinderEmitsTripleOnline verifies that triples are emitted during
// ProcessMessage, not held until EOF.
func TestPathFinderEmitsTripleOnline(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// A→M (ByDest): M.inSet={A}, outSet empty → no output yet
	if out := feed(t, pf, false, 1, "A", 9, "M"); len(out) != 0 {
		t.Fatalf("ByDest with empty outSet: expected 0 outputs, got %d", len(out))
	}
	// M→B (BySource): M.outSet={B}, inSet={A} → emit (A,M,B)
	out := feed(t, pf, true, 9, "M", 2, "B")
	if len(out) != 1 {
		t.Fatalf("BySource completing pair: expected 1 output, got %d", len(out))
	}
	paths := pathsIn(t, out)
	if _, ok := paths["A|M|B"]; !ok {
		t.Fatalf("expected triple A|M|B, got %v", paths)
	}

	// N only has outSet (no ByDest arrives) → must not emit
	if out := feed(t, pf, true, 5, "N", 6, "C"); len(out) != 0 {
		t.Fatalf("node with empty inSet must not emit, got %d outputs", len(out))
	}

	// EOF emits no data, only downstream EOFs
	outcome := triggerEOF(t, pf, 1)
	if len(outcome.Outputs) != 0 {
		t.Fatalf("EOF must not carry data outputs, got %d", len(outcome.Outputs))
	}
}

// TestPathFinderCrossProductEmittedOnline verifies the cross-product inSet×outSet
// is emitted incrementally as each edge arrives.
func TestPathFinderCrossProductEmittedOnline(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// P→M and Q→M (ByDest): build inSet — no output yet
	if out := feed(t, pf, false, 1, "P", 9, "M"); len(out) != 0 {
		t.Fatalf("P→M: expected 0, got %d", len(out))
	}
	if out := feed(t, pf, false, 1, "Q", 9, "M"); len(out) != 0 {
		t.Fatalf("Q→M: expected 0, got %d", len(out))
	}

	// M→X (BySource): cross with inSet={P,Q} → 2 triples
	out := feed(t, pf, true, 9, "M", 2, "X")
	if len(out) != 2 {
		t.Fatalf("M→X: expected 2 triples, got %d", len(out))
	}

	// M→Y (BySource): cross with inSet={P,Q} → 2 more triples
	out = feed(t, pf, true, 9, "M", 2, "Y")
	if len(out) != 2 {
		t.Fatalf("M→Y: expected 2 triples, got %d", len(out))
	}
}

// TestPathFinderSkipsWhenSourceEqualsDestination verifies the A≠B check.
func TestPathFinderSkipsWhenSourceEqualsDestination(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	feed(t, pf, false, 1, "A", 9, "M") // A→M: M.inSet={A}
	// M→A (BySource): dest=A == source=A → skip
	if out := feed(t, pf, true, 9, "M", 1, "A"); len(out) != 0 {
		t.Fatalf("M→A where A is source: expected 0 (A==B), got %d", len(out))
	}
	// M→B (BySource): dest=B ≠ source=A → emit (A,M,B)
	out := feed(t, pf, true, 9, "M", 2, "B")
	if len(out) != 1 {
		t.Fatalf("M→B: expected 1 triple, got %d", len(out))
	}
	paths := pathsIn(t, out)
	if _, ok := paths["A|M|B"]; !ok {
		t.Fatalf("expected A|M|B, got %v", paths)
	}
}

// TestPathFinderDuplicateEdgesIgnored verifies that repeated edges don't
// produce duplicate triples.
func TestPathFinderDuplicateEdgesIgnored(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// A→D three times — only first counts
	feed(t, pf, true, 9, "A", 2, "D")
	feed(t, pf, true, 9, "A", 2, "D")
	feed(t, pf, true, 9, "A", 2, "D")

	// B→A: A.inSet={B}, A.outSet={D} → emit (B,A,D)
	out := feed(t, pf, false, 1, "B", 9, "A")
	if len(out) != 1 {
		t.Fatalf("expected 1 triple (no duplicates), got %d", len(out))
	}

	// A→D again: already in outSet → no new triple
	if out := feed(t, pf, true, 9, "A", 2, "D"); len(out) != 0 {
		t.Fatalf("duplicate edge: expected 0, got %d", len(out))
	}
}

// TestPathFinderEmitsEOFsAcrossAllCounters verifies that K_COUNTERS EOFs are
// emitted after N_SHARDERS upstream EOFs have been received.
func TestPathFinderEmitsEOFsAcrossAllCounters(t *testing.T) {
	pf := newWithKN(t, 3, 2)

	// First EOF: still waiting for second.
	outcome, err := pf.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x", Total: 0})
	if err != nil {
		t.Fatalf("OnUpstreamEOF: %v", err)
	}
	if len(outcome.EOFs) != 0 {
		t.Fatalf("first EOF must not emit downstream, got %d", len(outcome.EOFs))
	}

	// Second EOF: must emit K_COUNTERS=3 EOFs and no data outputs.
	outcome, err = pf.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x", Total: 0})
	if err != nil {
		t.Fatalf("OnUpstreamEOF #2: %v", err)
	}
	if len(outcome.Outputs) != 0 {
		t.Fatalf("EOF must carry no data outputs, got %d", len(outcome.Outputs))
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
