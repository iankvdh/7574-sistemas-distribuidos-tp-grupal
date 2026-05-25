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

func feed(t *testing.T, pf *PathFinder, bySource bool, fromBank uint32, fromAcc string, toBank uint32, toAcc string) {
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
	if len(out) != 0 {
		t.Fatalf("ProcessMessage must never emit online, got %d outputs", len(out))
	}
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

// TestPathFinderEmitsTripleForNonEmptyInAndOut verifies that only intermediates
// with both inSet ≠ ∅ AND outSet ≠ ∅ produce output triples.
func TestPathFinderEmitsTripleForNonEmptyInAndOut(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// M: in={A}, out={B} → qualifies, should emit triple (A, M, B)
	feed(t, pf, false, 1, "A", 9, "M") // A→M (byDest: M.inSet.add(A))
	feed(t, pf, true, 9, "M", 2, "B")  // M→B (bySource: M.outSet.add(B))

	// N: in={}, out={C} → NO inSet, must not emit
	feed(t, pf, true, 5, "N", 6, "C")

	outcome := triggerEOF(t, pf, 1)

	if len(outcome.Outputs) != 1 {
		t.Fatalf("expected 1 triple output, got %d", len(outcome.Outputs))
	}
	paths := pathsIn(t, outcome.Outputs)
	if _, ok := paths["A|M|B"]; !ok {
		t.Fatalf("expected triple A|M|B in outputs, got %v", paths)
	}
}

// TestPathFinderCrossProductForQualifyingIntermediate verifies that the cross
// product inSet × outSet produces one triple per (source, intermediate, dest).
func TestPathFinderCrossProductForQualifyingIntermediate(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// M: in={P,Q}, out={X,Y} → 2×2 = 4 triples
	feed(t, pf, false, 1, "P", 9, "M") // P→M
	feed(t, pf, false, 1, "Q", 9, "M") // Q→M
	feed(t, pf, true, 9, "M", 2, "X")  // M→X
	feed(t, pf, true, 9, "M", 2, "Y")  // M→Y

	outcome := triggerEOF(t, pf, 1)

	if len(outcome.Outputs) != 4 {
		t.Fatalf("expected 4 triple outputs (cross-product), got %d", len(outcome.Outputs))
	}
	paths := pathsIn(t, outcome.Outputs)
	for _, want := range []string{"P|M|X", "P|M|Y", "Q|M|X", "Q|M|Y"} {
		if _, ok := paths[want]; !ok {
			t.Fatalf("expected triple %s in outputs, got %v", want, paths)
		}
	}
}

// TestPathFinderSkipsWhenSourceEqualsDestination verifies the A≠B check.
func TestPathFinderSkipsWhenSourceEqualsDestination(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// M: in={A}, out={A, B} → pair (A,A) skipped, only (A,M,B) emitted
	feed(t, pf, false, 1, "A", 9, "M") // A→M
	feed(t, pf, true, 9, "M", 1, "A")  // M→A (same as source)
	feed(t, pf, true, 9, "M", 2, "B")  // M→B

	outcome := triggerEOF(t, pf, 1)

	if len(outcome.Outputs) != 1 {
		t.Fatalf("expected 1 triple (A≠B filter), got %d", len(outcome.Outputs))
	}
	paths := pathsIn(t, outcome.Outputs)
	if _, ok := paths["A|M|B"]; !ok {
		t.Fatalf("expected triple A|M|B, got %v", paths)
	}
}

// TestPathFinderDuplicateEdgesIgnored verifies that repeated edges don't
// produce duplicate triples.
func TestPathFinderDuplicateEdgesIgnored(t *testing.T) {
	pf := newWithKN(t, 1, 1)

	// Same edge fed 3 times — outSet deduplicates
	feed(t, pf, true, 9, "A", 2, "D")
	feed(t, pf, true, 9, "A", 2, "D")
	feed(t, pf, true, 9, "A", 2, "D")
	feed(t, pf, false, 1, "B", 9, "A") // B→A

	outcome := triggerEOF(t, pf, 1)

	// A: in={B}, out={D} → 1 triple (B, A, D)
	if len(outcome.Outputs) != 1 {
		t.Fatalf("expected 1 triple (no duplicates), got %d", len(outcome.Outputs))
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

	// Second EOF: must emit K_COUNTERS=3 EOFs.
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
