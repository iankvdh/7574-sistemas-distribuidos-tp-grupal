package counter

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func newCounter(t *testing.T, nPathFinders, nFinalJoiners int) *Counter {
	t.Helper()
	t.Setenv("N_PATH_FINDERS", itoa(nPathFinders))
	t.Setenv("N_FINAL_JOINERS", itoa(nFinalJoiners))
	t.Setenv("MIN_INTERMEDIATES", "3") // use 3 in tests to keep fixtures small
	c := New()
	if err := c.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	return c
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

func feedPath(t *testing.T, c *Counter, srcBank uint32, src string, midBank uint32, mid string, dstBank uint32, dst string) []strategy.OutputMessage {
	t.Helper()
	body, err := inner.SerializeSuspiciousPath(&inner.SuspiciousPath{
		SourceBank: srcBank, SourceAccount: src,
		IntermediateBank: midBank, IntermediateAccount: mid,
		DestBank: dstBank, DestAccount: dst,
	})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	out, _, err := c.ProcessMessage(&inner.Envelope{
		Kind:     inner.SuspiciousPathMessage,
		ClientID: "client-x",
		Payload:  body,
		QueryID:  4,
	})
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	return out
}

// TestCounterEmitsAtThreshold verifies that accounts are emitted exactly when
// the intermediary count reaches MIN_INTERMEDIATES.
func TestCounterEmitsAtThreshold(t *testing.T) {
	c := newCounter(t, 1, 1) // MIN_INTERMEDIATES=3

	// pair (A,B): feed 2 intermediaries → no output yet
	if out := feedPath(t, c, 1, "A", 9, "M1", 2, "B"); len(out) != 0 {
		t.Fatalf("1st interm: expected 0 outputs, got %d", len(out))
	}
	if out := feedPath(t, c, 1, "A", 9, "M2", 2, "B"); len(out) != 0 {
		t.Fatalf("2nd interm: expected 0 outputs, got %d", len(out))
	}
	// 3rd intermediary → emit the (A,B) pair
	out := feedPath(t, c, 1, "A", 9, "M3", 2, "B")
	if len(out) != 1 {
		t.Fatalf("3rd interm: expected 1 output (pair A→B), got %d", len(out))
	}
	o := out[0]
	if o.BatchItemKind != inner.Query4PairItem {
		t.Fatalf("expected Query4PairItem, got %d", o.BatchItemKind)
	}
	if o.BatchQueryID != 4 {
		t.Fatalf("expected BatchQueryID=4, got %d", o.BatchQueryID)
	}
	pair, err := inner.DeserializeQuery4Pair(o.Body)
	if err != nil {
		t.Fatalf("deserialize pair: %v", err)
	}
	if pair.SourceBank != 1 || pair.SourceAccount != "A" || pair.DestBank != 2 || pair.DestAccount != "B" {
		t.Fatalf("unexpected pair payload: %+v", pair)
	}
}

// TestCounterDoesNotReemitAfterThreshold verifies the emitted=true guard.
func TestCounterDoesNotReemitAfterThreshold(t *testing.T) {
	c := newCounter(t, 1, 1)

	feedPath(t, c, 1, "A", 9, "M1", 2, "B")
	feedPath(t, c, 1, "A", 9, "M2", 2, "B")
	if out := feedPath(t, c, 1, "A", 9, "M3", 2, "B"); len(out) != 1 {
		t.Fatalf("threshold: expected 1 output, got %d", len(out))
	}
	// extra paths for the same pair must not re-emit
	if out := feedPath(t, c, 1, "A", 9, "M4", 2, "B"); len(out) != 0 {
		t.Fatalf("post-threshold: expected 0 outputs, got %d", len(out))
	}
}

// TestCounterDuplicateIntermediaryNotCounted verifies that the same M fed twice
// only counts once.
func TestCounterDuplicateIntermediaryNotCounted(t *testing.T) {
	c := newCounter(t, 1, 1)

	feedPath(t, c, 1, "A", 9, "M1", 2, "B")
	feedPath(t, c, 1, "A", 9, "M1", 2, "B") // duplicate
	feedPath(t, c, 1, "A", 9, "M2", 2, "B")
	// only 2 distinct intermediaries so far → still below threshold=3
	if out := feedPath(t, c, 1, "A", 9, "M2", 2, "B"); len(out) != 0 {
		t.Fatalf("duplicate M: should not emit yet, got %d", len(out))
	}
	// 3rd distinct M → emit
	if out := feedPath(t, c, 1, "A", 9, "M3", 2, "B"); len(out) != 1 {
		t.Fatalf("3rd distinct M: expected 1 output, got %d", len(out))
	}
}

// TestCounterIndependentPairs verifies that different (A,B) pairs are tracked
// independently.
func TestCounterIndependentPairs(t *testing.T) {
	c := newCounter(t, 1, 1)

	// pair (A,B): reaches threshold
	feedPath(t, c, 1, "A", 9, "M1", 2, "B")
	feedPath(t, c, 1, "A", 9, "M2", 2, "B")
	feedPath(t, c, 1, "A", 9, "M3", 2, "B")

	// pair (A,C): only 1 intermediary → no output
	if out := feedPath(t, c, 1, "A", 9, "M1", 3, "C"); len(out) != 0 {
		t.Fatalf("pair (A,C) with 1 interm: expected 0 outputs, got %d", len(out))
	}
}

// TestCounterEmitsEOFAfterAllPathFinders verifies the N_PATH_FINDERS EOF gate.
func TestCounterEmitsEOFAfterAllPathFinders(t *testing.T) {
	c := newCounter(t, 2, 1)

	if out, err := c.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x"}); err != nil || len(out.EOFs) != 0 {
		t.Fatalf("first EOF must not emit downstream: err=%v out=%+v", err, out)
	}
	outcome, err := c.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x"})
	if err != nil {
		t.Fatalf("second EOF: %v", err)
	}
	if len(outcome.EOFs) != 1 {
		t.Fatalf("expected 1 final EOF, got %d", len(outcome.EOFs))
	}
	if outcome.EOFs[0].RoutingKey == "" {
		t.Fatalf("EOF must include routing key")
	}
}
