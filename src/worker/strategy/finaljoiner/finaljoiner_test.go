package finaljoiner

import (
	"os"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

// newInited builds a FinalJoiner configured with per-query EOF cuotas. Pass 0
// for queries you want disabled in the test.
func newInited(t *testing.T, expectedQ1, expectedQ4, outputCount int) *FinalJoiner {
	t.Helper()
	if expectedQ1 > 0 {
		t.Setenv("EXPECTED_EOFS_Q1", itoa(expectedQ1))
	} else {
		os.Unsetenv("EXPECTED_EOFS_Q1")
	}
	if expectedQ4 > 0 {
		t.Setenv("EXPECTED_EOFS_Q4", itoa(expectedQ4))
	} else {
		os.Unsetenv("EXPECTED_EOFS_Q4")
	}
	j := New()
	if err := j.Init(strategy.StrategyConfig{
		OutputCount:  outputCount,
		ReplicaID:    0,
		NReplicas:    1,
		StrategyName: "final_joiner",
	}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	return j
}

// tiny strconv-free helper to keep tests dependency-light
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

func TestFinalJoinerInitRequiresAtLeastOneQuery(t *testing.T) {
	os.Unsetenv("EXPECTED_EOFS_Q1")
	os.Unsetenv("EXPECTED_EOFS_Q4")
	if err := New().Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err == nil {
		t.Fatalf("expected error when no EXPECTED_EOFS_Q<n> is set")
	}
}

func TestFinalJoinerProcessQ1FormatsRow(t *testing.T) {
	j := newInited(t, 1, 0, 1)

	row := &inner.Query1Row{
		SourceBank: 11, SourceAccount: "ACC-A",
		DestBank: 12, DestAccount: "ACC-B",
		Amount: 42.5,
	}
	payload, _ := inner.SerializeQuery1Row(row)
	out, counts, err := j.ProcessMessage(&inner.Envelope{
		Kind:      inner.Query1RowItem,
		ClientID:  "c-1",
		GatewayID: 1,
		QueryID:   1,
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	if counts.Processed != 1 || counts.Matched != 1 {
		t.Fatalf("counts mismatch: %+v", counts)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 OutputMessage, got %d", len(out))
	}
	o := out[0]
	if o.BatchQueryID != 1 {
		t.Fatalf("BatchQueryID: got %d want 1", o.BatchQueryID)
	}
	if len(o.OutputIndices) != 1 || o.OutputIndices[0] != 0 {
		t.Fatalf("OutputIndices: %v want [0]", o.OutputIndices)
	}
	if string(o.Body) != "11,ACC-A,12,ACC-B,42.50" {
		t.Fatalf("unexpected row: %q", string(o.Body))
	}
}

func TestFinalJoinerProcessQ4DedupesAndDefersUntilEOF(t *testing.T) {
	j := newInited(t, 0, 1, 1)

	feed := func(srcBank uint32, src string, dstBank uint32, dst string) {
		pair := &inner.Query4Pair{SourceBank: srcBank, SourceAccount: src, DestBank: dstBank, DestAccount: dst}
		payload, _ := inner.SerializeQuery4Pair(pair)
		out, _, err := j.ProcessMessage(&inner.Envelope{
			Kind: inner.Query4PairItem, ClientID: "c-1", GatewayID: 1, QueryID: 4, Payload: payload,
		})
		if err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
		if len(out) != 0 {
			t.Fatalf("Q4 should defer outputs to EOF, got %d", len(out))
		}
	}

	feed(7, "A", 8, "B")
	feed(9, "C", 10, "D")
	feed(7, "A", 8, "B") // dup, no emit
	feed(9, "C", 11, "E")

	outcome, err := j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c-1", GatewayID: 1, QueryID: 4, Total: 0,
	})
	if err != nil {
		t.Fatalf("OnUpstreamEOF: %v", err)
	}
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("expected ActionEmitEOFs, got %d", outcome.Action.Kind)
	}
	if len(outcome.Outputs) != 3 {
		t.Fatalf("expected 3 deduped outputs, got %d (%+v)", len(outcome.Outputs), outcome.Outputs)
	}

	wants := []string{"7,A,8,B", "9,C,10,D", "9,C,11,E"}
	for i, o := range outcome.Outputs {
		if string(o.Body) != wants[i] {
			t.Fatalf("output[%d]: got %q want %q", i, string(o.Body), wants[i])
		}
		if o.BatchQueryID != 4 {
			t.Fatalf("output[%d]: BatchQueryID got %d want 4", i, o.BatchQueryID)
		}
	}

	if len(outcome.EOFs) != 1 || outcome.EOFs[0].QueryID != 4 {
		t.Fatalf("expected 1 EOFEmit for Q4, got %+v", outcome.EOFs)
	}
}

func TestFinalJoinerHandlesQ1AndQ4InSameInstance(t *testing.T) {
	j := newInited(t, 2, 2, 1) // 2 EOFs cuota per query

	// Q1 item — should emit immediately.
	q1 := &inner.Query1Row{SourceBank: 1, SourceAccount: "x", DestBank: 2, DestAccount: "y", Amount: 1}
	q1Payload, _ := inner.SerializeQuery1Row(q1)
	out, _, err := j.ProcessMessage(&inner.Envelope{
		Kind: inner.Query1RowItem, ClientID: "c", GatewayID: 1, QueryID: 1, Payload: q1Payload,
	})
	if err != nil || len(out) != 1 {
		t.Fatalf("Q1 ProcessMessage: err=%v out=%d", err, len(out))
	}

	// Q4 item — should buffer.
	q4 := &inner.Query4Pair{SourceBank: 3, SourceAccount: "M", DestBank: 4, DestAccount: "N"}
	q4Payload, _ := inner.SerializeQuery4Pair(q4)
	out, _, err = j.ProcessMessage(&inner.Envelope{
		Kind: inner.Query4PairItem, ClientID: "c", GatewayID: 1, QueryID: 4, Payload: q4Payload,
	})
	if err != nil || len(out) != 0 {
		t.Fatalf("Q4 ProcessMessage: err=%v out=%d (want 0)", err, len(out))
	}

	// First EOF of EACH query — neither should fire yet (cuota 2 each).
	for _, qid := range []uint8{1, 4} {
		outcome, err := j.OnUpstreamEOF(&inner.Envelope{
			Kind: inner.InternalEOF, ClientID: "c", GatewayID: 1, QueryID: qid, Total: 0,
		})
		if err != nil {
			t.Fatalf("first EOF Q%d: %v", qid, err)
		}
		if outcome.Action.Kind != eof.ActionNone {
			t.Fatalf("first EOF Q%d should not emit yet, got %d", qid, outcome.Action.Kind)
		}
	}

	// Second EOF for Q1 → fires (no Outputs because Q1 is streaming).
	outcome, err := j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c", GatewayID: 1, QueryID: 1, Total: 0,
	})
	if err != nil {
		t.Fatalf("second EOF Q1: %v", err)
	}
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("Q1 second EOF should fire, got %d", outcome.Action.Kind)
	}
	if len(outcome.EOFs) != 1 || outcome.EOFs[0].QueryID != 1 {
		t.Fatalf("Q1 EOFEmit: %+v", outcome.EOFs)
	}
	if len(outcome.Outputs) != 0 {
		t.Fatalf("Q1 should not emit deferred Outputs, got %d", len(outcome.Outputs))
	}

	// Second EOF for Q4 → fires WITH deferred outputs (the buffered account).
	outcome, err = j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c", GatewayID: 1, QueryID: 4, Total: 0,
	})
	if err != nil {
		t.Fatalf("second EOF Q4: %v", err)
	}
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("Q4 second EOF should fire, got %d", outcome.Action.Kind)
	}
	if len(outcome.Outputs) != 1 || string(outcome.Outputs[0].Body) != "3,M,4,N" {
		t.Fatalf("Q4 deferred outputs: %+v", outcome.Outputs)
	}
	if len(outcome.EOFs) != 1 || outcome.EOFs[0].QueryID != 4 {
		t.Fatalf("Q4 EOFEmit: %+v", outcome.EOFs)
	}
}

func TestFinalJoinerProcessRoutesToCorrectGateway(t *testing.T) {
	j := newInited(t, 1, 0, 2) // 2 gateway outputs

	for gw := 1; gw <= 2; gw++ {
		row := &inner.Query1Row{SourceBank: 1, SourceAccount: "x", DestBank: 2, DestAccount: "y", Amount: 1}
		payload, _ := inner.SerializeQuery1Row(row)
		out, _, err := j.ProcessMessage(&inner.Envelope{
			Kind: inner.Query1RowItem, ClientID: "c", GatewayID: inner.GatewayID(gw),
			QueryID: 1, Payload: payload,
		})
		if err != nil {
			t.Fatalf("gw=%d: %v", gw, err)
		}
		if out[0].OutputIndices[0] != gw-1 {
			t.Fatalf("gw=%d: OutputIndex got %d want %d", gw, out[0].OutputIndices[0], gw-1)
		}
	}
}

func TestFinalJoinerProcessRejectsUnknownGateway(t *testing.T) {
	j := newInited(t, 1, 0, 1)

	row := &inner.Query1Row{SourceBank: 1, SourceAccount: "x", DestBank: 2, DestAccount: "y", Amount: 1}
	payload, _ := inner.SerializeQuery1Row(row)

	_, _, err := j.ProcessMessage(&inner.Envelope{
		Kind: inner.Query1RowItem, ClientID: "c", GatewayID: 5, // out of range
		QueryID: 1, Payload: payload,
	})
	if err == nil {
		t.Fatalf("expected error for GatewayID out of range")
	}
}

func TestFinalJoinerDropsMessagesForUnconfiguredQuery(t *testing.T) {
	j := newInited(t, 0, 1, 1) // only Q4 active

	row := &inner.Query1Row{SourceBank: 1, SourceAccount: "x", DestBank: 2, DestAccount: "y", Amount: 1}
	payload, _ := inner.SerializeQuery1Row(row)

	out, counts, err := j.ProcessMessage(&inner.Envelope{
		Kind: inner.Query1RowItem, ClientID: "c", GatewayID: 1, QueryID: 1, Payload: payload,
	})
	if err != nil {
		t.Fatalf("expected silent drop for unconfigured query, got error: %v", err)
	}
	if len(out) != 0 || counts.Matched != 0 {
		t.Fatalf("expected no output for unconfigured query, got %+v counts=%+v", out, counts)
	}
}

func TestFinalJoinerDropsEOFsForUnconfiguredQuery(t *testing.T) {
	j := newInited(t, 1, 0, 1) // only Q1 active

	outcome, err := j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c", GatewayID: 1, QueryID: 4, Total: 0,
	})
	if err != nil {
		t.Fatalf("expected silent drop, got error: %v", err)
	}
	if outcome.Action.Kind != eof.ActionNone {
		t.Fatalf("expected ActionNone, got %d", outcome.Action.Kind)
	}
}

func TestFinalJoinerEOFForUnknownGatewayFails(t *testing.T) {
	j := newInited(t, 1, 0, 1)
	_, err := j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c", GatewayID: 9, QueryID: 1, Total: 0,
	})
	if err == nil {
		t.Fatalf("expected error for GatewayID out of range on EOF")
	}
}
