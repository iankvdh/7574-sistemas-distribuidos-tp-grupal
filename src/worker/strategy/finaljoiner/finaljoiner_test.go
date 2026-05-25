package finaljoiner

import (
	"os"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func newInited(t *testing.T, queryID, expectedEOFs, outputCount int) *FinalJoiner {
	t.Helper()
	t.Setenv("QUERY_ID", itoa(queryID))
	t.Setenv("EXPECTED_EOFS", itoa(expectedEOFs))
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

func TestFinalJoinerInitRequiresQueryID(t *testing.T) {
	os.Unsetenv("QUERY_ID")
	t.Setenv("EXPECTED_EOFS", "1")
	if err := New().Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err == nil {
		t.Fatalf("expected error when QUERY_ID missing")
	}
}

func TestFinalJoinerInitRequiresExpectedEOFs(t *testing.T) {
	t.Setenv("QUERY_ID", "4")
	os.Unsetenv("EXPECTED_EOFS")
	if err := New().Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err == nil {
		t.Fatalf("expected error when EXPECTED_EOFS missing")
	}
}

func TestFinalJoinerProcessQ1FormatsRow(t *testing.T) {
	j := newInited(t, 1, 1, 1)

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
	j := newInited(t, 4, 1, 1)

	feed := func(bank uint32, account string) {
		acc := &inner.Query4Account{Bank: bank, Account: account}
		payload, _ := inner.SerializeQuery4Account(acc)
		out, _, err := j.ProcessMessage(&inner.Envelope{
			Kind: inner.Query4AccountItem, ClientID: "c-1", GatewayID: 1, QueryID: 4, Payload: payload,
		})
		if err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
		if len(out) != 0 {
			t.Fatalf("Q4 should defer outputs to EOF, got %d", len(out))
		}
	}

	// Two pairs sharing a source — dedupe should collapse the source.
	feed(7, "A")
	feed(9, "B")
	feed(7, "A") // dup, no emit
	feed(9, "C")

	outcome, err := j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c-1", GatewayID: 1, Total: 0,
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

	// Sorted by (bank, account).
	wants := []string{"7,A", "9,B", "9,C"}
	for i, o := range outcome.Outputs {
		if string(o.Body) != wants[i] {
			t.Fatalf("output[%d]: got %q want %q", i, string(o.Body), wants[i])
		}
		if o.BatchQueryID != 4 {
			t.Fatalf("output[%d]: BatchQueryID got %d want 4", i, o.BatchQueryID)
		}
		if len(o.OutputIndices) != 1 || o.OutputIndices[0] != 0 {
			t.Fatalf("output[%d]: OutputIndices %v want [0]", i, o.OutputIndices)
		}
	}

	if len(outcome.EOFs) != 1 {
		t.Fatalf("expected 1 EOFEmit, got %d", len(outcome.EOFs))
	}
	if outcome.EOFs[0].QueryID != 4 {
		t.Fatalf("EOF QueryID got %d want 4", outcome.EOFs[0].QueryID)
	}
}

func TestFinalJoinerProcessRoutesToCorrectGateway(t *testing.T) {
	j := newInited(t, 1, 1, 2) // 2 gateway outputs

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
	j := newInited(t, 1, 1, 1)

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

func TestFinalJoinerDropsMismatchedQueryID(t *testing.T) {
	j := newInited(t, 4, 1, 1)

	row := &inner.Query1Row{SourceBank: 1, SourceAccount: "x", DestBank: 2, DestAccount: "y", Amount: 1}
	payload, _ := inner.SerializeQuery1Row(row)

	out, counts, err := j.ProcessMessage(&inner.Envelope{
		Kind: inner.Query1RowItem, ClientID: "c", GatewayID: 1, QueryID: 1, Payload: payload,
	})
	if err != nil {
		t.Fatalf("expected silent drop, got error: %v", err)
	}
	if len(out) != 0 || counts.Matched != 0 {
		t.Fatalf("expected no output for mismatched QueryID, got %+v counts=%+v", out, counts)
	}
}

func TestFinalJoinerEOFAccumulatesAndEmitsAfterN(t *testing.T) {
	j := newInited(t, 1, 3, 1) // need 3 EOFs per client (Q1, no deferred outputs)

	// First two EOFs: no emit.
	for i := 0; i < 2; i++ {
		outcome, err := j.OnUpstreamEOF(&inner.Envelope{
			Kind: inner.InternalEOF, ClientID: "c-x", GatewayID: 1, Total: 0,
		})
		if err != nil {
			t.Fatalf("EOF %d: %v", i, err)
		}
		if outcome.Action.Kind != eof.ActionNone {
			t.Fatalf("EOF %d: expected ActionNone, got %d", i, outcome.Action.Kind)
		}
	}

	// Third EOF triggers emit.
	outcome, err := j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c-x", GatewayID: 1, Total: 0,
	})
	if err != nil {
		t.Fatalf("third EOF: %v", err)
	}
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("expected ActionEmitEOFs, got %d", outcome.Action.Kind)
	}
	if len(outcome.EOFs) != 1 {
		t.Fatalf("expected 1 EOFEmit, got %d", len(outcome.EOFs))
	}
	emit := outcome.EOFs[0]
	if emit.OutputIndex != 0 {
		t.Fatalf("OutputIndex got %d want 0", emit.OutputIndex)
	}
	if emit.QueryID != 1 {
		t.Fatalf("QueryID got %d want 1", emit.QueryID)
	}
}

func TestFinalJoinerEOFForUnknownGatewayFails(t *testing.T) {
	j := newInited(t, 1, 1, 1)
	_, err := j.OnUpstreamEOF(&inner.Envelope{
		Kind: inner.InternalEOF, ClientID: "c", GatewayID: 9, Total: 0,
	})
	if err == nil {
		t.Fatalf("expected error for GatewayID out of range on EOF")
	}
}
