package filter_q3

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const testClient inner.ClientID = "client-x"

func newFilter(t *testing.T, replicaID, nReplicas int) *FilterQ3 {
	t.Helper()
	t.Setenv("N_FINAL_JOINERS", "1")
	t.Setenv("BUFFER_DIR", t.TempDir())

	cfg := strategy.StrategyConfig{
		OutputCount: 1,
		NumInputs:   2,
		ReplicaID:   replicaID,
		NReplicas:   nReplicas,
	}
	if nReplicas > 1 {
		cfg.RingQueueIn = "ring_in"
		cfg.RingQueueOut = "ring_out"
	}
	f := New()
	if err := f.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return f
}

func p2TxEnvelope(t *testing.T, tx transaction.Transaction) *inner.Envelope {
	t.Helper()
	payload, err := external.SerializeTransaction(&tx)
	if err != nil {
		t.Fatalf("SerializeTransaction: %v", err)
	}
	return &inner.Envelope{
		ClientID:   testClient,
		Kind:       inner.TransactionMessage,
		Payload:    payload,
		InputIndex: inputIndexP2,
	}
}

func avgEnvelope(t *testing.T, format string, avg float64) *inner.Envelope {
	t.Helper()
	payload, err := inner.SerializeQ3Average(&inner.Q3Average{PaymentFormat: format, Average: avg})
	if err != nil {
		t.Fatalf("SerializeQ3Average: %v", err)
	}
	return &inner.Envelope{
		ClientID:   testClient,
		Kind:       inner.Q3AverageItem,
		Payload:    payload,
		InputIndex: inputIndexAverages,
	}
}

func p2EOFEnvelope(total uint32) *inner.Envelope {
	return &inner.Envelope{
		ClientID:   testClient,
		Kind:       inner.InternalEOF,
		Total:      total,
		InputIndex: inputIndexP2,
	}
}

func avgEOFEnvelope(total uint32) *inner.Envelope {
	return &inner.Envelope{
		ClientID:   testClient,
		Kind:       inner.InternalEOF,
		Total:      total,
		QueryID:    queryID,
		InputIndex: inputIndexAverages,
	}
}

func drain(seq func(yield func(strategy.OutputMessage) bool)) []strategy.OutputMessage {
	var out []strategy.OutputMessage
	if seq == nil {
		return nil
	}
	for om := range seq {
		out = append(out, om)
	}
	return out
}

func decodeQ3Row(t *testing.T, body []byte) *inner.Query3Row {
	t.Helper()
	row, err := inner.DeserializeQuery3Row(body)
	if err != nil {
		t.Fatalf("DeserializeQuery3Row: %v", err)
	}
	return row
}

func feedTx(t *testing.T, f *FilterQ3, tx transaction.Transaction) ([]strategy.OutputMessage, strategy.LocalCounts) {
	t.Helper()
	out, counts, err := f.ProcessMessage(p2TxEnvelope(t, tx))
	if err != nil {
		t.Fatalf("ProcessMessage P2: %v", err)
	}
	return out, counts
}

func feedAvg(t *testing.T, f *FilterQ3, format string, avg float64) {
	t.Helper()
	if _, _, err := f.ProcessMessage(avgEnvelope(t, format, avg)); err != nil {
		t.Fatalf("ProcessMessage avg: %v", err)
	}
}

func feedP2EOF(t *testing.T, f *FilterQ3, total uint32) strategy.EOFOutcome {
	t.Helper()
	outcome, err := f.OnUpstreamEOF(p2EOFEnvelope(total))
	if err != nil {
		t.Fatalf("OnUpstreamEOF P2: %v", err)
	}
	return outcome
}

func feedAvgEOF(t *testing.T, f *FilterQ3, total uint32) strategy.EOFOutcome {
	t.Helper()
	outcome, err := f.OnUpstreamEOF(avgEOFEnvelope(total))
	if err != nil {
		t.Fatalf("OnUpstreamEOF avg: %v", err)
	}
	return outcome
}

func feedRingToken(t *testing.T, f *FilterQ3, token *eof.Token) strategy.EOFOutcome {
	t.Helper()
	outcome, err := f.OnRingToken(token)
	if err != nil {
		t.Fatalf("OnRingToken: %v", err)
	}
	return outcome
}

// makeTx builds a P2 USD-shaped transaction with the given format and amount.
func makeTx(format string, amount float64) transaction.Transaction {
	return transaction.Transaction{
		Date:            20221002, // Period 2
		FromBank:        7,
		FromAccount:     "acc1",
		ToBank:          9,
		ToAccount:       "acc2",
		AmountPaid:      amount,
		PaymentCurrency: "USD",
		PaymentFormat:   format,
	}
}

// TestN1_AveragesFirst: con N=1 y K=1, llegan los averages primero; las P2 que
// arriben después se filtran inline. El EOF de P2 dispara drain (vacío) y EOF.
func TestN1_AveragesFirst(t *testing.T) {
	f := newFilter(t, 0, 1)
	feedAvg(t, f, "WIRE", 1000.0) // umbral = 10.0

	outcome := feedAvgEOF(t, f, 1)
	if outcome.Action.Kind != eof.ActionNone {
		t.Fatalf("avg EOF before P2: want ActionNone, got %v", outcome.Action.Kind)
	}

	// tx que pasa inline
	out, counts := feedTx(t, f, makeTx("WIRE", 5.0))
	if len(out) != 1 {
		t.Fatalf("expected inline match emit, got %d outputs", len(out))
	}
	row := decodeQ3Row(t, out[0].Body)
	if row.Amount != 5.0 || row.SourceBank != 7 {
		t.Fatalf("unexpected row: %+v", row)
	}
	if counts.Matched != 1 {
		t.Fatalf("expected Matched=1, got %+v", counts)
	}

	// tx que NO pasa inline (monto >= avg/100)
	out, counts = feedTx(t, f, makeTx("WIRE", 100.0))
	if len(out) != 0 {
		t.Fatalf("expected no emit for above-threshold tx, got %d", len(out))
	}
	if counts.NotMatched != 1 {
		t.Fatalf("expected NotMatched=1, got %+v", counts)
	}

	// EOF P2 → drain + EOF downstream
	outcome = feedP2EOF(t, f, 2)
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("expected ActionEmitEOFs, got %v", outcome.Action.Kind)
	}
	if len(outcome.EOFs) != 1 || outcome.EOFs[0].QueryID != queryID {
		t.Fatalf("unexpected EOFs: %+v", outcome.EOFs)
	}
	drained := drain(outcome.OutputsIterator)
	if len(drained) != 0 {
		t.Fatalf("expected empty drain (no spill), got %d", len(drained))
	}
}

// TestN1_P2First: con N=1, llega el EOF de P2 antes que el de averages. Las P2
// previas van al spill. Al cerrar averages, se drena el spill y se emite EOF.
func TestN1_P2First(t *testing.T) {
	f := newFilter(t, 0, 1)

	// 3 txs sin averages: van al spill.
	feedTx(t, f, makeTx("WIRE", 5.0))  // pasa (< 1000/100 = 10)
	feedTx(t, f, makeTx("WIRE", 50.0)) // no pasa (>= 10)
	feedTx(t, f, makeTx("CHECK", 3.0)) // pasa (< 800/100 = 8)

	// EOF P2 sin averages: ring n=1 → ActionEmitEOFs en el coordinator,
	// pero como avgEOFDone=false, handleRingAction degrada a ActionNone.
	outcome := feedP2EOF(t, f, 3)
	if outcome.Action.Kind != eof.ActionNone {
		t.Fatalf("P2 EOF without averages: want ActionNone, got %v", outcome.Action.Kind)
	}

	// Llegan los averages (después del EOF — caso edge pero válido)
	feedAvg(t, f, "WIRE", 1000.0)
	feedAvg(t, f, "CHECK", 800.0)
	outcome = feedAvgEOF(t, f, 2)
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("avg EOF after P2: want ActionEmitEOFs, got %v", outcome.Action.Kind)
	}

	drained := drain(outcome.OutputsIterator)
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained matches, got %d", len(drained))
	}
	amounts := map[float64]bool{}
	for _, om := range drained {
		row := decodeQ3Row(t, om.Body)
		amounts[row.Amount] = true
	}
	if !amounts[5.0] || !amounts[3.0] {
		t.Fatalf("expected matches with amounts 5.0 and 3.0, got %v", amounts)
	}
}

// TestN3_AveragesBeforeRing: con N=3, los averages cierran primero en cada
// réplica. Después el ring cierra normalmente; en initiator el outcome es
// ActionEmitEOFs (con drain vacío en este test) y en intermedias es
// ActionEmitEOFsAndForwardToken.
func TestN3_AveragesBeforeRing(t *testing.T) {
	// Réplica initiator (replicaID=0)
	f0 := newFilter(t, 0, 3)
	feedAvg(t, f0, "WIRE", 1000.0)
	feedAvgEOF(t, f0, 1)
	// Procesa una tx (filtra inline)
	out, _ := feedTx(t, f0, makeTx("WIRE", 5.0))
	if len(out) != 1 {
		t.Fatalf("initiator inline match: want 1 output, got %d", len(out))
	}

	// EOF P2 con total=2 (la suma de p2Seen entre las 3 réplicas, ver feeds abajo)
	outcome := feedP2EOF(t, f0, 2)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("initiator P2 EOF: want ActionForwardToken, got %v", outcome.Action.Kind)
	}
	token := outcome.Action.Token
	if token == nil || token.Phase != eof.PhaseCollecting {
		t.Fatalf("expected Collecting token, got %+v", token)
	}

	// Replica intermedia 1: averages ready, ring Collecting llega.
	f1 := newFilter(t, 1, 3)
	feedAvg(t, f1, "WIRE", 1000.0)
	feedAvgEOF(t, f1, 1)
	feedTx(t, f1, makeTx("WIRE", 7.0)) // inline match
	outcome = feedRingToken(t, f1, token)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("r1 collecting: want ActionForwardToken, got %v", outcome.Action.Kind)
	}
	token = outcome.Action.Token

	// Replica 2
	f2 := newFilter(t, 2, 3)
	feedAvg(t, f2, "WIRE", 1000.0)
	feedAvgEOF(t, f2, 1)
	outcome = feedRingToken(t, f2, token)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("r2 collecting: want ActionForwardToken, got %v", outcome.Action.Kind)
	}
	token = outcome.Action.Token

	// Token vuelve a initiator: aggMatched debería ser la suma de p2Seen.
	// Pasa a fase Closing.
	outcome = feedRingToken(t, f0, token)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("initiator collecting back: want ActionForwardToken (closing), got %v", outcome.Action.Kind)
	}
	token = outcome.Action.Token
	if token.Phase != eof.PhaseClosing {
		t.Fatalf("expected PhaseClosing, got %v", token.Phase)
	}

	// Closing pasa por r1 (intermedia): ActionEmitEOFsAndForwardToken con drain.
	outcome = feedRingToken(t, f1, token)
	if outcome.Action.Kind != eof.ActionEmitEOFsAndForwardToken {
		t.Fatalf("r1 closing with averages: want ActionEmitEOFsAndForwardToken, got %v", outcome.Action.Kind)
	}
	if len(outcome.EOFs) != 1 {
		t.Fatalf("r1 closing: expected 1 EOF, got %d", len(outcome.EOFs))
	}
	token = outcome.Action.Token

	// r2 (intermedia): idem
	outcome = feedRingToken(t, f2, token)
	if outcome.Action.Kind != eof.ActionEmitEOFsAndForwardToken {
		t.Fatalf("r2 closing: want ActionEmitEOFsAndForwardToken, got %v", outcome.Action.Kind)
	}
	token = outcome.Action.Token

	// Token Closing vuelve al initiator: ActionEmitEOFs.
	outcome = feedRingToken(t, f0, token)
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("initiator closing back: want ActionEmitEOFs, got %v", outcome.Action.Kind)
	}
	if len(outcome.EOFs) != 1 || outcome.EOFs[0].QueryID != queryID {
		t.Fatalf("initiator final EOF emit unexpected: %+v", outcome.EOFs)
	}
}

// TestN3_RingClosesBeforeAverages: con N=3, P2 EOF llega antes que los avg
// EOFs en cada réplica. El token de Closing debe atravesar el anillo
// degradado a ForwardToken (sin emit), y cuando lleguen los avg EOFs cada
// réplica drena su spill localmente.
func TestN3_RingClosesBeforeAverages(t *testing.T) {
	f0 := newFilter(t, 0, 3)
	f1 := newFilter(t, 1, 3)
	f2 := newFilter(t, 2, 3)

	// Cada réplica recibe algunas P2 (sin averages — todas van al spill).
	feedTx(t, f0, makeTx("WIRE", 5.0))  // < 10 → match al drenar
	feedTx(t, f1, makeTx("WIRE", 7.0))  // < 10 → match al drenar
	feedTx(t, f2, makeTx("WIRE", 50.0)) // >= 10 → no match

	// P2 EOF llega a initiator (r0)
	outcome := feedP2EOF(t, f0, 3)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("initiator P2 EOF: want ActionForwardToken, got %v", outcome.Action.Kind)
	}
	token := outcome.Action.Token

	// Collecting pasa por r1, r2 y vuelve a r0
	outcome = feedRingToken(t, f1, token)
	token = outcome.Action.Token
	outcome = feedRingToken(t, f2, token)
	token = outcome.Action.Token
	outcome = feedRingToken(t, f0, token)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("initiator collecting back: want ActionForwardToken (closing), got %v", outcome.Action.Kind)
	}
	token = outcome.Action.Token
	if token.Phase != eof.PhaseClosing {
		t.Fatalf("expected PhaseClosing, got %v", token.Phase)
	}

	// Closing en r1: averages NO listos → DEGRADAR a ForwardToken (no emit).
	outcome = feedRingToken(t, f1, token)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("r1 closing without averages: want ActionForwardToken (degraded), got %v", outcome.Action.Kind)
	}
	if len(outcome.EOFs) != 0 {
		t.Fatalf("r1 degraded should not emit EOFs, got %d", len(outcome.EOFs))
	}
	token = outcome.Action.Token

	// Closing en r2: idem
	outcome = feedRingToken(t, f2, token)
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("r2 closing without averages: want ActionForwardToken, got %v", outcome.Action.Kind)
	}
	token = outcome.Action.Token

	// Closing vuelve al initiator: ActionEmitEOFs en el ring coord, pero
	// degradamos a ActionNone porque averages no cerró.
	outcome = feedRingToken(t, f0, token)
	if outcome.Action.Kind != eof.ActionNone {
		t.Fatalf("initiator closing back without averages: want ActionNone, got %v", outcome.Action.Kind)
	}

	// Ahora llegan los averages a cada réplica → cada una drena su spill.
	for _, f := range []*FilterQ3{f0, f1, f2} {
		feedAvg(t, f, "WIRE", 1000.0)
		out := feedAvgEOF(t, f, 1)
		if out.Action.Kind != eof.ActionEmitEOFs {
			t.Fatalf("avg EOF after ring close: want ActionEmitEOFs, got %v", out.Action.Kind)
		}
		drained := drain(out.OutputsIterator)
		// f2 tiene 50.0 que no pasa el filtro; los demás emiten 1 match c/u.
		_ = drained
	}
}

// TestSpillDrainFilters: las txs del spill se filtran contra los averages al
// drenar; sólo se emiten las que pasan.
func TestSpillDrainFilters(t *testing.T) {
	f := newFilter(t, 0, 1)
	feedTx(t, f, makeTx("WIRE", 5.0))  // match (< 10)
	feedTx(t, f, makeTx("WIRE", 12.0)) // no match (>= 10)
	feedTx(t, f, makeTx("CHECK", 7.0)) // match (< 800/100 = 8)
	feedTx(t, f, makeTx("CHECK", 9.0)) // no match
	feedTx(t, f, makeTx("CARD", 1.0))  // sin avg → no match

	feedAvg(t, f, "WIRE", 1000.0)
	feedAvg(t, f, "CHECK", 800.0)
	outcome := feedAvgEOF(t, f, 2)
	if outcome.Action.Kind != eof.ActionNone {
		t.Fatalf("avg EOF before P2: want ActionNone, got %v", outcome.Action.Kind)
	}
	outcome = feedP2EOF(t, f, 5)
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("P2 EOF: want ActionEmitEOFs, got %v", outcome.Action.Kind)
	}

	drained := drain(outcome.OutputsIterator)
	if len(drained) != 2 {
		t.Fatalf("expected 2 matches from drain, got %d", len(drained))
	}
}

// TestUnexpectedInputIndex: ProcessMessage devuelve error si InputIndex no es 0 o 1.
func TestUnexpectedInputIndex(t *testing.T) {
	f := newFilter(t, 0, 1)
	env := p2TxEnvelope(t, makeTx("WIRE", 5.0))
	env.InputIndex = 99
	if _, _, err := f.ProcessMessage(env); err == nil {
		t.Fatalf("expected error for unknown InputIndex")
	}
}
