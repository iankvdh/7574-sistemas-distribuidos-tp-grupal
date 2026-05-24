package sharder_q1

import (
	"os"
	"strconv"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func newSingleReplica(t *testing.T, n int) *Sharder {
	t.Helper()
	t.Setenv("N_FINAL_JOINERS", strconv.Itoa(n))
	s := New()
	if err := s.Init(strategy.StrategyConfig{
		OutputCount:  1,
		ReplicaID:    0,
		NReplicas:    1,
		StrategyName: "sharder_q1",
	}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	return s
}

func TestSharderQ1EmitsQuery1RowWithExpectedShard(t *testing.T) {
	s := newSingleReplica(t, 3)
	tx := &transaction.Transaction{
		FromBank: 11, FromAccount: "ACC-A",
		ToBank: 12, ToAccount: "ACC-B",
		AmountPaid:      42.50,
		PaymentCurrency: "USD",
	}
	payload, err := external.SerializeTransaction(tx)
	if err != nil {
		t.Fatalf("serialize tx: %v", err)
	}
	out, counts, err := s.ProcessMessage(&inner.Envelope{
		Kind:     inner.TransactionMessage,
		ClientID: "client-x",
		Payload:  payload,
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
	if o.BatchItemKind != inner.Query1RowItem {
		t.Fatalf("BatchItemKind: got %d want Query1RowItem", o.BatchItemKind)
	}
	if o.BatchQueryID != 1 {
		t.Fatalf("BatchQueryID: got %d want 1", o.BatchQueryID)
	}
	wantRK := strconv.Itoa(hashing.Shard("client-x", 3))
	if o.RoutingKey != wantRK {
		t.Fatalf("RoutingKey: got %q want %q", o.RoutingKey, wantRK)
	}
	row, err := inner.DeserializeQuery1Row(o.Body)
	if err != nil {
		t.Fatalf("DeserializeQuery1Row: %v", err)
	}
	if row.SourceBank != 11 || row.SourceAccount != "ACC-A" ||
		row.DestBank != 12 || row.DestAccount != "ACC-B" || row.Amount != 42.50 {
		t.Fatalf("row mismatch: %+v", row)
	}
}

func TestSharderQ1RejectsNonTransactionEnvelope(t *testing.T) {
	s := newSingleReplica(t, 1)
	_, _, err := s.ProcessMessage(&inner.Envelope{Kind: inner.AccountMessage, ClientID: "x"})
	if err == nil {
		t.Fatalf("expected error on non-TransactionMessage kind")
	}
}

func TestSharderQ1RequiresNFinalJoiners(t *testing.T) {
	os.Unsetenv("N_FINAL_JOINERS")
	s := New()
	if err := s.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err == nil {
		t.Fatalf("expected error when N_FINAL_JOINERS missing")
	}
}

func TestSharderQ1SingleReplicaEOFEmitsOneEmitWithClientRK(t *testing.T) {
	s := newSingleReplica(t, 4)
	outcome, err := s.OnUpstreamEOF(&inner.Envelope{
		Kind:     inner.InternalEOF,
		ClientID: "client-y",
		Total:    7,
	})
	if err != nil {
		t.Fatalf("OnUpstreamEOF: %v", err)
	}
	if outcome.Action.Kind != eof.ActionEmitEOFs {
		t.Fatalf("expected ActionEmitEOFs, got %d", outcome.Action.Kind)
	}
	if len(outcome.EOFs) != 1 {
		t.Fatalf("expected 1 EOFEmit, got %d", len(outcome.EOFs))
	}
	wantRK := strconv.Itoa(hashing.Shard("client-y", 4))
	if outcome.EOFs[0].RoutingKey != wantRK {
		t.Fatalf("EOF RoutingKey: got %q want %q", outcome.EOFs[0].RoutingKey, wantRK)
	}
}

func TestSharderQ1MultiReplicaUsesRingBroadcast(t *testing.T) {
	t.Setenv("N_FINAL_JOINERS", "2")
	s := New()
	if err := s.Init(strategy.StrategyConfig{
		OutputCount:  1,
		ReplicaID:    0,
		NReplicas:    2,
		StrategyName: "sharder_q1",
		RingQueueIn:  "ring_in",
		RingQueueOut: "ring_out",
	}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	outcome, err := s.OnUpstreamEOF(&inner.Envelope{
		Kind:     inner.InternalEOF,
		ClientID: "client-z",
		Total:    3,
	})
	if err != nil {
		t.Fatalf("OnUpstreamEOF: %v", err)
	}
	// Multi-replica: the initiator forwards a ring token first; EOFs are emitted
	// after the ring closes (handled via OnRingToken). Here we just assert the
	// kind is ActionForwardToken with a non-nil token.
	if outcome.Action.Kind != eof.ActionForwardToken {
		t.Fatalf("expected ActionForwardToken, got %d", outcome.Action.Kind)
	}
	if outcome.Action.Token == nil {
		t.Fatalf("expected non-nil ring token")
	}
}

func TestSharderQ1MultiReplicaRequiresRingQueues(t *testing.T) {
	t.Setenv("N_FINAL_JOINERS", "2")
	s := New()
	if err := s.Init(strategy.StrategyConfig{
		OutputCount: 1, NReplicas: 2, ReplicaID: 0,
	}); err == nil {
		t.Fatalf("expected error when ring queues are missing for N_REPLICAS>1")
	}
}
