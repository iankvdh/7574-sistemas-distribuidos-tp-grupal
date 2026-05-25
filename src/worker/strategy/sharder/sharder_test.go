package sharder

import (
	"os"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func newWithK(t *testing.T, k int) *Sharder {
	t.Helper()
	t.Setenv("K_SUSPICIOUS_FILTERS", itoa(k))
	s := New()
	if err := s.Init(strategy.StrategyConfig{
		OutputCount:  1,
		ReplicaID:    0,
		NReplicas:    1,
		StrategyName: "sharder_q4",
	}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	return s
}

func itoa(n int) string {
	// avoid bringing strconv; tiny custom impl is enough for K∈[1, 99]
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

func TestSharderEmitsTwoMessagesWithExpectedFlags(t *testing.T) {
	s := newWithK(t, 2)
	tx := &transaction.Transaction{
		FromBank: 1, FromAccount: "ACC-A",
		ToBank: 2, ToAccount: "ACC-B",
		AmountPaid:      99.5,
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
	if counts.Processed != 1 {
		t.Fatalf("expected Processed=1, got %d", counts.Processed)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 OutputMessage, got %d", len(out))
	}
	if out[0].BatchItemKind != inner.ShardedTxMessage || out[1].BatchItemKind != inner.ShardedTxMessage {
		t.Fatalf("BatchItemKind must be ShardedTxMessage")
	}
	if out[0].BatchQueryID != 4 || out[1].BatchQueryID != 4 {
		t.Fatalf("BatchQueryID must be 4")
	}
	first, err := inner.DeserializeShardedTx(out[0].Body)
	if err != nil {
		t.Fatalf("deserialize 0: %v", err)
	}
	second, err := inner.DeserializeShardedTx(out[1].Body)
	if err != nil {
		t.Fatalf("deserialize 1: %v", err)
	}
	if !first.ShardedBySource {
		t.Fatalf("first message must be sharded by source")
	}
	if second.ShardedBySource {
		t.Fatalf("second message must be sharded by destination")
	}
	if out[0].RoutingKey == "" || out[1].RoutingKey == "" {
		t.Fatalf("routing keys must be set")
	}
}

func TestSharderRejectsNonTransactionEnvelope(t *testing.T) {
	s := newWithK(t, 1)
	_, _, err := s.ProcessMessage(&inner.Envelope{Kind: inner.AccountMessage, ClientID: "x"})
	if err == nil {
		t.Fatalf("expected error on non-TransactionMessage kind")
	}
}

func TestSharderRequiresKSuspiciousFilters(t *testing.T) {
	os.Unsetenv("K_SUSPICIOUS_FILTERS")
	s := New()
	if err := s.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err == nil {
		t.Fatalf("expected error when K_SUSPICIOUS_FILTERS missing")
	}
}
