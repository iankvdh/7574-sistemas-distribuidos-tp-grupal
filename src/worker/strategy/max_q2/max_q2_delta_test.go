package max_q2

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func txEnv(t *testing.T, cid inner.ClientID, bank uint32, acc string, amount float64) *inner.Envelope {
	t.Helper()
	body, err := external.SerializeTransaction(&transaction.Transaction{
		FromBank: bank, FromAccount: acc, AmountPaid: amount,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &inner.Envelope{ClientID: cid, Kind: inner.TransactionMessage, Payload: body}
}

func TestMaxQ2DeltaReplayEquivalence(t *testing.T) {
	const cid = inner.ClientID("c")
	live, replayed := New(), New()

	type tx struct {
		bank uint32
		acc  string
		amt  float64
	}
	txs := []tx{
		{1, "a", 10}, {2, "b", 50}, {1, "c", 5},
		{1, "d", 30}, {2, "e", 40}, {1, "f", 100},
		{3, "g", 7}, {1, "h", 99},
	}
	var deltas [][]byte
	for i, x := range txs {
		live.ProcessMessage(txEnv(t, cid, x.bank, x.acc, x.amt))
		if (i+1)%3 == 0 {
			if d := live.TakeDelta(cid); d != nil {
				deltas = append(deltas, d)
			}
		}
	}
	if d := live.TakeDelta(cid); d != nil {
		deltas = append(deltas, d)
	}
	for _, d := range deltas {
		if err := replayed.ApplyDelta(cid, d); err != nil {
			t.Fatal(err)
		}
	}

	ls, rs := live.state[cid], replayed.state[cid]
	if len(ls.maxes) != len(rs.maxes) {
		t.Fatalf("maxes size: live=%d replayed=%d", len(ls.maxes), len(rs.maxes))
	}
	for bank, lm := range ls.maxes {
		rm := rs.maxes[bank]
		if rm == nil || rm.Amount != lm.Amount || rm.FromAccount != lm.FromAccount {
			t.Errorf("bank %d: live=%v replayed=%v", bank, lm, rm)
		}
	}
	if ls.maxes[1].Amount != 100 || ls.maxes[1].FromAccount != "f" {
		t.Fatalf("bank 1 max wrong: %v", ls.maxes[1])
	}
}
