package bank_aggregator

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

func accEnvelope(t *testing.T, clientID inner.ClientID, bankID uint32, bankName string) *inner.Envelope {
	t.Helper()
	body, err := external.SerializeAccount(&account.Account{BankID: bankID, BankName: bankName})
	if err != nil {
		t.Fatalf("serialize account: %v", err)
	}
	return &inner.Envelope{ClientID: clientID, Kind: inner.AccountMessage, Payload: body}
}

func TestDeltaReplayEquivalence(t *testing.T) {
	const clientID = inner.ClientID("c1")
	live := New()
	replayed := New()

	var deltas [][]byte
	for i := 0; i < 200; i++ {
		bankID := uint32(i % 17)
		name := "BANK_" + string(rune('A'+i%17))
		if _, _, err := live.ProcessMessage(accEnvelope(t, clientID, bankID, name)); err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
		if (i+1)%7 == 0 {
			if d := live.TakeDelta(clientID); d != nil {
				deltas = append(deltas, d)
			}
		}
	}
	if d := live.TakeDelta(clientID); d != nil {
		deltas = append(deltas, d)
	}

	for _, d := range deltas {
		if err := replayed.ApplyDelta(clientID, d); err != nil {
			t.Fatalf("ApplyDelta: %v", err)
		}
	}

	ls := live.stateFor(clientID)
	rs := replayed.stateFor(clientID)
	if len(ls.bankNames) != len(rs.bankNames) {
		t.Fatalf("bankNames size mismatch: live=%d replayed=%d", len(ls.bankNames), len(rs.bankNames))
	}
	for id, name := range ls.bankNames {
		if rs.bankNames[id] != name {
			t.Errorf("bank %d: live=%q replayed=%q", id, name, rs.bankNames[id])
		}
	}
}

func TestTakeDeltaResets(t *testing.T) {
	const clientID = inner.ClientID("c2")
	b := New()
	b.ProcessMessage(accEnvelope(t, clientID, 5, "FOO"))
	if d := b.TakeDelta(clientID); d == nil {
		t.Fatal("expected a delta after processing")
	}
	if d := b.TakeDelta(clientID); d != nil {
		t.Fatalf("expected nil on second TakeDelta, got %q", d)
	}
}
