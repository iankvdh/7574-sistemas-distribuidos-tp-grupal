package suspicious_filter

import (
	"fmt"
	"sort"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

func newTestFilter(threshold, kPathFinders int) *SuspiciousFilter {
	s := &SuspiciousFilter{state: map[inner.ClientID]*clientState{}}
	s.threshold = threshold
	s.kPathFinders = kPathFinders
	s.rkCache = make([]string, kPathFinders)
	for i := range s.rkCache {
		s.rkCache[i] = fmt.Sprintf("%d", i)
	}
	return s
}

func shardedTxEnv(t *testing.T, clientID inner.ClientID, fromAcc, toAcc string, bySource bool) *inner.Envelope {
	t.Helper()
	body, err := inner.SerializeShardedTx(&inner.ShardedTx{
		FromBank: 1, FromAccount: fromAcc, ToBank: 1, ToAccount: toAcc, ShardedBySource: bySource,
	})
	if err != nil {
		t.Fatalf("serialize sharded tx: %v", err)
	}
	return &inner.Envelope{ClientID: clientID, Kind: inner.ShardedTxMessage, Payload: body}
}

func TestEmitsOnlyAboveThreshold(t *testing.T) {
	const clientID = inner.ClientID("c1")
	s := newTestFilter(3, 2)

	s.ProcessMessage(shardedTxEnv(t, clientID, "A", "B1", true))
	s.ProcessMessage(shardedTxEnv(t, clientID, "A", "B2", true))
	s.ProcessMessage(shardedTxEnv(t, clientID, "A", "B3", true))
	s.ProcessMessage(shardedTxEnv(t, clientID, "A", "B1", true))
	s.ProcessMessage(shardedTxEnv(t, clientID, "X", "Y1", true))
	s.ProcessMessage(shardedTxEnv(t, clientID, "X", "Y2", true))

	outputs, perShard, err := s.buildSuspiciousOutputs(clientID)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 3 {
		t.Fatalf("expected 3 emitted edges (A's distinct dests), got %d", len(outputs))
	}
	var total uint32
	for _, n := range perShard {
		total += n
	}
	if total != 3 {
		t.Fatalf("perShard total=%d, want 3", total)
	}
}

func graphRepr(s *SuspiciousFilter, clientID inner.ClientID) []string {
	cst := s.state[clientID]
	var lines []string
	for k, accSt := range cst.accounts {
		for p := range accSt.outAccounts {
			lines = append(lines, fmt.Sprintf("%v->out->%v", k, p))
		}
		for p := range accSt.inAccounts {
			lines = append(lines, fmt.Sprintf("%v->in->%v", k, p))
		}
	}
	sort.Strings(lines)
	return lines
}

func TestDeltaReplayEquivalence(t *testing.T) {
	const clientID = inner.ClientID("c2")
	live := newTestFilter(3, 2)
	replayed := newTestFilter(3, 2)

	type tx struct {
		from, to string
		bySource bool
	}
	txs := []tx{
		{"A", "B1", true}, {"A", "B2", true}, {"A", "B1", true},
		{"C", "A", false}, {"D", "A", false}, {"A", "B3", true},
		{"E", "F", true}, {"G", "A", false}, {"A", "B2", true},
	}
	var deltas [][]byte
	for i, x := range txs {
		live.ProcessMessage(shardedTxEnv(t, clientID, x.from, x.to, x.bySource))
		if (i+1)%3 == 0 {
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

	lg, rg := graphRepr(live, clientID), graphRepr(replayed, clientID)
	if len(lg) != len(rg) {
		t.Fatalf("graph size mismatch: live=%d replayed=%d", len(lg), len(rg))
	}
	for i := range lg {
		if lg[i] != rg[i] {
			t.Fatalf("graph mismatch at %d: live=%q replayed=%q", i, lg[i], rg[i])
		}
	}

	lo, _, _ := live.buildSuspiciousOutputs(clientID)
	ro, _, _ := replayed.buildSuspiciousOutputs(clientID)
	if len(lo) != len(ro) {
		t.Fatalf("emitted output count mismatch: live=%d replayed=%d", len(lo), len(ro))
	}
}
