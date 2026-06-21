package pathfinder

import (
	"fmt"
	"sort"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

func pfEnv(t *testing.T, clientID inner.ClientID, from, to string, bySource bool) *inner.Envelope {
	t.Helper()
	body, err := inner.SerializeShardedTx(&inner.ShardedTx{
		FromBank: 1, FromAccount: from, ToBank: 1, ToAccount: to, ShardedBySource: bySource,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &inner.Envelope{ClientID: clientID, Kind: inner.ShardedTxMessage, Payload: body}
}

func pfGraph(p *PathFinder, clientID inner.ClientID) []string {
	var lines []string
	for m, accSt := range p.state[clientID].accounts {
		for a := range accSt.inSet {
			lines = append(lines, fmt.Sprintf("%v<-in-%v", m, a))
		}
		for b := range accSt.outSet {
			lines = append(lines, fmt.Sprintf("%v-out->%v", m, b))
		}
	}
	sort.Strings(lines)
	return lines
}

func TestPathfinderDeltaReplayEquivalence(t *testing.T) {
	const cid = inner.ClientID("c")
	live, replayed := New(), New()
	type tx struct {
		from, to string
		bySource bool
	}
	txs := []tx{
		{"M1", "B1", true}, {"A1", "M1", false}, {"M1", "B1", true},
		{"M1", "B2", true}, {"A2", "M1", false}, {"M2", "B3", true},
		{"A1", "M1", false}, {"M2", "B3", true},
	}
	var deltas [][]byte
	for i, x := range txs {
		live.ProcessMessage(pfEnv(t, cid, x.from, x.to, x.bySource))
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
	lg, rg := pfGraph(live, cid), pfGraph(replayed, cid)
	if fmt.Sprint(lg) != fmt.Sprint(rg) {
		t.Fatalf("graph mismatch:\n live=%v\n repl=%v", lg, rg)
	}
}
