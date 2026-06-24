package counter

import (
	"fmt"
	"sort"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

func pathEnv(t *testing.T, cid inner.ClientID, src, interm, dst string) *inner.Envelope {
	t.Helper()
	body, err := inner.SerializeSuspiciousPath(&inner.SuspiciousPath{
		SourceBank: 1, SourceAccount: src,
		IntermediateBank: 1, IntermediateAccount: interm,
		DestBank: 1, DestAccount: dst,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &inner.Envelope{ClientID: cid, Kind: inner.SuspiciousPathMessage, Payload: body}
}

func counterRepr(c *Counter, cid inner.ClientID) []string {
	var lines []string
	for k, ps := range c.state[cid].pairs {
		lines = append(lines, fmt.Sprintf("%v q=%v n=%d", k, ps.qualified, len(ps.intermediaries)))
	}
	sort.Strings(lines)
	return lines
}

func TestCounterDeltaReplayEquivalence(t *testing.T) {
	const cid = inner.ClientID("c")
	live, replayed := New(), New()
	live.minIntermediates, replayed.minIntermediates = 3, 3

	msgs := [][3]string{
		{"A", "I1", "B"}, {"A", "I2", "B"}, {"C", "I1", "D"},
		{"A", "I1", "B"},
		{"A", "I3", "B"},
		{"A", "I4", "B"},
		{"C", "I2", "D"},
	}
	var deltas [][]byte
	for i, m := range msgs {
		live.ProcessMessage(pathEnv(t, cid, m[0], m[1], m[2]))
		if (i+1)%2 == 0 {
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
	lr, rr := counterRepr(live, cid), counterRepr(replayed, cid)
	if fmt.Sprint(lr) != fmt.Sprint(rr) {
		t.Fatalf("pairs mismatch:\n live=%v\n repl=%v", lr, rr)
	}
	if !live.state[cid].pairs[pairKey{accountKey{1, "A"}, accountKey{1, "B"}}].qualified {
		t.Fatal("A→B should be qualified")
	}
	if live.state[cid].pairs[pairKey{accountKey{1, "C"}, accountKey{1, "D"}}].qualified {
		t.Fatal("C→D should NOT be qualified")
	}
}
