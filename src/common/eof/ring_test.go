package eof

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

func TestRingSingleReplicaEmitsImmediately(t *testing.T) {
	c := NewRingCoordinator(0, 1)
	action, result := c.OnUpstreamEOF("client-x", 5, 3, 2)
	if action.Kind != ActionEmitEOFs {
		t.Fatalf("expected ActionEmitEOFs, got %v", action.Kind)
	}
	if result == nil || result.AggMatched != 3 || result.AggNotMatched != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRingSingleReplicaReenqueuesWhenCountsMismatch(t *testing.T) {
	c := NewRingCoordinator(0, 1)
	action, _ := c.OnUpstreamEOF("client-x", 10, 3, 2)
	if action.Kind != ActionReenqueueUpstreamEOF {
		t.Fatalf("expected ActionReenqueueUpstreamEOF, got %v", action.Kind)
	}
}

func TestRingMultiReplicaForwardsTokenFromInitiator(t *testing.T) {
	initiator := NewRingCoordinator(0, 3)
	action, result := initiator.OnUpstreamEOF("client-x", 10, 4, 2)
	if action.Kind != ActionForwardToken {
		t.Fatalf("expected ActionForwardToken, got %v", action.Kind)
	}
	if result != nil {
		t.Fatalf("expected no RingResult on first hop, got %+v", result)
	}
	if action.Token.InitiatorID != 0 || action.Token.AggMatched != 4 || action.Token.AggNotMatched != 2 {
		t.Fatalf("unexpected token: %+v", action.Token)
	}
}

func TestRingMultiReplicaIntermediateSumsAndForwards(t *testing.T) {
	intermediate := NewRingCoordinator(1, 3)
	token := &Token{ClientID: "client-x", InitiatorID: 0, AggMatched: 4, AggNotMatched: 2}
	action, _ := intermediate.OnRingToken(token, 1, 3)
	if action.Kind != ActionForwardToken {
		t.Fatalf("expected ActionForwardToken, got %v", action.Kind)
	}
	if action.Token.AggMatched != 5 || action.Token.AggNotMatched != 5 {
		t.Fatalf("intermediate must sum local counts: %+v", action.Token)
	}
}

func TestRingMultiReplicaInitiatorFinalizes(t *testing.T) {
	initiator := NewRingCoordinator(0, 3)
	// Initiator first receives upstream EOF with total=10; the token it emits is ignored here.
	_, _ = initiator.OnUpstreamEOF("client-x", 10, 4, 2)
	// Token comes back fully aggregated to (5,5) → total 10 matches upstream.
	token := &Token{ClientID: "client-x", InitiatorID: 0, AggMatched: 5, AggNotMatched: 5}
	action, result := initiator.OnRingToken(token, 999, 999) // initiator should not re-sum local counts
	if action.Kind != ActionEmitEOFs {
		t.Fatalf("expected ActionEmitEOFs, got %v", action.Kind)
	}
	if result == nil || result.AggMatched != 5 || result.AggNotMatched != 5 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRingMultiReplicaInitiatorReenqueuesOnMismatch(t *testing.T) {
	initiator := NewRingCoordinator(0, 3)
	_, _ = initiator.OnUpstreamEOF("client-x", 10, 4, 2)
	token := &Token{ClientID: "client-x", InitiatorID: 0, AggMatched: 5, AggNotMatched: 4} // total 9 != 10
	action, result := initiator.OnRingToken(token, 0, 0)
	if action.Kind != ActionReenqueueUpstreamEOF {
		t.Fatalf("expected ActionReenqueueUpstreamEOF, got %v", action.Kind)
	}
	if result != nil {
		t.Fatalf("expected nil result on mismatch, got %+v", result)
	}
}

func TestRingFullSimulationN3(t *testing.T) {
	// Three replicas processing distinct portions of the input. The token should
	// complete a single round and converge at the initiator.
	c0 := NewRingCoordinator(0, 3)
	c1 := NewRingCoordinator(1, 3)
	c2 := NewRingCoordinator(2, 3)

	clientID := inner.ClientID("client-x")
	upstreamTotal := uint32(12)

	// Each replica processed some matches / non-matches before the EOF arrived.
	locals := []struct{ matched, notMatched uint64 }{
		{matched: 2, notMatched: 1}, // c0
		{matched: 3, notMatched: 2}, // c1
		{matched: 1, notMatched: 3}, // c2
	}
	// Sum = 6 matched + 6 notMatched = 12 → matches upstreamTotal.

	// c0 receives upstream EOF, emits the first token.
	action, _ := c0.OnUpstreamEOF(clientID, upstreamTotal, locals[0].matched, locals[0].notMatched)
	if action.Kind != ActionForwardToken {
		t.Fatalf("c0 expected ActionForwardToken, got %v", action.Kind)
	}
	token := action.Token

	// c1 intermediate
	action, _ = c1.OnRingToken(token, locals[1].matched, locals[1].notMatched)
	if action.Kind != ActionForwardToken {
		t.Fatalf("c1 expected ActionForwardToken, got %v", action.Kind)
	}
	token = action.Token

	// c2 intermediate
	action, _ = c2.OnRingToken(token, locals[2].matched, locals[2].notMatched)
	if action.Kind != ActionForwardToken {
		t.Fatalf("c2 expected ActionForwardToken, got %v", action.Kind)
	}
	token = action.Token

	// Back to c0: finalize
	action, result := c0.OnRingToken(token, locals[0].matched, locals[0].notMatched)
	if action.Kind != ActionEmitEOFs {
		t.Fatalf("c0 finalize expected ActionEmitEOFs, got %v", action.Kind)
	}
	if result.AggMatched != 6 || result.AggNotMatched != 6 {
		t.Fatalf("aggregated counts wrong: %+v", result)
	}
}
