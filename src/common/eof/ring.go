package eof

import (
	"log/slog"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

type RingCoordinator struct {
	replicaID           int
	nReplicas           int
	broadcastEOFOnClose bool
	state               map[inner.ClientID]*RingStateSnapshot
}

type RingStateSnapshot struct {
	IsInitiator     bool
	UpstreamTotal   uint32
	Phase           int // 0=collecting, 1=closing
	LocalMatched    uint64
	LocalNotMatched uint64
}

type RingResult struct {
	AggMatched    uint64
	AggNotMatched uint64
}

func NewRingCoordinator(replicaID, nReplicas int) *RingCoordinator {
	return &RingCoordinator{
		replicaID: replicaID,
		nReplicas: nReplicas,
		state:     map[inner.ClientID]*RingStateSnapshot{},
	}
}

func NewBroadcastRingCoordinator(replicaID, nReplicas int) *RingCoordinator {
	c := NewRingCoordinator(replicaID, nReplicas)
	c.broadcastEOFOnClose = true
	return c
}

func (r *RingCoordinator) OnUpstreamEOF(clientID inner.ClientID, upstreamTotal uint32, localMatched, localNotMatched uint64) (Action, *RingResult) {
	state := r.stateFor(clientID)
	state.IsInitiator = true
	state.UpstreamTotal = upstreamTotal
	state.LocalMatched = localMatched
	state.LocalNotMatched = localNotMatched

	if r.nReplicas <= 1 {
		return r.tryFinalize(clientID, localMatched, localNotMatched)
	}

	state.Phase = 0 // collecting
	return Action{
		Kind: ActionForwardToken,
		Token: &Token{
			ClientID:      clientID,
			InitiatorID:   r.replicaID,
			AggMatched:    localMatched,
			AggNotMatched: localNotMatched,
			Phase:         PhaseCollecting,
		},
	}, nil
}

func (r *RingCoordinator) OnRingToken(token *Token, localMatched, localNotMatched uint64) (Action, *RingResult) {
	if token.Phase == PhaseClosing {
		return r.onClosingToken(token)
	}

	if token.InitiatorID == r.replicaID {
		state := r.state[token.ClientID]
		if state == nil || !state.IsInitiator {
			return Action{Kind: ActionNone}, nil
		}
		return r.tryFinalize(token.ClientID, token.AggMatched, token.AggNotMatched)
	}
	token.AggMatched += localMatched
	token.AggNotMatched += localNotMatched
	return Action{Kind: ActionForwardToken, Token: token}, nil
}

func (r *RingCoordinator) onClosingToken(token *Token) (Action, *RingResult) {
	if token.InitiatorID == r.replicaID {
		delete(r.state, token.ClientID)
		return Action{Kind: ActionEmitEOFs}, &RingResult{
			AggMatched:    token.AggMatched,
			AggNotMatched: token.AggNotMatched,
		}
	}
	return Action{
			Kind:  ActionEmitEOFsAndForwardToken,
			Token: token,
		}, &RingResult{
			AggMatched:    token.AggMatched,
			AggNotMatched: token.AggNotMatched,
		}
}

func (r *RingCoordinator) tryFinalize(clientID inner.ClientID, aggMatched, aggNotMatched uint64) (Action, *RingResult) {
	state := r.state[clientID]
	aggTotal := aggMatched + aggNotMatched
	if uint32(aggTotal) > state.UpstreamTotal {
		slog.Error("ring: aggTotal exceeded UpstreamTotal — this should never happen",
			"clientID", clientID,
			"aggTotal", aggTotal,
			"upstreamTotal", state.UpstreamTotal,
		)
		delete(r.state, clientID)
		return Action{Kind: ActionReenqueueUpstreamEOF}, nil
	}
	if uint32(aggTotal) < state.UpstreamTotal {
		delete(r.state, clientID)
		return Action{Kind: ActionReenqueueUpstreamEOF}, nil
	}

	if r.broadcastEOFOnClose && r.nReplicas > 1 {
		state.Phase = 1 // closing
		return Action{
			Kind: ActionForwardToken,
			Token: &Token{
				ClientID:      clientID,
				InitiatorID:   r.replicaID,
				AggMatched:    aggMatched,
				AggNotMatched: aggNotMatched,
				Phase:         PhaseClosing,
			},
		}, nil
	}

	delete(r.state, clientID)
	return Action{Kind: ActionEmitEOFs}, &RingResult{AggMatched: aggMatched, AggNotMatched: aggNotMatched}
}

func (r *RingCoordinator) stateFor(clientID inner.ClientID) *RingStateSnapshot {
	state, ok := r.state[clientID]
	if !ok {
		state = &RingStateSnapshot{}
		r.state[clientID] = state
	}
	return state
}

func (r *RingCoordinator) GetClientRingState(clientID inner.ClientID) (RingStateSnapshot, bool) {
	state, ok := r.state[clientID]
	if !ok {
		return RingStateSnapshot{}, false
	}
	return *state, true
}

func (r *RingCoordinator) RestoreClientRingState(clientID inner.ClientID, snap RingStateSnapshot) {
	r.state[clientID] = &snap
}

func (r *RingCoordinator) CleanupClient(clientID inner.ClientID) {
	delete(r.state, clientID)
}
