package eof

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

type RingCoordinator struct {
	replicaID int
	nReplicas int
	state     map[inner.ClientID]*ringState
}

type ringState struct {
	isInitiator   bool
	upstreamTotal uint32
}

type RingResult struct {
	AggMatched    uint64
	AggNotMatched uint64
}

func NewRingCoordinator(replicaID, nReplicas int) *RingCoordinator {
	return &RingCoordinator{
		replicaID: replicaID,
		nReplicas: nReplicas,
		state:     map[inner.ClientID]*ringState{},
	}
}

func (r *RingCoordinator) OnUpstreamEOF(clientID inner.ClientID, upstreamTotal uint32, localMatched, localNotMatched uint64) (Action, *RingResult) {
	state := r.stateFor(clientID)
	state.isInitiator = true
	state.upstreamTotal = upstreamTotal

	if r.nReplicas <= 1 {
		return r.tryFinalize(clientID, localMatched, localNotMatched)
	}

	return Action{
		Kind: ActionForwardToken,
		Token: &Token{
			ClientID:      clientID,
			InitiatorID:   r.replicaID,
			AggMatched:    localMatched,
			AggNotMatched: localNotMatched,
		},
	}, nil
}

func (r *RingCoordinator) OnRingToken(token *Token, localMatched, localNotMatched uint64) (Action, *RingResult) {
	if token.InitiatorID == r.replicaID {
		state := r.state[token.ClientID]
		if state == nil || !state.isInitiator {
			return Action{Kind: ActionNone}, nil
		}
		return r.tryFinalize(token.ClientID, token.AggMatched, token.AggNotMatched)
	}
	token.AggMatched += localMatched
	token.AggNotMatched += localNotMatched
	return Action{Kind: ActionForwardToken, Token: token}, nil
}

func (r *RingCoordinator) tryFinalize(clientID inner.ClientID, aggMatched, aggNotMatched uint64) (Action, *RingResult) {
	state := r.state[clientID]
	aggTotal := aggMatched + aggNotMatched
	if uint32(aggTotal) != state.upstreamTotal {
		delete(r.state, clientID)
		return Action{Kind: ActionReenqueueUpstreamEOF}, nil
	}
	delete(r.state, clientID)
	return Action{Kind: ActionEmitEOFs}, &RingResult{AggMatched: aggMatched, AggNotMatched: aggNotMatched}
}

func (r *RingCoordinator) stateFor(clientID inner.ClientID) *ringState {
	state, ok := r.state[clientID]
	if !ok {
		state = &ringState{}
		r.state[clientID] = state
	}
	return state
}
