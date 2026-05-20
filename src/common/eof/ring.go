package eof

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

// RingCoordinator implements the single-round ring EOF protocol used by filter
// strategies running with N replicas.
//
// Semantics:
//   - Whichever replica receives the upstream EOF (via round-robin on the input
//     queue) becomes the "initiator" for that client. It launches a Token over
//     the ring with its local counts already added.
//   - Every non-initiator replica that sees the Token sums its own local counts
//     and forwards it.
//   - When the Token returns to the initiator (InitiatorID == this replica) it
//     compares aggMatched + aggNotMatched against the upstreamTotal recorded on
//     receipt. If they match, the initiator emits the aggregated EOFs to each
//     output. If they don't, the initiator re-enqueues the upstream EOF on the
//     input queue (giving slow replicas more time to drain their buffers); the
//     ring restarts whenever another upstream EOF arrives.
//
// The coordinator itself does not know the output layout (match vs no-match):
// it just returns the aggregated counts via the Action's EOFs slice (with
// OutputIndex=-1 to signal "use the strategy's layout"). The runtime (or the
// strategy) translates those counts into actual EOFEmit entries.
type RingCoordinator struct {
	replicaID int
	nReplicas int
	state     map[inner.ClientID]*ringState
}

type ringState struct {
	isInitiator   bool
	upstreamTotal uint32
}

// RingResult conveys the aggregated counts the strategy can use to build EOFEmit entries.
type RingResult struct {
	AggMatched    uint64
	AggNotMatched uint64
}

// NewRingCoordinator builds a coordinator for a strategy running on `replicaID`
// out of `nReplicas` total replicas.
func NewRingCoordinator(replicaID, nReplicas int) *RingCoordinator {
	return &RingCoordinator{
		replicaID: replicaID,
		nReplicas: nReplicas,
		state:     map[inner.ClientID]*ringState{},
	}
}

// OnUpstreamEOF is invoked when an EOF arrives on the input queue. The replica
// records itself as initiator and emits the first Token. With N=1, no token is
// needed: the call returns an EmitEOFs action directly.
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

// OnRingToken is invoked when a Token arrives on this replica's private ring queue.
// If the token has completed the ring (InitiatorID == this replica), the coordinator
// validates the totals and either returns an EmitEOFs (with the aggregated counts in
// RingResult) or a Reenqueue action. Otherwise it returns a Forward with the token
// updated with this replica's counts.
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
