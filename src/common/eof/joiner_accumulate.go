package eof

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

// JoinerAccumulateCoordinator implements an EOF topology that waits for a fixed
// number of upstream EOFs per client before emitting a single aggregated EOF to
// downstream. It is used by strategies that merge multiple upstream branches
// (e.g. joiner_usd merging USD transactions from period1, period2 and "other dates").
type JoinerAccumulateCoordinator struct {
	expectedEOFs int
	state        map[inner.ClientID]*accumState
}

type accumState struct {
	eofsSeen int
	aggTotal uint64
}

// NewJoinerAccumulateCoordinator builds a coordinator that expects `expectedEOFs`
// EOFs per client before forwarding a unified one.
func NewJoinerAccumulateCoordinator(expectedEOFs int) *JoinerAccumulateCoordinator {
	return &JoinerAccumulateCoordinator{
		expectedEOFs: expectedEOFs,
		state:        map[inner.ClientID]*accumState{},
	}
}

// OnUpstreamEOF accumulates one upstream EOF for the given client. Returns an
// Action{Kind: ActionEmitEOFs} with a single EOFEmit (OutputIndex=0, Total=aggregated)
// once the expected count has been reached. Otherwise returns ActionNone (wait).
func (j *JoinerAccumulateCoordinator) OnUpstreamEOF(clientID inner.ClientID, upstreamTotal uint32) Action {
	state, ok := j.state[clientID]
	if !ok {
		state = &accumState{}
		j.state[clientID] = state
	}
	state.eofsSeen++
	state.aggTotal += uint64(upstreamTotal)

	if state.eofsSeen < j.expectedEOFs {
		return Action{Kind: ActionNone}
	}

	delete(j.state, clientID)
	return Action{
		Kind: ActionEmitEOFs,
		EOFs: []EOFEmit{{OutputIndex: 0, Total: uint32(state.aggTotal)}},
	}
}
