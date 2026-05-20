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
	outputCount  int
	state        map[inner.ClientID]*accumState
}

type accumState struct {
	eofsSeen int
	aggTotal uint64
}

// NewJoinerAccumulateCoordinator builds a coordinator that waits for
// `expectedEOFs` upstream EOFs per client and then emits one aggregated EOF to
// each of `outputCount` downstream outputs.
func NewJoinerAccumulateCoordinator(expectedEOFs, outputCount int) *JoinerAccumulateCoordinator {
	return &JoinerAccumulateCoordinator{
		expectedEOFs: expectedEOFs,
		outputCount:  outputCount,
		state:        map[inner.ClientID]*accumState{},
	}
}

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
	emits := make([]EOFEmit, j.outputCount)
	for i := 0; i < j.outputCount; i++ {
		emits[i] = EOFEmit{OutputIndex: i, Total: uint32(state.aggTotal)}
	}
	return Action{Kind: ActionEmitEOFs, EOFs: emits}
}
