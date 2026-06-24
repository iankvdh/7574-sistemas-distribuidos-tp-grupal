package eof

import (
	"fmt"
	"maps"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

type JACStateSnapshot struct {
	ReceivedFrom map[string]uint32
	Expected     int
	Finalized bool
}

func (s *JACStateSnapshot) recordEOF(senderStageType uint8, senderReplicaID uint16, total uint32) {
	key := fmt.Sprintf("%d:%d", senderStageType, senderReplicaID)
	if existing, ok := s.ReceivedFrom[key]; ok {
		if existing != total {
			panic(fmt.Sprintf("JAC: duplicate EOF with different Total — protocol bug: key=%s previous=%d new=%d", key, existing, total))
		}
		return
	}
	s.ReceivedFrom[key] = total
}

func (s *JACStateSnapshot) allReceived() bool {
	return len(s.ReceivedFrom) >= s.Expected
}

func (s *JACStateSnapshot) AggTotal() uint64 {
	var total uint64
	for _, t := range s.ReceivedFrom {
		total += uint64(t)
	}
	return total
}

// JoinerAccumulateCoordinator implements an EOF topology that waits for a fixed
// number of upstream EOFs per client before emitting a single aggregated EOF to
// downstream. It is used by strategies that merge multiple upstream branches
// (e.g. joiner_usd merging USD transactions from period1, period2 and "other dates").
type JoinerAccumulateCoordinator struct {
	expectedEOFs int
	outputCount  int
	state        map[inner.ClientID]*JACStateSnapshot
}

// NewJoinerAccumulateCoordinator builds a coordinator that waits for
// `expectedEOFs` upstream EOFs per client and then emits one aggregated EOF to
// each of `outputCount` downstream outputs.
func NewJoinerAccumulateCoordinator(expectedEOFs, outputCount int) *JoinerAccumulateCoordinator {
	return &JoinerAccumulateCoordinator{
		expectedEOFs: expectedEOFs,
		outputCount:  outputCount,
		state:        map[inner.ClientID]*JACStateSnapshot{},
	}
}

func (j *JoinerAccumulateCoordinator) OnUpstreamEOF(clientID inner.ClientID, senderStageType uint8, senderReplicaID uint16, upstreamTotal uint32, localCount uint64) Action {
	state, ok := j.state[clientID]
	if !ok {
		state = &JACStateSnapshot{
			ReceivedFrom: make(map[string]uint32),
			Expected:     j.expectedEOFs,
		}
		j.state[clientID] = state
	}

	state.recordEOF(senderStageType, senderReplicaID, upstreamTotal)

	if state.Finalized {
		return Action{Kind: ActionNone}
	}
	if !state.allReceived() {
		return Action{Kind: ActionNone}
	}

	aggTotal := state.AggTotal()
	if aggTotal > 0 && localCount < aggTotal {
		return Action{Kind: ActionReenqueueUpstreamEOF}
	}

	state.Finalized = true
	emits := make([]EOFEmit, j.outputCount)
	for i := 0; i < j.outputCount; i++ {
		emits[i] = EOFEmit{OutputIndex: i, Total: uint32(aggTotal)}
	}
	return Action{Kind: ActionEmitEOFs, EOFs: emits}
}

func (j *JoinerAccumulateCoordinator) GetClientJACState(clientID inner.ClientID) JACStateSnapshot {
	state, ok := j.state[clientID]
	if !ok {
		return JACStateSnapshot{
			ReceivedFrom: map[string]uint32{},
			Expected:     j.expectedEOFs,
		}
	}
	snap := JACStateSnapshot{
		ReceivedFrom: make(map[string]uint32, len(state.ReceivedFrom)),
		Expected:     state.Expected,
		Finalized:    state.Finalized,
	}
	maps.Copy(snap.ReceivedFrom, state.ReceivedFrom)
	return snap
}

func (j *JoinerAccumulateCoordinator) RestoreClientJACState(clientID inner.ClientID, snap JACStateSnapshot) {
	restored := &JACStateSnapshot{
		ReceivedFrom: make(map[string]uint32, len(snap.ReceivedFrom)),
		Expected:     snap.Expected,
		Finalized:    snap.Finalized,
	}
	maps.Copy(restored.ReceivedFrom, snap.ReceivedFrom)
	j.state[clientID] = restored
}

func (j *JoinerAccumulateCoordinator) CleanupClient(clientID inner.ClientID) {
	delete(j.state, clientID)
}
