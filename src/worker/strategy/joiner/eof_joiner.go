package joiner

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type joinerCheckpoint struct {
	ReceivedFrom map[string]uint32 `json:"received_from"`
	Received     uint64            `json:"received"`
	AllEOFsSeen  bool              `json:"all_eofs_seen"`
}

const defaultExpectedEOFs = 3 // period1, period2, other_periods

type joinerState struct {
	receivedFrom map[string]uint32 // "stageType:replicaID" → Total received
	received     uint64
	allEOFsSeen  bool
}

func (st *joinerState) aggTotal() uint64 {
	var total uint64
	for _, t := range st.receivedFrom {
		total += uint64(t)
	}
	return total
}

type EOFJoiner struct {
	strategy.NoopValidator
	cfg          strategy.StrategyConfig
	expectedEOFs int
	state        map[inner.ClientID]*joinerState
}

func NewEOFJoiner() *EOFJoiner {
	return &EOFJoiner{state: map[inner.ClientID]*joinerState{}}
}

func (j *EOFJoiner) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount < 1 {
		return fmt.Errorf("joiner_usd requires at least 1 output, got %d", cfg.OutputCount)
	}
	expected := defaultExpectedEOFs
	if raw := os.Getenv("EXPECTED_EOFS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("invalid EXPECTED_EOFS=%q", raw)
		}
		expected = parsed
	}
	j.cfg = cfg
	j.expectedEOFs = expected
	return nil
}

func (j *EOFJoiner) ProcessMessage(env *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("joiner_usd expects TransactionMessage, got kind=%d", env.Kind)
	}
	j.stateFor(env.ClientID).received++
	indices := make([]int, j.cfg.OutputCount)
	for i := range indices {
		indices[i] = i
	}
	return []strategy.OutputMessage{{
		OutputIndices: indices,
		Body:          env.Payload,
		ClientID:      env.ClientID,
	}}, strategy.LocalCounts{Processed: 1}, nil
}

func (j *EOFJoiner) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	st := j.stateFor(env.ClientID)
	if st.allEOFsSeen {
		return noneOutcome(), nil
	}
	key := fmt.Sprintf("%d:%d", env.SenderStageType, env.SenderReplicaID)
	if _, ok := st.receivedFrom[key]; ok {
		return noneOutcome(), nil // idempotent re-delivery
	}
	st.receivedFrom[key] = env.Total
	if len(st.receivedFrom) < j.expectedEOFs {
		return noneOutcome(), nil
	}
	st.allEOFsSeen = true
	if st.received >= st.aggTotal() {
		return j.emitOutcome(env.ClientID), nil
	}
	return noneOutcome(), nil
}

func (j *EOFJoiner) ReadyEOFs(env *inner.Envelope) (strategy.EOFOutcome, bool) {
	st := j.state[env.ClientID]
	if st == nil || !st.allEOFsSeen || st.received < st.aggTotal() {
		return strategy.EOFOutcome{}, false
	}
	return j.emitOutcome(env.ClientID), true
}

func (j *EOFJoiner) OnRingToken(_ *eof.Token, _ uint64) (strategy.EOFOutcome, error) {
	return noneOutcome(), nil
}

func (j *EOFJoiner) emitOutcome(clientID inner.ClientID) strategy.EOFOutcome {
	st := j.state[clientID]
	total := st.aggTotal()
	emits := make([]eof.EOFEmit, j.cfg.OutputCount)
	for i := 0; i < j.cfg.OutputCount; i++ {
		emits[i] = eof.EOFEmit{OutputIndex: i, Total: uint32(total)}
	}
	return strategy.EOFOutcome{
		Action:          eof.Action{Kind: eof.ActionEmitEOFs},
		EOFs:            emits,
		ClientCompleted: true,
	}
}

func (j *EOFJoiner) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	state := j.state[clientID]
	if state == nil {
		return json.Marshal(joinerCheckpoint{ReceivedFrom: map[string]uint32{}})
	}
	return json.Marshal(joinerCheckpoint{
		ReceivedFrom: state.receivedFrom,
		Received:     state.received,
		AllEOFsSeen:  state.allEOFsSeen,
	})
}

func (j *EOFJoiner) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint joinerCheckpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	j.state[clientID] = &joinerState{
		receivedFrom: checkPoint.ReceivedFrom,
		received:     checkPoint.Received,
		allEOFsSeen:  checkPoint.AllEOFsSeen,
	}
	return nil
}

func (j *EOFJoiner) CleanupClient(clientID inner.ClientID) {
	delete(j.state, clientID)
}

func (j *EOFJoiner) stateFor(clientID inner.ClientID) *joinerState {
	st, ok := j.state[clientID]
	if !ok {
		st = &joinerState{receivedFrom: map[string]uint32{}}
		j.state[clientID] = st
	}
	return st
}

func noneOutcome() strategy.EOFOutcome {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}
}
