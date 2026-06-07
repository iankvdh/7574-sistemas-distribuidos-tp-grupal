package joiner

import (
	"fmt"
	"os"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const defaultExpectedEOFs = 3 // period1, period2, other_periods

type joinerState struct {
	eofsSeen    int
	aggTotal    uint64
	received    uint64
	allEOFsSeen bool
}

type EOFJoiner struct {
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
	st.eofsSeen++
	st.aggTotal += uint64(env.Total)
	if st.eofsSeen < j.expectedEOFs {
		return noneOutcome(), nil
	}
	st.allEOFsSeen = true
	if st.received >= st.aggTotal {
		return j.emitOutcome(env.ClientID), nil
	}
	return noneOutcome(), nil
}

func (j *EOFJoiner) ReadyEOFs(env *inner.Envelope) (strategy.EOFOutcome, bool) {
	st := j.state[env.ClientID]
	if st == nil || !st.allEOFsSeen || st.received < st.aggTotal {
		return strategy.EOFOutcome{}, false
	}
	return j.emitOutcome(env.ClientID), true
}

func (j *EOFJoiner) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return noneOutcome(), nil
}

func (j *EOFJoiner) emitOutcome(clientID inner.ClientID) strategy.EOFOutcome {
	st := j.state[clientID]
	emits := make([]eof.EOFEmit, j.cfg.OutputCount)
	for i := 0; i < j.cfg.OutputCount; i++ {
		emits[i] = eof.EOFEmit{OutputIndex: i, Total: uint32(st.aggTotal)}
	}
	delete(j.state, clientID)
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionEmitEOFs}, EOFs: emits}
}

func (j *EOFJoiner) stateFor(clientID inner.ClientID) *joinerState {
	st, ok := j.state[clientID]
	if !ok {
		st = &joinerState{}
		j.state[clientID] = st
	}
	return st
}

func noneOutcome() strategy.EOFOutcome {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}
}
