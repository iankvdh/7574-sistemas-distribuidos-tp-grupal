// Package joiner merges multiple upstream branches into a single downstream
// stream by accumulating their EOFs. joiner_usd forwards each transaction 1:1
// and emits one unified EOF per client after EXPECTED_EOFS upstream EOFs.
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

type EOFJoiner struct {
	cfg          strategy.StrategyConfig
	coordinator  *eof.JoinerAccumulateCoordinator
	expectedEOFs int
}

func NewEOFJoiner() *EOFJoiner {
	return &EOFJoiner{}
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
	j.coordinator = eof.NewJoinerAccumulateCoordinator(expected, cfg.OutputCount)
	return nil
}

func (j *EOFJoiner) ProcessMessage(env *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("joiner_usd expects TransactionMessage, got kind=%d", env.Kind)
	}
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
	action := j.coordinator.OnUpstreamEOF(env.ClientID, env.Total)
	return strategy.EOFOutcome{Action: action, EOFs: action.EOFs}, nil
}

func (j *EOFJoiner) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

