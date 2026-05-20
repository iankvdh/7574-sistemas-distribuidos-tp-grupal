// Package joiner contains strategies that merge multiple upstream branches into
// a single downstream stream by accumulating their EOFs.
//
// joiner_usd is the only joiner so far: it forwards every transaction it receives
// to its single output (1:1) and waits for N upstream EOFs per client before
// emitting one unified EOF downstream. This isolates the rest of the pipeline
// from the fan-in fact that USD transactions come from three different periods.
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

// EOFJoiner implements the strategy contract for joiner_usd.
type EOFJoiner struct {
	name         string
	ctx          strategy.Context
	coordinator  *eof.JoinerAccumulateCoordinator
	expectedEOFs int
}

func init() {
	strategy.Register("joiner_usd", func() (strategy.Strategy, error) {
		return &EOFJoiner{name: "joiner_usd"}, nil
	})
}

func (j *EOFJoiner) Name() string { return j.name }

func (j *EOFJoiner) Init(ctx strategy.Context) error {
	if ctx.OutputCount != 1 {
		return fmt.Errorf("joiner_usd requires exactly 1 output, got %d", ctx.OutputCount)
	}
	expected := defaultExpectedEOFs
	if raw := os.Getenv("EXPECTED_EOFS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("invalid EXPECTED_EOFS=%q", raw)
		}
		expected = parsed
	}
	j.ctx = ctx
	j.expectedEOFs = expected
	j.coordinator = eof.NewJoinerAccumulateCoordinator(expected)
	return nil
}

// ProcessMessage forwards each transaction 1:1 to the single output, attaching
// the client_id for sharding if the output happens to be sharded (it usually isn't
// in this strategy, but the runtime will ignore the routing key otherwise).
func (j *EOFJoiner) ProcessMessage(env *inner.Envelope) ([]strategy.Decision, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("joiner_usd expects TransactionMessage, got kind=%d", env.Kind)
	}
	body, err := encodeEnvelope(env)
	if err != nil {
		return nil, strategy.LocalCounts{}, err
	}
	return []strategy.Decision{{
		OutputIndices: []int{0},
		Body:          body,
		ClientID:      env.ClientID,
	}}, strategy.LocalCounts{Processed: 1}, nil
}

func (j *EOFJoiner) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	action := j.coordinator.OnUpstreamEOF(env.ClientID, env.Total)
	return strategy.EOFOutcome{Action: action, EOFs: action.EOFs}, nil
}

func (j *EOFJoiner) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	// joiner_usd does not participate in a ring.
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func encodeEnvelope(env *inner.Envelope) (string, error) {
	msg, err := inner.SerializeEnvelope(env.Kind, env.GatewayID, env.ClientID, env.Total, env.Payload)
	if err != nil {
		return "", err
	}
	return msg.Body, nil
}
