package noop

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const name = "noop"

// Strategy forwards every message it receives to all configured outputs unchanged.
// Used in Phase 0 to validate the worker scaffolding end-to-end. No ring topology.
type Strategy struct {
	ctx    strategy.Context
	counts map[inner.ClientID]uint64
}

func init() {
	strategy.Register(name, func() (strategy.Strategy, error) {
		return &Strategy{counts: map[inner.ClientID]uint64{}}, nil
	})
}

func (s *Strategy) Init(ctx strategy.Context) error {
	s.ctx = ctx
	return nil
}

func (s *Strategy) Name() string { return name }

func (s *Strategy) ProcessMessage(env *inner.Envelope) ([]strategy.Decision, strategy.LocalCounts, error) {
	indices := make([]int, 0, s.ctx.OutputCount)
	for i := 0; i < s.ctx.OutputCount; i++ {
		indices = append(indices, i)
	}
	body, err := encodeEnvelope(env)
	if err != nil {
		return nil, strategy.LocalCounts{}, err
	}
	s.counts[env.ClientID]++
	return []strategy.Decision{{OutputIndices: indices, Body: body, ClientID: env.ClientID}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (s *Strategy) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	emits := make([]eof.EOFEmit, 0, s.ctx.OutputCount)
	for i := 0; i < s.ctx.OutputCount; i++ {
		emits = append(emits, eof.EOFEmit{OutputIndex: i, Total: uint32(s.counts[env.ClientID])})
	}
	delete(s.counts, env.ClientID)
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionEmitEOFs}, EOFs: emits}, nil
}

func (s *Strategy) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	// noop strategy does not participate in a ring.
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func encodeEnvelope(env *inner.Envelope) (string, error) {
	msg, err := inner.SerializeEnvelope(env.Kind, env.GatewayID, env.ClientID, env.Total, env.Payload)
	if err != nil {
		return "", err
	}
	return msg.Body, nil
}
