package noop

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const name = "noop"

// Strategy forwards every message to all configured outputs unchanged.
type Strategy struct {
	cfg    strategy.StrategyConfig
	counts map[inner.ClientID]uint64
}

func New() *Strategy {
	return &Strategy{counts: map[inner.ClientID]uint64{}}
}

func (s *Strategy) Init(cfg strategy.StrategyConfig) error {
	s.cfg = cfg
	return nil
}

func (s *Strategy) Name() string { return name }

func (s *Strategy) ProcessMessage(env *inner.Envelope) ([]strategy.Decision, strategy.LocalCounts, error) {
	indices := make([]int, 0, s.cfg.OutputCount)
	for i := 0; i < s.cfg.OutputCount; i++ {
		indices = append(indices, i)
	}
	s.counts[env.ClientID]++
	return []strategy.Decision{{OutputIndices: indices, Body: env.Payload, ClientID: env.ClientID}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (s *Strategy) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	emits := make([]eof.EOFEmit, 0, s.cfg.OutputCount)
	for i := 0; i < s.cfg.OutputCount; i++ {
		emits = append(emits, eof.EOFEmit{OutputIndex: i, Total: uint32(s.counts[env.ClientID])})
	}
	delete(s.counts, env.ClientID)
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionEmitEOFs}, EOFs: emits}, nil
}

func (s *Strategy) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

