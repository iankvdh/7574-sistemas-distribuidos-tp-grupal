package drain

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type q4Pair struct {
	SourceBank    uint32
	SourceAccount string
	DestBank      uint32
	DestAccount   string
}

// Q4Strategy is the terminal sink for the Q4 pipeline. It collects
// Query4PairItem messages from all counter_q4 shards, waits for N_COUNTERS
// upstream EOFs, then sorts and writes each unique (source, dest) pair to
// DRAIN_OUTPUT_FILE in "src_bank,src_acc,dst_bank,dst_acc" CSV format.
type Q4Strategy struct {
	cfg         strategy.StrategyConfig
	mu          sync.Mutex
	file        *os.File
	pairs       map[inner.ClientID]map[q4Pair]struct{}
	coordinator *eof.JoinerAccumulateCoordinator
}

func NewQ4() *Q4Strategy {
	return &Q4Strategy{pairs: map[inner.ClientID]map[q4Pair]struct{}{}}
}

func (s *Q4Strategy) Name() string { return "drain_q4" }

func (s *Q4Strategy) Init(cfg strategy.StrategyConfig) error {
	path := os.Getenv("DRAIN_OUTPUT_FILE")
	if path == "" {
		return errors.New("DRAIN_OUTPUT_FILE is required for drain strategies")
	}
	nCounters, err := env.RequiredInt("N_COUNTERS", true)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open drain output: %w", err)
	}
	s.cfg = cfg
	s.file = f
	s.coordinator = eof.NewJoinerAccumulateCoordinator(nCounters, 0)
	return nil
}

func (s *Q4Strategy) ProcessMessage(env *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if env.Kind != inner.Query4PairItem {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	pair, err := inner.DeserializeQuery4Pair(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize query4 pair: %w", err)
	}
	s.mu.Lock()
	m := s.pairs[env.ClientID]
	if m == nil {
		m = map[q4Pair]struct{}{}
		s.pairs[env.ClientID] = m
	}
	m[q4Pair{
		SourceBank:    pair.SourceBank,
		SourceAccount: pair.SourceAccount,
		DestBank:      pair.DestBank,
		DestAccount:   pair.DestAccount,
	}] = struct{}{}
	s.mu.Unlock()
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (s *Q4Strategy) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	action := s.coordinator.OnUpstreamEOF(env.ClientID, env.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	pairSet := s.pairs[env.ClientID]
	delete(s.pairs, env.ClientID)

	pairs := make([]q4Pair, 0, len(pairSet))
	for p := range pairSet {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].SourceBank != pairs[j].SourceBank {
			return pairs[i].SourceBank < pairs[j].SourceBank
		}
		if pairs[i].SourceAccount != pairs[j].SourceAccount {
			return pairs[i].SourceAccount < pairs[j].SourceAccount
		}
		if pairs[i].DestBank != pairs[j].DestBank {
			return pairs[i].DestBank < pairs[j].DestBank
		}
		return pairs[i].DestAccount < pairs[j].DestAccount
	})

	for _, p := range pairs {
		if _, err := fmt.Fprintf(s.file, "%d,%s,%d,%s\n", p.SourceBank, p.SourceAccount, p.DestBank, p.DestAccount); err != nil {
			return strategy.EOFOutcome{}, err
		}
	}
	if _, err := fmt.Fprintf(s.file, "# EOF client=%s count=%d\n", env.ClientID, len(pairs)); err != nil {
		return strategy.EOFOutcome{}, err
	}
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (s *Q4Strategy) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}
