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

type q4Account struct {
	Bank    uint32
	Account string
}

// Q4Strategy is the terminal sink for the Q4 pipeline. It collects
// Query4AccountItem messages from all counter_q4 shards, waits for N_COUNTERS
// upstream EOFs, then sorts and writes each unique (Bank, Account) pair to
// DRAIN_OUTPUT_FILE in "bank,account" CSV format.
type Q4Strategy struct {
	cfg         strategy.StrategyConfig
	mu          sync.Mutex
	file        *os.File
	accounts    map[inner.ClientID]map[q4Account]struct{}
	coordinator *eof.JoinerAccumulateCoordinator
}

func NewQ4() *Q4Strategy {
	return &Q4Strategy{accounts: map[inner.ClientID]map[q4Account]struct{}{}}
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
	if env.Kind != inner.Query4AccountItem {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	acc, err := inner.DeserializeQuery4Account(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize query4 account: %w", err)
	}
	s.mu.Lock()
	m := s.accounts[env.ClientID]
	if m == nil {
		m = map[q4Account]struct{}{}
		s.accounts[env.ClientID] = m
	}
	m[q4Account{Bank: acc.Bank, Account: acc.Account}] = struct{}{}
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
	accSet := s.accounts[env.ClientID]
	delete(s.accounts, env.ClientID)

	accs := make([]q4Account, 0, len(accSet))
	for a := range accSet {
		accs = append(accs, a)
	}
	sort.Slice(accs, func(i, j int) bool {
		if accs[i].Bank != accs[j].Bank {
			return accs[i].Bank < accs[j].Bank
		}
		return accs[i].Account < accs[j].Account
	})

	for _, a := range accs {
		if _, err := fmt.Fprintf(s.file, "%d,%s\n", a.Bank, a.Account); err != nil {
			return strategy.EOFOutcome{}, err
		}
	}
	if _, err := fmt.Fprintf(s.file, "# EOF client=%s count=%d\n", env.ClientID, len(accs)); err != nil {
		return strategy.EOFOutcome{}, err
	}
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (s *Q4Strategy) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}
