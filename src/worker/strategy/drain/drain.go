// Package drain is a sink that counts the transactions it receives per client
// and writes a single EOF marker line per client to DRAIN_OUTPUT_FILE when the
// upstream EOF arrives. One drain per terminal queue gives an end-to-end check
// that the right number of items reaches every final destination, without
// dumping the full payload to disk.
package drain

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type Strategy struct {
	cfg   strategy.StrategyConfig
	mu    sync.Mutex
	file  *os.File
	count map[inner.ClientID]uint64
}

func New() *Strategy {
	return &Strategy{count: map[inner.ClientID]uint64{}}
}

func (s *Strategy) Init(cfg strategy.StrategyConfig) error {
	path := os.Getenv("DRAIN_OUTPUT_FILE")
	if path == "" {
		return errors.New("DRAIN_OUTPUT_FILE is required for drain strategies")
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open drain output: %w", err)
	}
	s.cfg = cfg
	s.file = f
	return nil
}

func (s *Strategy) ProcessMessage(env *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	s.mu.Lock()
	s.count[env.ClientID]++
	s.mu.Unlock()
	return nil, strategy.LocalCounts{Processed: 1}, nil
}

func (s *Strategy) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := fmt.Fprintf(
		s.file,
		"# EOF client=%s upstream_total=%d received=%d\n",
		env.ClientID, env.Total, s.count[env.ClientID],
	); err != nil {
		return strategy.EOFOutcome{}, err
	}
	delete(s.count, env.ClientID)
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (s *Strategy) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}
