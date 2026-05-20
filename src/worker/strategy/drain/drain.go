// Package drain is a sink that writes each transaction and a per-client EOF
// marker to DRAIN_OUTPUT_FILE. One drain per terminal queue gives an
// end-to-end check that data and EOF totals reach every final destination.
package drain

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const name = "drain"

type Strategy struct {
	ctx   strategy.Context
	mu    sync.Mutex
	file  *os.File
	count map[inner.ClientID]uint64
}

func init() {
	strategy.Register(name, func() (strategy.Strategy, error) {
		return &Strategy{count: map[inner.ClientID]uint64{}}, nil
	})
}

func (s *Strategy) Name() string { return name }

func (s *Strategy) Init(ctx strategy.Context) error {
	path := os.Getenv("DRAIN_OUTPUT_FILE")
	if path == "" {
		return errors.New("DRAIN_OUTPUT_FILE is required for drain strategies")
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open drain output: %w", err)
	}
	if _, err := fmt.Fprintln(f, "client_id,date,from_acc,to_acc,amount,currency,format"); err != nil {
		_ = f.Close()
		return err
	}
	s.ctx = ctx
	s.file = f
	return nil
}

func (s *Strategy) ProcessMessage(env *inner.Envelope) ([]strategy.Decision, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	tx, err := external.DeserializeTransaction(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count[env.ClientID]++
	if _, err := fmt.Fprintf(
		s.file,
		"%s,%d,%s,%s,%.2f,%s,%s\n",
		env.ClientID, tx.Date, tx.FromAccount, tx.ToAccount, tx.AmountPaid, tx.PaymentCurrency, tx.PaymentFormat,
	); err != nil {
		return nil, strategy.LocalCounts{}, err
	}
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
