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

// Q4Strategy es un drain provisorio para el pipeline de Q4. Espera mensajes
// Query4PairItem (pares orig→dst que el Counter emite al exchange `results`)
// y los escribe a DRAIN_OUTPUT_FILE en formato CSV. Cada EOF agrega una línea
// "# EOF ..." con el conteo recibido para ese cliente.
type Q4Strategy struct {
	cfg   strategy.StrategyConfig
	mu    sync.Mutex
	file  *os.File
	count map[inner.ClientID]uint64
}

func NewQ4() *Q4Strategy {
	return &Q4Strategy{count: map[inner.ClientID]uint64{}}
}

func (s *Q4Strategy) Name() string { return "drain_q4" }

func (s *Q4Strategy) Init(cfg strategy.StrategyConfig) error {
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

func (s *Q4Strategy) ProcessMessage(env *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if env.Kind != inner.Query4PairItem {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	pair, err := inner.DeserializeQuery4Pair(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize query4 pair: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := fmt.Fprintf(
		s.file,
		"%s,%d,%s,%d,%s\n",
		env.ClientID,
		pair.SourceBank, pair.SourceAccount,
		pair.DestBank, pair.DestAccount,
	); err != nil {
		return nil, strategy.LocalCounts{}, err
	}
	s.count[env.ClientID]++
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (s *Q4Strategy) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := fmt.Fprintf(
		s.file,
		"# EOF client=%s received=%d\n",
		env.ClientID, s.count[env.ClientID],
	); err != nil {
		return strategy.EOFOutcome{}, err
	}
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (s *Q4Strategy) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}
