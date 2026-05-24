package counter

import (
	"fmt"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const defaultMinIntermediates = 5

type accountKey struct {
	Bank    uint32
	Account string
}

type pairKey struct {
	Source accountKey
	Dest   accountKey
}

type pairState struct {
	intermediates map[accountKey]struct{}
	emitted       bool
}

type clientState struct {
	pairs map[pairKey]*pairState
}

type Counter struct {
	cfg              strategy.StrategyConfig
	nPathFinders     int
	nFinalJoiners    int
	minIntermediates int
	coordinator      *eof.JoinerAccumulateCoordinator
	state            map[inner.ClientID]*clientState
}

func New() *Counter {
	return &Counter{state: map[inner.ClientID]*clientState{}}
}

func (c *Counter) Name() string { return "counter_q4" }

func (c *Counter) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("counter_q4 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	n, err := env.RequiredInt("N_PATH_FINDERS", true)
	if err != nil {
		return err
	}
	finalJoiners, err := env.RequiredInt("N_FINAL_JOINERS", true)
	if err != nil {
		return err
	}
	minIntermediates, err := env.IntWithDefault("MIN_INTERMEDIATES", defaultMinIntermediates, true)
	if err != nil {
		return err
	}
	c.cfg = cfg
	c.nPathFinders = n
	c.nFinalJoiners = finalJoiners
	c.minIntermediates = minIntermediates
	c.coordinator = eof.NewJoinerAccumulateCoordinator(n, 1)
	return nil
}

func (c *Counter) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.SuspiciousPathMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("counter_q4 expects SuspiciousPathMessage, got kind=%d", envelope.Kind)
	}
	path, err := inner.DeserializeSuspiciousPath(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize suspicious path: %w", err)
	}

	key := pairKey{
		Source: accountKey{Bank: path.SourceBank, Account: path.SourceAccount},
		Dest:   accountKey{Bank: path.DestBank, Account: path.DestAccount},
	}
	mid := accountKey{Bank: path.IntermediateBank, Account: path.IntermediateAccount}

	st := c.stateFor(envelope.ClientID)
	ps := st.pairFor(key)
	if ps.emitted {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	if _, exists := ps.intermediates[mid]; exists {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	ps.intermediates[mid] = struct{}{}
	if len(ps.intermediates) < c.minIntermediates {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	ps.emitted = true
	ps.intermediates = nil

	pair := &inner.Query4Pair{
		SourceBank:    key.Source.Bank,
		SourceAccount: key.Source.Account,
		DestBank:      key.Dest.Bank,
		DestAccount:   key.Dest.Account,
	}
	body, err := inner.SerializeQuery4Pair(pair)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("serialize query4 pair: %w", err)
	}

	qid := envelope.QueryID
	if qid == 0 {
		qid = 4
	}
	return []strategy.OutputMessage{{
		OutputIndices: []int{0},
		Body:          body,
		ClientID:      envelope.ClientID,
		RoutingKey:    c.routingKeyFor(envelope.ClientID),
		BatchItemKind: inner.Query4PairItem,
		BatchQueryID:  qid,
	}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (c *Counter) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := c.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	delete(c.state, envelope.ClientID)
	return strategy.EOFOutcome{
		Action: eof.Action{Kind: eof.ActionEmitEOFs},
		EOFs: []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  c.routingKeyFor(envelope.ClientID),
		}},
	}, nil
}

func (c *Counter) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (c *Counter) routingKeyFor(clientID inner.ClientID) string {
	return strconv.Itoa(hashing.Shard(string(clientID), c.nFinalJoiners))
}

func (c *Counter) stateFor(clientID inner.ClientID) *clientState {
	st, ok := c.state[clientID]
	if !ok {
		st = &clientState{pairs: map[pairKey]*pairState{}}
		c.state[clientID] = st
	}
	return st
}

func (s *clientState) pairFor(k pairKey) *pairState {
	p, ok := s.pairs[k]
	if !ok {
		p = &pairState{intermediates: map[accountKey]struct{}{}}
		s.pairs[k] = p
	}
	return p
}
