// Package filter contains all filter strategies. A filter routes each
// transaction to the match outputs [0, MatchCount) or the no-match outputs
// [MatchCount, OutputCount). EOF coordination across replicas is handled by a
// ring coordinator.
package filter

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type Predicate func(transaction.Transaction) bool

type Filter struct {
	name        string
	predicate   Predicate
	cfg         strategy.StrategyConfig
	state       map[inner.ClientID]*filterState
	matchCount  int
	coordinator *eof.RingCoordinator
}

type filterState struct {
	matched    uint64
	notMatched uint64
}

func New(name string, predicate Predicate) *Filter {
	return &Filter{
		name:      name,
		predicate: predicate,
		state:     map[inner.ClientID]*filterState{},
	}
}

func (f *Filter) Name() string { return f.name }

func (f *Filter) Init(cfg strategy.StrategyConfig) error {
	f.cfg = cfg
	f.matchCount = cfg.MatchCount
	if f.matchCount < 0 || f.matchCount > cfg.OutputCount {
		return fmt.Errorf("invalid MatchCount=%d for %d outputs", f.matchCount, cfg.OutputCount)
	}
	f.coordinator = eof.NewRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	return nil
}

// ProcessMessage routes the transaction to match or no-match outputs. When no
// no-match outputs are configured the message is dropped, but the local counter
// still grows so ring totals stay correct.
func (f *Filter) ProcessMessage(env *inner.Envelope) ([]strategy.Decision, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("filter %s expects TransactionMessage, got kind=%d", f.name, env.Kind)
	}
	tx, err := external.DeserializeTransaction(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
	}

	state := f.stateFor(env.ClientID)
	matched := f.predicate(*tx)
	if matched {
		state.matched++
		if f.matchCount == 0 {
			return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
		}
		return []strategy.Decision{{
			OutputIndices: rangeIndices(0, f.matchCount),
			Body:          env.Payload,
			ClientID:      env.ClientID,
		}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
	}

	state.notMatched++
	totalOutputs := f.cfg.OutputCount
	if totalOutputs == f.matchCount {
		return nil, strategy.LocalCounts{Processed: 1, NotMatched: 1}, nil
	}
	return []strategy.Decision{{
		OutputIndices: rangeIndices(f.matchCount, totalOutputs),
		Body:          env.Payload,
		ClientID:      env.ClientID,
	}}, strategy.LocalCounts{Processed: 1, NotMatched: 1}, nil
}

func (f *Filter) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	state := f.stateFor(env.ClientID)
	action, result := f.coordinator.OnUpstreamEOF(env.ClientID, env.Total, state.matched, state.notMatched)
	outcome := strategy.EOFOutcome{Action: action}
	if action.Kind == eof.ActionEmitEOFs && result != nil {
		outcome.EOFs = f.buildEOFEmits(result.AggMatched, result.AggNotMatched)
		delete(f.state, env.ClientID)
	}
	return outcome, nil
}

func (f *Filter) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	state := f.stateFor(token.ClientID)
	action, result := f.coordinator.OnRingToken(token, state.matched, state.notMatched)
	outcome := strategy.EOFOutcome{Action: action}
	if action.Kind == eof.ActionEmitEOFs && result != nil {
		outcome.EOFs = f.buildEOFEmits(result.AggMatched, result.AggNotMatched)
		delete(f.state, token.ClientID)
	}
	return outcome, nil
}

func (f *Filter) buildEOFEmits(aggMatched, aggNotMatched uint64) []eof.EOFEmit {
	totalOutputs := f.cfg.OutputCount
	emits := make([]eof.EOFEmit, 0, totalOutputs)
	for i := 0; i < f.matchCount; i++ {
		emits = append(emits, eof.EOFEmit{OutputIndex: i, Total: uint32(aggMatched)})
	}
	for i := f.matchCount; i < totalOutputs; i++ {
		emits = append(emits, eof.EOFEmit{OutputIndex: i, Total: uint32(aggNotMatched)})
	}
	return emits
}

func (f *Filter) stateFor(clientID inner.ClientID) *filterState {
	state, ok := f.state[clientID]
	if !ok {
		state = &filterState{}
		f.state[clientID] = state
	}
	return state
}

func rangeIndices(start, end int) []int {
	if end <= start {
		return nil
	}
	out := make([]int, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, i)
	}
	return out
}
