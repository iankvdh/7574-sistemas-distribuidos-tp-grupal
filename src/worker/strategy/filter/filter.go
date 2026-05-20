// Package filter contains all filter strategies. A filter applies a boolean
// predicate to each transaction it consumes and routes it to either the "match"
// outputs or the "no-match" outputs (which may be empty, meaning discard).
//
// All filters share the same base struct (Filter) and differ only in the predicate
// they register. Per-client counts are kept locally; coordination across replicas
// is delegated to a ring EOF coordinator.
package filter

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

// Predicate decides whether a transaction matches the filter.
type Predicate func(transaction.Transaction) bool

// Filter is the shared implementation for every concrete filter strategy.
type Filter struct {
	name        string
	predicate   Predicate
	ctx         strategy.Context
	state       map[inner.ClientID]*filterState
	matchCount  int
	coordinator *eof.RingCoordinator
}

type filterState struct {
	matched    uint64
	notMatched uint64
}

// New builds a Filter with the given identifier and predicate.
func New(name string, predicate Predicate) *Filter {
	return &Filter{
		name:      name,
		predicate: predicate,
		state:     map[inner.ClientID]*filterState{},
	}
}

func (f *Filter) Name() string { return f.name }

func (f *Filter) Init(ctx strategy.Context) error {
	f.ctx = ctx
	f.matchCount = ctx.MatchCount
	if f.matchCount < 0 || f.matchCount > ctx.OutputCount {
		return fmt.Errorf("invalid MatchCount=%d for %d outputs", f.matchCount, ctx.OutputCount)
	}
	f.coordinator = eof.NewRingCoordinator(ctx.ReplicaID, ctx.NReplicas)
	return nil
}

// ProcessMessage parses one transaction, evaluates the predicate, and routes it.
// Matches go to outputs[0:MatchCount]; no-matches go to outputs[MatchCount:].
// If there are no no-match outputs configured, the no-match path is a discard:
// the message is dropped but the local counter still grows so the ring totals stay correct.
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

	body, err := encodeEnvelope(env)
	if err != nil {
		return nil, strategy.LocalCounts{}, err
	}

	if matched {
		state.matched++
		if f.matchCount == 0 {
			return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
		}
		return []strategy.Decision{{
			OutputIndices: rangeIndices(0, f.matchCount),
			Body:          body,
			ClientID:      env.ClientID,
		}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
	}

	state.notMatched++
	totalOutputs := f.ctx.OutputCount
	if totalOutputs == f.matchCount {
		return nil, strategy.LocalCounts{Processed: 1, NotMatched: 1}, nil
	}
	return []strategy.Decision{{
		OutputIndices: rangeIndices(f.matchCount, totalOutputs),
		Body:          body,
		ClientID:      env.ClientID,
	}}, strategy.LocalCounts{Processed: 1, NotMatched: 1}, nil
}

// OnUpstreamEOF delegates to the ring coordinator. With N=1 the coordinator will
// usually emit the EOFs immediately; with N>1 it returns a Forward action with the
// token to send around the ring.
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

// OnRingToken delegates to the ring coordinator. Non-initiator replicas forward the
// token after summing their local counts; the initiator finalizes or re-enqueues.
func (f *Filter) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	state := f.stateFor(token.ClientID)
	action, result := f.coordinator.OnRingToken(token, state.matched, state.notMatched)
	outcome := strategy.EOFOutcome{Action: action}
	if action.Kind == eof.ActionEmitEOFs && result != nil {
		outcome.EOFs = f.buildEOFEmits(result.AggMatched, result.AggNotMatched)
		delete(f.state, token.ClientID)
	}
	if action.Kind == eof.ActionReenqueueUpstreamEOF {
		delete(f.state, token.ClientID)
	}
	return outcome, nil
}

func (f *Filter) buildEOFEmits(aggMatched, aggNotMatched uint64) []eof.EOFEmit {
	totalOutputs := f.ctx.OutputCount
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

func encodeEnvelope(env *inner.Envelope) (string, error) {
	msg, err := inner.SerializeEnvelope(env.Kind, env.GatewayID, env.ClientID, env.Total, env.Payload)
	if err != nil {
		return "", err
	}
	return msg.Body, nil
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
