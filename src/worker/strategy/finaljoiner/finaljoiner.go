// Package finaljoiner is the worker strategy that closes one shard of a query
// pipeline. It consumes a single routing key on the `results` direct exchange
// (one shard per replica), accumulates the expected number of upstream EOFs
// per client, and forwards each result row — plus a final EOF marker — to the
// gateway's `final_<gatewayID>` queue.
//
// Wire shape: the runtime serializes output to `final_queue` targets as
// FinalQueryResultBatch envelopes, so this strategy only needs to emit
// per-item CSV-formatted bodies (one row per OutputMessage). The QueryID and
// gateway routing are derived from the upstream envelope.
package finaljoiner

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type q4Account struct {
	Bank    uint32
	Account string
}

type clientQ4State struct {
	gatewayID inner.GatewayID
	accounts  map[q4Account]struct{}
}

type FinalJoiner struct {
	cfg         strategy.StrategyConfig
	queryID     uint8
	coordinator *eof.JoinerAccumulateCoordinator
	// Q4 acumula cuentas únicas por cliente hasta el EOF, momento en el que
	// emite las filas ordenadas + el EOF al gateway. Para otros QueryIDs el
	// FJ es forward-only (cada item se emite al vuelo).
	q4State map[inner.ClientID]*clientQ4State
}

func New() *FinalJoiner {
	return &FinalJoiner{q4State: map[inner.ClientID]*clientQ4State{}}
}

func (j *FinalJoiner) Name() string { return "final_joiner" }

func (j *FinalJoiner) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount < 1 {
		return fmt.Errorf("final_joiner requires at least 1 output, got %d", cfg.OutputCount)
	}
	qid, err := env.RequiredInt("QUERY_ID", true)
	if err != nil {
		return err
	}
	if qid < 1 || qid > 5 {
		return fmt.Errorf("QUERY_ID must be in [1,5], got %d", qid)
	}
	expectedEOFs, err := env.RequiredInt("EXPECTED_EOFS", true)
	if err != nil {
		return err
	}
	j.cfg = cfg
	j.queryID = uint8(qid)
	j.coordinator = eof.NewJoinerAccumulateCoordinator(expectedEOFs, 1)
	return nil
}

func (j *FinalJoiner) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.QueryID != 0 && envelope.QueryID != j.queryID {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}

	switch j.queryID {
	case 4:
		return j.processQ4Item(envelope)
	default:
		return j.processStreamingItem(envelope)
	}
}

// processStreamingItem forwards a single item as a CSV row to the gateway's
// per-replica output. Used by queries whose rows are emitted as-they-come (Q1).
func (j *FinalJoiner) processStreamingItem(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	row, err := j.formatRow(envelope.Kind, envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, err
	}
	idx, err := j.outputIndexFor(envelope.GatewayID)
	if err != nil {
		return nil, strategy.LocalCounts{}, err
	}
	return []strategy.OutputMessage{{
		OutputIndices: []int{idx},
		Body:          []byte(row),
		ClientID:      envelope.ClientID,
		BatchQueryID:  j.queryID,
	}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

// processQ4Item dedupes incoming (bank, account) entries per client. Q4's
// counter emits both source and destination of each detected pair separately,
// and the spec output is the set of distinct accounts involved in scatter
// patterns. We buffer here and flush on EOF (see OnUpstreamEOF).
func (j *FinalJoiner) processQ4Item(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.Query4AccountItem {
		return nil, strategy.LocalCounts{}, fmt.Errorf("Q4 expects Query4AccountItem, got kind=%d", envelope.Kind)
	}
	acc, err := inner.DeserializeQuery4Account(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize query4 account: %w", err)
	}
	state := j.q4StateFor(envelope.ClientID, envelope.GatewayID)
	state.accounts[q4Account{Bank: acc.Bank, Account: acc.Account}] = struct{}{}
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (j *FinalJoiner) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := j.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	idx, err := j.outputIndexFor(envelope.GatewayID)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}

	var outputs []strategy.OutputMessage
	if j.queryID == 4 {
		outputs = j.flushQ4Outputs(envelope.ClientID, idx)
	}

	return strategy.EOFOutcome{
		Action:  eof.Action{Kind: eof.ActionEmitEOFs},
		Outputs: outputs,
		EOFs:    []eof.EOFEmit{{OutputIndex: idx, QueryID: j.queryID}},
	}, nil
}

func (j *FinalJoiner) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

// outputIndexFor maps the originating gateway (1-based) to an output index
// (0-based). The spec declares one final_queue:final_<i> per gateway.
func (j *FinalJoiner) outputIndexFor(gatewayID inner.GatewayID) (int, error) {
	idx := int(gatewayID) - 1
	if idx < 0 || idx >= j.cfg.OutputCount {
		return 0, fmt.Errorf("final_joiner has %d outputs but received gateway_id=%d", j.cfg.OutputCount, gatewayID)
	}
	return idx, nil
}

func (j *FinalJoiner) formatRow(itemKind inner.MsgKind, payload []byte) (string, error) {
	switch j.queryID {
	case 1:
		if itemKind != inner.Query1RowItem {
			return "", fmt.Errorf("Q1 expects Query1RowItem, got kind=%d", itemKind)
		}
		row, err := inner.DeserializeQuery1Row(payload)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d,%s,%d,%s,%s",
			row.SourceBank, row.SourceAccount,
			row.DestBank, row.DestAccount,
			strconv.FormatFloat(row.Amount, 'f', 2, 64),
		), nil
	default:
		return "", fmt.Errorf("formatter for QUERY_ID=%d not implemented", j.queryID)
	}
}

func (j *FinalJoiner) q4StateFor(clientID inner.ClientID, gatewayID inner.GatewayID) *clientQ4State {
	state, ok := j.q4State[clientID]
	if !ok {
		state = &clientQ4State{gatewayID: gatewayID, accounts: map[q4Account]struct{}{}}
		j.q4State[clientID] = state
	} else if gatewayID != 0 {
		state.gatewayID = gatewayID
	}
	return state
}

// flushQ4Outputs builds a sorted, deduplicated list of OutputMessages for the
// client's accumulated accounts and clears the in-memory state.
func (j *FinalJoiner) flushQ4Outputs(clientID inner.ClientID, outputIdx int) []strategy.OutputMessage {
	state := j.q4State[clientID]
	if state == nil || len(state.accounts) == 0 {
		delete(j.q4State, clientID)
		return nil
	}
	accs := make([]q4Account, 0, len(state.accounts))
	for a := range state.accounts {
		accs = append(accs, a)
	}
	sort.Slice(accs, func(i, k int) bool {
		if accs[i].Bank != accs[k].Bank {
			return accs[i].Bank < accs[k].Bank
		}
		return accs[i].Account < accs[k].Account
	})
	outputs := make([]strategy.OutputMessage, 0, len(accs))
	for _, a := range accs {
		outputs = append(outputs, strategy.OutputMessage{
			OutputIndices: []int{outputIdx},
			Body:          []byte(fmt.Sprintf("%d,%s", a.Bank, a.Account)),
			ClientID:      clientID,
			BatchQueryID:  j.queryID,
		})
	}
	delete(j.q4State, clientID)
	return outputs
}
