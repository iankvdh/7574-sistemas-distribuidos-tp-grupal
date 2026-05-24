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
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type FinalJoiner struct {
	cfg         strategy.StrategyConfig
	queryID     uint8
	coordinator *eof.JoinerAccumulateCoordinator
}

func New() *FinalJoiner { return &FinalJoiner{} }

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
	// One EOF goes out per client when the coordinator fires; the strategy
	// addresses it to the per-gateway output in OnUpstreamEOF.
	j.coordinator = eof.NewJoinerAccumulateCoordinator(expectedEOFs, 1)
	return nil
}

func (j *FinalJoiner) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.QueryID != 0 && envelope.QueryID != j.queryID {
		// Mismatched query — dropped silently. Useful if Q1 and Q4 ever share
		// the same `results` exchange routing key space.
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
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

func (j *FinalJoiner) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := j.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	idx, err := j.outputIndexFor(envelope.GatewayID)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}
	return strategy.EOFOutcome{
		Action: eof.Action{Kind: eof.ActionEmitEOFs},
		EOFs:   []eof.EOFEmit{{OutputIndex: idx, QueryID: j.queryID}},
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
	case 4:
		if itemKind != inner.Query4PairItem {
			return "", fmt.Errorf("Q4 expects Query4PairItem, got kind=%d", itemKind)
		}
		pair, err := inner.DeserializeQuery4Pair(payload)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d,%s,%d,%s",
			pair.SourceBank, pair.SourceAccount,
			pair.DestBank, pair.DestAccount,
		), nil
	default:
		return "", fmt.Errorf("formatter for QUERY_ID=%d not implemented", j.queryID)
	}
}
