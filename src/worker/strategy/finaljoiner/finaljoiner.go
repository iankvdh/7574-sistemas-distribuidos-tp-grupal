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

var supportedQueries = []uint8{1, 2, 4}

type q4Pair struct {
	SourceBank    uint32
	SourceAccount string
	DestBank      uint32
	DestAccount   string
}

type clientQ4State struct {
	gatewayID inner.GatewayID
	pairs     map[q4Pair]struct{}
}

type FinalJoiner struct {
	cfg          strategy.StrategyConfig
	coordinators map[uint8]*eof.JoinerAccumulateCoordinator
	q4State      map[inner.ClientID]*clientQ4State
}

func New() *FinalJoiner {
	return &FinalJoiner{
		coordinators: map[uint8]*eof.JoinerAccumulateCoordinator{},
		q4State:      map[inner.ClientID]*clientQ4State{},
	}
}

func (j *FinalJoiner) Name() string { return "final_joiner" }

func (j *FinalJoiner) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount < 1 {
		return fmt.Errorf("final_joiner requires at least 1 output, got %d", cfg.OutputCount)
	}
	for _, qid := range supportedQueries {
		expected, err := env.IntWithDefault(expectedEOFsEnv(qid), 0, false)
		if err != nil {
			return err
		}
		if expected <= 0 {
			continue
		}
		j.coordinators[qid] = eof.NewJoinerAccumulateCoordinator(expected, 1)
	}
	if len(j.coordinators) == 0 {
		return fmt.Errorf("final_joiner needs at least one EXPECTED_EOFS_Q<n> > 0 to be configured")
	}
	j.cfg = cfg
	return nil
}

func (j *FinalJoiner) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	qid := envelope.QueryID
	if _, ok := j.coordinators[qid]; !ok {
		// QueryID==0 or one we weren't configured for — drop silently.
		// Useful when running a single-query scenario over the shared exchange.
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	switch qid {
	case 4:
		return j.processQ4Item(envelope)
	default:
		return j.processStreamingItem(envelope)
	}
}

// processStreamingItem forwards a single item as a CSV row to the gateway's
// per-replica output. Used by queries whose rows are emitted as-they-come (Q1).
func (j *FinalJoiner) processStreamingItem(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	row, err := j.formatRow(envelope.QueryID, envelope.Kind, envelope.Payload)
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
		BatchQueryID:  envelope.QueryID,
	}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

// processQ4Item dedupes incoming (source, dest) pairs per client. Q4's counter
// emits one Query4PairItem per detected scatter pair; we buffer the distinct
// pairs here and flush them as CSV rows on EOF (see OnUpstreamEOF).
func (j *FinalJoiner) processQ4Item(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.Query4PairItem {
		return nil, strategy.LocalCounts{}, fmt.Errorf("Q4 expects Query4PairItem, got kind=%d", envelope.Kind)
	}
	pair, err := inner.DeserializeQuery4Pair(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize query4 pair: %w", err)
	}
	state := j.q4StateFor(envelope.ClientID, envelope.GatewayID)
	state.pairs[q4Pair{
		SourceBank:    pair.SourceBank,
		SourceAccount: pair.SourceAccount,
		DestBank:      pair.DestBank,
		DestAccount:   pair.DestAccount,
	}] = struct{}{}
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (j *FinalJoiner) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	qid := envelope.QueryID
	coordinator, ok := j.coordinators[qid]
	if !ok {
		// EOF for a query this FJ doesn't track — silently ignore.
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	action := coordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	idx, err := j.outputIndexFor(envelope.GatewayID)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}

	var outputs []strategy.OutputMessage
	if qid == 4 {
		outputs = j.flushQ4Outputs(envelope.ClientID, idx, qid)
	}

	return strategy.EOFOutcome{
		Action:  eof.Action{Kind: eof.ActionEmitEOFs},
		Outputs: outputs,
		EOFs:    []eof.EOFEmit{{OutputIndex: idx, QueryID: qid}},
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

func (j *FinalJoiner) formatRow(queryID uint8, itemKind inner.MsgKind, payload []byte) (string, error) {
	switch queryID {
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
	case 2:
		if itemKind != inner.Q2ResultItem {
			return "", fmt.Errorf("Q2 expects Q2ResultItem, got kind=%d", itemKind)
		}
		res, err := inner.DeserializeQ2Result(payload)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d,%s,%s,%s",
			res.BankID, res.FromAccount, res.BankName,
			strconv.FormatFloat(res.MaxAmount, 'f', 2, 64),
		), nil
	default:
		return "", fmt.Errorf("formatter for QUERY_ID=%d not implemented", queryID)
	}
}

func (j *FinalJoiner) q4StateFor(clientID inner.ClientID, gatewayID inner.GatewayID) *clientQ4State {
	state, ok := j.q4State[clientID]
	if !ok {
		state = &clientQ4State{gatewayID: gatewayID, pairs: map[q4Pair]struct{}{}}
		j.q4State[clientID] = state
	} else if gatewayID != 0 {
		state.gatewayID = gatewayID
	}
	return state
}

// flushQ4Outputs builds a sorted, deduplicated list of OutputMessages for the
// client's accumulated pairs and clears the in-memory state.
func (j *FinalJoiner) flushQ4Outputs(clientID inner.ClientID, outputIdx int, queryID uint8) []strategy.OutputMessage {
	state := j.q4State[clientID]
	if state == nil || len(state.pairs) == 0 {
		delete(j.q4State, clientID)
		return nil
	}
	pairs := make([]q4Pair, 0, len(state.pairs))
	for p := range state.pairs {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, k int) bool {
		if pairs[i].SourceBank != pairs[k].SourceBank {
			return pairs[i].SourceBank < pairs[k].SourceBank
		}
		if pairs[i].SourceAccount != pairs[k].SourceAccount {
			return pairs[i].SourceAccount < pairs[k].SourceAccount
		}
		if pairs[i].DestBank != pairs[k].DestBank {
			return pairs[i].DestBank < pairs[k].DestBank
		}
		return pairs[i].DestAccount < pairs[k].DestAccount
	})
	outputs := make([]strategy.OutputMessage, 0, len(pairs))
	for _, p := range pairs {
		outputs = append(outputs, strategy.OutputMessage{
			OutputIndices: []int{outputIdx},
			Body:          []byte(fmt.Sprintf("%d,%s,%d,%s", p.SourceBank, p.SourceAccount, p.DestBank, p.DestAccount)),
			ClientID:      clientID,
			BatchQueryID:  queryID,
		})
	}
	delete(j.q4State, clientID)
	return outputs
}

func expectedEOFsEnv(qid uint8) string {
	return fmt.Sprintf("EXPECTED_EOFS_Q%d", qid)
}
