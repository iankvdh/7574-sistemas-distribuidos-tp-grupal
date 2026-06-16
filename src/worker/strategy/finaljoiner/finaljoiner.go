package finaljoiner

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type finalJoinerCheckpoint struct {
	JACs map[string]eof.JACStateSnapshot `json:"jacs"`
}

var supportedQueries = []uint8{1, 2, 3, 4, 5}

type FinalJoiner struct {
	strategy.NoopValidator
	cfg          strategy.StrategyConfig
	coordinators map[uint8]*eof.JoinerAccumulateCoordinator
}

func New() *FinalJoiner {
	return &FinalJoiner{
		coordinators: map[uint8]*eof.JoinerAccumulateCoordinator{},
	}
}

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
	if _, ok := j.coordinators[envelope.QueryID]; !ok {
		// QueryID==0 or one we weren't configured for — drop silently.
		// Useful when running a single-query scenario over the shared exchange.
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
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

func (j *FinalJoiner) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	qid := envelope.QueryID
	coordinator, ok := j.coordinators[qid]
	if !ok {
		// EOF for a query this FJ doesn't track — silently ignore.
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	action := coordinator.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	idx, err := j.outputIndexFor(envelope.GatewayID)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}

	return strategy.EOFOutcome{
		Action: eof.Action{Kind: eof.ActionEmitEOFs},
		EOFs:   []eof.EOFEmit{{OutputIndex: idx, QueryID: qid}},
	}, nil
}

func (j *FinalJoiner) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (j *FinalJoiner) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	checkPoint := finalJoinerCheckpoint{
		JACs: make(map[string]eof.JACStateSnapshot, len(j.coordinators)),
	}
	for qid, coord := range j.coordinators {
		checkPoint.JACs[fmt.Sprintf("q%d", qid)] = coord.GetClientJACState(clientID)
	}
	return json.Marshal(checkPoint)
}

func (j *FinalJoiner) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint finalJoinerCheckpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	for qid, coord := range j.coordinators {
		key := fmt.Sprintf("q%d", qid)
		if snap, ok := checkPoint.JACs[key]; ok {
			coord.RestoreClientJACState(clientID, snap)
		}
	}
	return nil
}

func (j *FinalJoiner) CleanupClient(clientID inner.ClientID) {
	for _, coord := range j.coordinators {
		coord.CleanupClient(clientID)
	}
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
	case 3:
		if itemKind != inner.Query3RowItem {
			return "", fmt.Errorf("Q3 expects Query3RowItem, got kind=%d", itemKind)
		}
		row, err := inner.DeserializeQuery3Row(payload)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d,%s,%s",
			row.SourceBank, row.SourceAccount,
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
	case 5:
		if itemKind != inner.Query5RowItem {
			return "", fmt.Errorf("Q5 expects Query5RowItem, got kind=%d", itemKind)
		}
		row, err := inner.DeserializeQuery5Row(payload)
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(row.Count, 10), nil
	default:
		return "", fmt.Errorf("formatter for QUERY_ID=%d not implemented", queryID)
	}
}

func expectedEOFsEnv(qid uint8) string {
	return fmt.Sprintf("EXPECTED_EOFS_Q%d", qid)
}
