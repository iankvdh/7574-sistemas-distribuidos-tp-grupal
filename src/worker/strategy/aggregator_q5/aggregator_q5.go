package aggregator_q5

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID uint8 = 5

type aggQ5Checkpoint struct {
	Total uint64               `json:"total"`
	JAC   eof.JACStateSnapshot `json:"jac"`
}

type AggregatorQ5 struct {
	strategy.NoopValidator
	cfg           strategy.StrategyConfig
	coordinator   *eof.JoinerAccumulateCoordinator
	totals        map[inner.ClientID]uint64
	nFinalJoiners int
}

func New() *AggregatorQ5 {
	return &AggregatorQ5{totals: map[inner.ClientID]uint64{}}
}

func (a *AggregatorQ5) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("aggregator_q5 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	expected, err := env.RequiredInt("N_MICRO_TX_COUNTER", true)
	if err != nil {
		return err
	}
	nFinal, err := env.RequiredInt("N_FINAL_JOINERS", true)
	if err != nil {
		return err
	}
	a.cfg = cfg
	a.nFinalJoiners = nFinal
	a.coordinator = eof.NewJoinerAccumulateCoordinator(expected, 1)
	return nil
}

func (a *AggregatorQ5) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.Q5PartialCountItem {
		return nil, strategy.LocalCounts{}, fmt.Errorf("aggregator_q5 expects Q5PartialCountItem, got kind=%d", envelope.Kind)
	}
	partial, err := inner.DeserializeQ5PartialCount(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q5PartialCount: %w", err)
	}
	a.totals[envelope.ClientID] += partial.Count
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (a *AggregatorQ5) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := a.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total, envelope.LocalCount)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}

	total := a.totals[envelope.ClientID]
	rk := strconv.Itoa(hashing.Shard(string(envelope.ClientID), a.nFinalJoiners))
	body, err := inner.SerializeQuery5Row(&inner.Query5Row{Count: total})
	if err != nil {
		return strategy.EOFOutcome{}, fmt.Errorf("serialize Query5Row: %w", err)
	}

	return strategy.EOFOutcome{
		Action: eof.Action{Kind: eof.ActionEmitEOFs},
		Outputs: []strategy.OutputMessage{{
			OutputIndices: []int{0},
			Body:          body,
			ClientID:      envelope.ClientID,
			RoutingKey:    rk,
			BatchItemKind: inner.Query5RowItem,
			BatchQueryID:  queryID,
		}},
		EOFs: []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
		}},
		ClientCompleted: true,
	}, nil
}

func (a *AggregatorQ5) OnRingToken(_ *eof.Token, _ uint64) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (a *AggregatorQ5) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	return json.Marshal(aggQ5Checkpoint{
		Total: a.totals[clientID],
		JAC:   a.coordinator.GetClientJACState(clientID),
	})
}

func (a *AggregatorQ5) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint aggQ5Checkpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	a.totals[clientID] = checkPoint.Total
	a.coordinator.RestoreClientJACState(clientID, checkPoint.JAC)
	return nil
}

func (a *AggregatorQ5) CleanupClient(clientID inner.ClientID) {
	delete(a.totals, clientID)
	a.coordinator.CleanupClient(clientID)
}
