package aggregator_q2

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID uint8 = 2

type partialMax struct {
	fromAccount string
	amount      float64
}

type clientState struct {
	maxes     map[uint32]*partialMax
	bankNames map[uint32]string
}

type AggregatorQ2 struct {
	strategy.NoopValidator
	cfg           strategy.StrategyConfig
	nFinalJoiners int
	coordinator   *eof.JoinerAccumulateCoordinator
	state         map[inner.ClientID]*clientState
}

func New() *AggregatorQ2 {
	return &AggregatorQ2{state: map[inner.ClientID]*clientState{}}
}

func (a *AggregatorQ2) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("aggregator_q2 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	expectedEOFs, err := env.RequiredInt("EXPECTED_PARTIAL_EOFS", true)
	if err != nil {
		return err
	}
	nFinal, err := env.RequiredInt("N_FINAL_JOINERS", true)
	if err != nil {
		return err
	}
	a.cfg = cfg
	a.nFinalJoiners = nFinal
	a.coordinator = eof.NewJoinerAccumulateCoordinator(expectedEOFs, 1)
	return nil
}

func (a *AggregatorQ2) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	switch envelope.Kind {
	case inner.Q2PartialMaxItem:
		pm, err := inner.DeserializeQ2PartialMax(envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q2PartialMax: %w", err)
		}
		st := a.stateFor(envelope.ClientID)
		current, ok := st.maxes[pm.BankID]
		if !ok || pm.MaxAmount > current.amount {
			st.maxes[pm.BankID] = &partialMax{fromAccount: pm.FromAccount, amount: pm.MaxAmount}
		}
		return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
	case inner.Q2BankNameItem:
		bn, err := inner.DeserializeQ2BankName(envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q2BankName: %w", err)
		}
		st := a.stateFor(envelope.ClientID)
		st.bankNames[bn.BankID] = bn.BankName
		return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
	default:
		return nil, strategy.LocalCounts{}, fmt.Errorf("aggregator_q2 unexpected kind=%d", envelope.Kind)
	}
}

func (a *AggregatorQ2) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := a.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}

	st := a.stateFor(envelope.ClientID)
	rk := a.routingKeyFor(envelope.ClientID)
	outputs := make([]strategy.OutputMessage, 0, len(st.maxes))
	for bankID, pm := range st.maxes {
		name, ok := st.bankNames[bankID]
		if !ok {
			slog.Warn("aggregator_q2: BankName missing for bank — using fallback",
				"client_id", envelope.ClientID, "bank_id", bankID)
			name = "BANK_" + strconv.FormatUint(uint64(bankID), 10)
		}
		body, err := inner.SerializeQ2Result(&inner.Q2Result{
			BankID:      bankID,
			BankName:    name,
			FromAccount: pm.fromAccount,
			MaxAmount:   pm.amount,
		})
		if err != nil {
			continue
		}
		outputs = append(outputs, strategy.OutputMessage{
			OutputIndices: []int{0},
			Body:          body,
			ClientID:      envelope.ClientID,
			RoutingKey:    rk,
			BatchItemKind: inner.Q2ResultItem,
			BatchQueryID:  queryID,
		})
	}
	delete(a.state, envelope.ClientID)

	return strategy.EOFOutcome{
		Action:  eof.Action{Kind: eof.ActionEmitEOFs},
		Outputs: outputs,
		EOFs: []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
		}},
	}, nil
}

func (a *AggregatorQ2) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (a *AggregatorQ2) routingKeyFor(clientID inner.ClientID) string {
	return strconv.Itoa(hashing.Shard(string(clientID), a.nFinalJoiners))
}

func (a *AggregatorQ2) stateFor(clientID inner.ClientID) *clientState {
	st, ok := a.state[clientID]
	if !ok {
		st = &clientState{
			maxes:     map[uint32]*partialMax{},
			bankNames: map[uint32]string{},
		}
		a.state[clientID] = st
	}
	return st
}
