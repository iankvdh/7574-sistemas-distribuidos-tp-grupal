package average_global_q3

import (
	"fmt"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID uint8 = 3

type clientState struct {
	sums   map[string]float64
	counts map[string]uint64
}

type AverageGlobalQ3 struct {
	cfg         strategy.StrategyConfig
	nFilterQ3   int
	coordinator *eof.JoinerAccumulateCoordinator
	state       map[inner.ClientID]*clientState
	rkCache     []string
}

func New() *AverageGlobalQ3 {
	return &AverageGlobalQ3{state: map[inner.ClientID]*clientState{}}
}

func (a *AverageGlobalQ3) Name() string { return "average_global_q3" }

func (a *AverageGlobalQ3) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("average_global_q3 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	expectedEOFs, err := env.RequiredInt("EXPECTED_PARTIAL_EOFS", true)
	if err != nil {
		return err
	}
	n, err := env.RequiredInt("N_FILTER_Q3", true)
	if err != nil {
		return err
	}
	a.cfg = cfg
	a.nFilterQ3 = n
	a.rkCache = make([]string, n)
	for i := 0; i < n; i++ {
		a.rkCache[i] = strconv.Itoa(i)
	}
	a.coordinator = eof.NewJoinerAccumulateCoordinator(expectedEOFs, 1)
	return nil
}

func (a *AverageGlobalQ3) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.Q3PartialAvgItem {
		return nil, strategy.LocalCounts{}, fmt.Errorf("average_global_q3 expects Q3PartialAvgItem, got kind=%d", envelope.Kind)
	}
	pa, err := inner.DeserializeQ3PartialAvg(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q3PartialAvg: %w", err)
	}
	st := a.stateFor(envelope.ClientID)
	st.sums[pa.PaymentFormat] += pa.Sum
	st.counts[pa.PaymentFormat] += pa.Count
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (a *AverageGlobalQ3) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := a.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}

	st := a.stateFor(envelope.ClientID)
	outputs := make([]strategy.OutputMessage, 0, len(st.sums)*a.nFilterQ3)
	eofs := make([]eof.EOFEmit, 0, a.nFilterQ3)

	for i := 0; i < a.nFilterQ3; i++ {
		rk := a.rkCache[i]
		for format, sum := range st.sums {
			count := st.counts[format]
			if count == 0 {
				continue
			}
			avg := sum / float64(count)
			body, err := inner.SerializeQ3Average(&inner.Q3Average{
				PaymentFormat: format,
				Average:       avg,
			})
			if err != nil {
				continue
			}
			outputs = append(outputs, strategy.OutputMessage{
				OutputIndices: []int{0},
				Body:          body,
				ClientID:      envelope.ClientID,
				RoutingKey:    rk,
				BatchItemKind: inner.Q3AverageItem,
				BatchQueryID:  queryID,
			})
		}
		eofs = append(eofs, eof.EOFEmit{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
		})
	}
	delete(a.state, envelope.ClientID)

	return strategy.EOFOutcome{
		Action:  eof.Action{Kind: eof.ActionEmitEOFs},
		Outputs: outputs,
		EOFs:    eofs,
	}, nil
}

func (a *AverageGlobalQ3) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (a *AverageGlobalQ3) stateFor(clientID inner.ClientID) *clientState {
	st, ok := a.state[clientID]
	if !ok {
		st = &clientState{
			sums:   map[string]float64{},
			counts: map[string]uint64{},
		}
		a.state[clientID] = st
	}
	return st
}
