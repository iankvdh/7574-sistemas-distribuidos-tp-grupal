package sum_q3

import (
	"fmt"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID uint8 = 3

type partialAvg struct {
	sum   float64
	count uint64
}

type clientState struct {
	processed uint64
	partials  map[string]*partialAvg
}

type SumQ3 struct {
	strategy.NoopValidator
	cfg             strategy.StrategyConfig
	kAggregators    int
	ringCoordinator *eof.RingCoordinator
	state           map[inner.ClientID]*clientState
	rkCache         []string
}

func New() *SumQ3 {
	return &SumQ3{state: map[inner.ClientID]*clientState{}}
}

func (a *SumQ3) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("sum_q3 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("sum_q3 requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	k, err := env.RequiredInt("K_AGGREGATORS_Q3", true)
	if err != nil {
		return err
	}
	a.cfg = cfg
	a.kAggregators = k
	a.rkCache = make([]string, k)
	for i := 0; i < k; i++ {
		a.rkCache[i] = strconv.Itoa(i)
	}
	a.ringCoordinator = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	return nil
}

func (a *SumQ3) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("sum_q3 expects TransactionMessage, got kind=%d", envelope.Kind)
	}
	tx, err := external.DeserializeTransaction(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
	}

	st := a.stateFor(envelope.ClientID)
	st.processed++

	pa, ok := st.partials[tx.PaymentFormat]
	if !ok {
		pa = &partialAvg{}
		st.partials[tx.PaymentFormat] = pa
	}
	pa.sum += tx.AmountPaid
	pa.count++
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (a *SumQ3) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	st := a.stateFor(envelope.ClientID)
	action, _ := a.ringCoordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.processed, 0)
	return a.outcomeFor(envelope.ClientID, action), nil
}

func (a *SumQ3) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	if a.ringCoordinator == nil {
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	st := a.stateFor(token.ClientID)
	action, _ := a.ringCoordinator.OnRingToken(token, st.processed, 0)
	return a.outcomeFor(token.ClientID, action), nil
}

func (a *SumQ3) outcomeFor(clientID inner.ClientID, action eof.Action) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		st := a.stateFor(clientID)
		rk := a.routingKeyFor(clientID)
		outputs := make([]strategy.OutputMessage, 0, len(st.partials))
		for format, pa := range st.partials {
			body, err := inner.SerializeQ3PartialAvg(&inner.Q3PartialAvg{
				PaymentFormat: format,
				Sum:           pa.sum,
				Count:         pa.count,
			})
			if err != nil {
				continue
			}
			outputs = append(outputs, strategy.OutputMessage{
				OutputIndices: []int{0},
				Body:          body,
				ClientID:      clientID,
				RoutingKey:    rk,
				BatchItemKind: inner.Q3PartialAvgItem,
				BatchQueryID:  queryID,
			})
		}
		outcome.Outputs = outputs
		outcome.EOFs = []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
		}}
		delete(a.state, clientID)
	}
	return outcome
}

func (a *SumQ3) routingKeyFor(clientID inner.ClientID) string {
	return a.rkCache[hashing.Shard(string(clientID), a.kAggregators)]
}

func (a *SumQ3) stateFor(clientID inner.ClientID) *clientState {
	st, ok := a.state[clientID]
	if !ok {
		st = &clientState{partials: map[string]*partialAvg{}}
		a.state[clientID] = st
	}
	return st
}
