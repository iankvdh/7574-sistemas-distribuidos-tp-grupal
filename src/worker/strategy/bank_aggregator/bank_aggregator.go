package bank_aggregator

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

type bankAggCheckpoint struct {
	Processed uint64                `json:"processed"`
	BankNames map[string]string     `json:"banks"`
	Ring      eof.RingStateSnapshot `json:"ring"`
}

const queryID uint8 = 2

type clientState struct {
	processed uint64
	bankNames map[uint32]string
}

type BankAggregator struct {
	strategy.NoopValidator
	cfg             strategy.StrategyConfig
	kAggregators    int
	ringCoordinator *eof.RingCoordinator
	state           map[inner.ClientID]*clientState
	rkCache         []string
}

func New() *BankAggregator {
	return &BankAggregator{state: map[inner.ClientID]*clientState{}}
}

func (b *BankAggregator) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("bank_aggregator expects exactly 1 output, got %d", cfg.OutputCount)
	}
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("bank_aggregator requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	k, err := env.RequiredInt("K_AGGREGATORS_Q2", true)
	if err != nil {
		return err
	}
	b.cfg = cfg
	b.kAggregators = k
	b.rkCache = make([]string, k)
	for i := range k {
		b.rkCache[i] = strconv.Itoa(i)
	}
	b.ringCoordinator = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	return nil
}

func (b *BankAggregator) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.AccountMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("bank_aggregator expects AccountMessage, got kind=%d", envelope.Kind)
	}
	acc, err := external.DeserializeAccount(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize account: %w", err)
	}

	st := b.stateFor(envelope.ClientID)
	st.processed++
	st.bankNames[acc.BankID] = acc.BankName
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (b *BankAggregator) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	st := b.stateFor(envelope.ClientID)
	action, _ := b.ringCoordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.processed, 0)
	return b.outcomeFor(envelope.ClientID, action), nil
}

func (b *BankAggregator) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	if b.ringCoordinator == nil {
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	st := b.stateFor(token.ClientID)
	action, _ := b.ringCoordinator.OnRingToken(token, st.processed, 0)
	return b.outcomeFor(token.ClientID, action), nil
}

func (b *BankAggregator) outcomeFor(clientID inner.ClientID, action eof.Action) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		st := b.stateFor(clientID)

		bankIDs := make([]uint32, 0, len(st.bankNames))
		for k := range st.bankNames {
			bankIDs = append(bankIDs, k)
		}
		slices.Sort(bankIDs)

		outputs := make([]strategy.OutputMessage, 0, len(st.bankNames))
		for _, bankID := range bankIDs {
			body, err := inner.SerializeQ2BankName(&inner.Q2BankName{BankID: bankID, BankName: st.bankNames[bankID]})
			if err != nil {
				continue
			}
			outputs = append(outputs, strategy.OutputMessage{
				OutputIndices: []int{0},
				Body:          body,
				ClientID:      clientID,
				RoutingKey:    b.routingKeyFor(clientID, bankID),
				BatchItemKind: inner.Q2BankNameItem,
				BatchQueryID:  queryID,
			})
		}
		outcome.Outputs = outputs
		emits := make([]eof.EOFEmit, b.kAggregators)
		for i := range b.kAggregators {
			emits[i] = eof.EOFEmit{OutputIndex: 0, RoutingKey: b.rkCache[i], QueryID: queryID}
		}
		outcome.EOFs = emits
		outcome.ClientCompleted = true
	}
	return outcome
}

func (b *BankAggregator) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	state := b.state[clientID]
	checkPoint := bankAggCheckpoint{}
	if state != nil {
		checkPoint.Processed = state.processed
		checkPoint.BankNames = make(map[string]string, len(state.bankNames))
		for k, v := range state.bankNames {
			checkPoint.BankNames[strconv.FormatUint(uint64(k), 10)] = v
		}
	}
	checkPoint.Ring, _ = b.ringCoordinator.GetClientRingState(clientID)
	return json.Marshal(checkPoint)
}

func (b *BankAggregator) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint bankAggCheckpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	state := b.stateFor(clientID)
	state.processed = checkPoint.Processed
	state.bankNames = make(map[uint32]string, len(checkPoint.BankNames))
	for k, v := range checkPoint.BankNames {
		bankID, err := strconv.ParseUint(k, 10, 32)
		if err != nil {
			return fmt.Errorf("bad bank id key %q: %w", k, err)
		}
		state.bankNames[uint32(bankID)] = v
	}
	b.ringCoordinator.RestoreClientRingState(clientID, checkPoint.Ring)
	return nil
}

func (b *BankAggregator) CleanupClient(clientID inner.ClientID) {
	delete(b.state, clientID)
	b.ringCoordinator.CleanupClient(clientID)
}

func (b *BankAggregator) routingKeyFor(clientID inner.ClientID, bankID uint32) string {
	return b.rkCache[hashing.Shard(string(clientID)+"-"+strconv.FormatUint(uint64(bankID), 10), b.kAggregators)]
}

func (b *BankAggregator) stateFor(clientID inner.ClientID) *clientState {
	state, ok := b.state[clientID]
	if !ok {
		state = &clientState{bankNames: map[uint32]string{}}
		b.state[clientID] = state
	}
	return state
}
