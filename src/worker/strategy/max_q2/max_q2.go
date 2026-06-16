package max_q2

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

const queryID uint8 = 2

type partialMax struct {
	FromAccount string
	Amount      float64
}

type clientState struct {
	processed uint64
	maxes     map[uint32]*partialMax
}

type maxQ2Checkpoint struct {
	Processed uint64                 `json:"processed"`
	Maxes     map[string]*partialMax `json:"maxes"`
	Ring      eof.RingStateSnapshot  `json:"ring"`
}

type MaxQ2 struct {
	strategy.NoopValidator
	cfg             strategy.StrategyConfig
	kAggregators    int
	ringCoordinator *eof.RingCoordinator
	state           map[inner.ClientID]*clientState
	rkCache         []string
}

func New() *MaxQ2 {
	return &MaxQ2{state: map[inner.ClientID]*clientState{}}
}

func (m *MaxQ2) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("max_q2 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("max_q2 requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	k, err := env.RequiredInt("K_AGGREGATORS_Q2", true)
	if err != nil {
		return err
	}
	m.cfg = cfg
	m.kAggregators = k
	m.rkCache = make([]string, k)
	for i := range k {
		m.rkCache[i] = strconv.Itoa(i)
	}
	m.ringCoordinator = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	return nil
}

func (m *MaxQ2) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("max_q2 expects TransactionMessage, got kind=%d", envelope.Kind)
	}
	tx, err := external.DeserializeTransaction(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
	}

	st := m.stateFor(envelope.ClientID)
	st.processed++

	current, ok := st.maxes[tx.FromBank]
	if !ok || tx.AmountPaid > current.Amount {
		st.maxes[tx.FromBank] = &partialMax{FromAccount: tx.FromAccount, Amount: tx.AmountPaid}
	}
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (m *MaxQ2) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	st := m.stateFor(envelope.ClientID)
	action, _ := m.ringCoordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.processed, 0)
	return m.outcomeFor(envelope.ClientID, action), nil
}

func (m *MaxQ2) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	st := m.stateFor(token.ClientID)
	action, _ := m.ringCoordinator.OnRingToken(token, st.processed, 0)
	return m.outcomeFor(token.ClientID, action), nil
}

func (m *MaxQ2) outcomeFor(clientID inner.ClientID, action eof.Action) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		st := m.stateFor(clientID)

		bankIDs := make([]uint32, 0, len(st.maxes))
		for k := range st.maxes {
			bankIDs = append(bankIDs, k)
		}
		slices.Sort(bankIDs)

		outputs := make([]strategy.OutputMessage, 0, len(st.maxes))
		for _, bankID := range bankIDs {
			pm := st.maxes[bankID]
			body, err := inner.SerializeQ2PartialMax(&inner.Q2PartialMax{
				BankID:      bankID,
				FromAccount: pm.FromAccount,
				MaxAmount:   pm.Amount,
			})
			if err != nil {
				continue
			}
			outputs = append(outputs, strategy.OutputMessage{
				OutputIndices: []int{0},
				Body:          body,
				ClientID:      clientID,
				RoutingKey:    m.routingKeyFor(clientID, bankID),
				BatchItemKind: inner.Q2PartialMaxItem,
				BatchQueryID:  queryID,
			})
		}
		outcome.Outputs = outputs
		emits := make([]eof.EOFEmit, m.kAggregators)
		for i := range m.kAggregators {
			emits[i] = eof.EOFEmit{OutputIndex: 0, RoutingKey: m.rkCache[i], QueryID: queryID}
		}
		outcome.EOFs = emits
		outcome.ClientCompleted = true
	}
	return outcome
}

func (m *MaxQ2) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	state := m.state[clientID]
	checkPoint := maxQ2Checkpoint{}
	if state != nil {
		checkPoint.Processed = state.processed
		checkPoint.Maxes = make(map[string]*partialMax, len(state.maxes))
		for k, v := range state.maxes {
			checkPoint.Maxes[strconv.FormatUint(uint64(k), 10)] = v
		}
	}
	checkPoint.Ring, _ = m.ringCoordinator.GetClientRingState(clientID)
	return json.Marshal(checkPoint)
}

func (m *MaxQ2) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint maxQ2Checkpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	state := m.stateFor(clientID)
	state.processed = checkPoint.Processed
	state.maxes = make(map[uint32]*partialMax, len(checkPoint.Maxes))
	for k, v := range checkPoint.Maxes {
		bankID, err := strconv.ParseUint(k, 10, 32)
		if err != nil {
			return fmt.Errorf("bad bank id key %q: %w", k, err)
		}
		state.maxes[uint32(bankID)] = v
	}
	m.ringCoordinator.RestoreClientRingState(clientID, checkPoint.Ring)
	return nil
}

func (m *MaxQ2) CleanupClient(clientID inner.ClientID) {
	delete(m.state, clientID)
	m.ringCoordinator.CleanupClient(clientID)
}

func (m *MaxQ2) routingKeyFor(clientID inner.ClientID, bankID uint32) string {
	return m.rkCache[hashing.Shard(string(clientID)+"-"+strconv.FormatUint(uint64(bankID), 10), m.kAggregators)]
}

func (m *MaxQ2) stateFor(clientID inner.ClientID) *clientState {
	state, ok := m.state[clientID]
	if !ok {
		state = &clientState{maxes: map[uint32]*partialMax{}}
		m.state[clientID] = state
	}
	return state
}
