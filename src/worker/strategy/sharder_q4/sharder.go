package sharder_q4

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID uint8 = 4

type Sharder struct {
	strategy.NoopValidator
	cfg                strategy.StrategyConfig
	kSuspiciousFilters int
	coordinator        *eof.RingCoordinator
	state              map[inner.ClientID]*clientState
	rkCache            []string
}

type clientState struct {
	perShard map[string]uint64
}

type sharderCheckpoint struct {
	PerShard map[string]uint64     `json:"perShard,omitempty"`
	Ring     eof.RingStateSnapshot `json:"ring"`
}

func New() *Sharder {
	return &Sharder{state: map[inner.ClientID]*clientState{}}
}

func (s *Sharder) Init(cfg strategy.StrategyConfig) error {
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("sharder_q4 requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	if cfg.OutputCount != 1 {
		return fmt.Errorf("sharder_q4 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	k, err := env.RequiredInt("K_SUSPICIOUS_FILTERS", true)
	if err != nil {
		return err
	}
	s.cfg = cfg
	s.kSuspiciousFilters = k
	s.coordinator = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)

	s.rkCache = make([]string, k)
	for i := 0; i < k; i++ {
		s.rkCache[i] = strconv.Itoa(i)
	}
	return nil
}

func (s *Sharder) ProcessMessage(env *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("sharder_q4 expects TransactionMessage, got kind=%d", env.Kind)
	}
	tx, _, err := external.DeserializeTransaction(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
	}

	st := s.stateFor(env.ClientID)

	bySource := &inner.ShardedTx{
		FromBank:        tx.FromBank,
		FromAccount:     tx.FromAccount,
		ToBank:          tx.ToBank,
		ToAccount:       tx.ToAccount,
		ShardedBySource: true,
	}
	byDest := &inner.ShardedTx{
		FromBank:        tx.FromBank,
		FromAccount:     tx.FromAccount,
		ToBank:          tx.ToBank,
		ToAccount:       tx.ToAccount,
		ShardedBySource: false,
	}

	bodyBySource, err := inner.SerializeShardedTx(bySource)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("serialize sharded tx (source): %w", err)
	}
	bodyByDest, err := inner.SerializeShardedTx(byDest)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("serialize sharded tx (dest): %w", err)
	}

	rkSource := s.rkCache[hashing.Shard(accountKey(tx.FromBank, tx.FromAccount), s.kSuspiciousFilters)]
	rkDest := s.rkCache[hashing.Shard(accountKey(tx.ToBank, tx.ToAccount), s.kSuspiciousFilters)]
	st.perShard[rkSource]++
	st.perShard[rkDest]++

	out := []strategy.OutputMessage{
		{
			OutputIndices: []int{0},
			Body:          bodyBySource,
			ClientID:      env.ClientID,
			RoutingKey:    rkSource,
			BatchItemKind: inner.ShardedTxMessage,
			BatchQueryID:  queryID,
		},
		{
			OutputIndices: []int{0},
			Body:          bodyByDest,
			ClientID:      env.ClientID,
			RoutingKey:    rkDest,
			BatchItemKind: inner.ShardedTxMessage,
			BatchQueryID:  queryID,
		},
	}
	return out, strategy.LocalCounts{Processed: 1}, nil
}

func (s *Sharder) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	action, _ := s.coordinator.OnUpstreamEOF(env.ClientID, env.Total, env.LocalCount, 0)
	return s.outcomeFor(env.ClientID, action), nil
}

func (s *Sharder) OnRingToken(token *eof.Token, localCount uint64) (strategy.EOFOutcome, error) {
	action, _ := s.coordinator.OnRingToken(token, localCount, 0)
	return s.outcomeFor(token.ClientID, action), nil
}

func (s *Sharder) outcomeFor(clientID inner.ClientID, action eof.Action) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		outcome.EOFs = s.buildEOFEmits(clientID)
		outcome.ClientCompleted = true
	}
	return outcome
}

func (s *Sharder) buildEOFEmits(clientID inner.ClientID) []eof.EOFEmit {
	perShard := s.stateFor(clientID).perShard
	emits := make([]eof.EOFEmit, 0, s.kSuspiciousFilters)
	for i := 0; i < s.kSuspiciousFilters; i++ {
		emits = append(emits, eof.EOFEmit{
			OutputIndex: 0,
			RoutingKey:  s.rkCache[i],
			Total:       uint32(perShard[s.rkCache[i]]),
		})
	}
	return emits
}

func (s *Sharder) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	state := s.state[clientID]
	checkPoint := sharderCheckpoint{}
	if state != nil {
		checkPoint.PerShard = state.perShard
	}
	checkPoint.Ring, _ = s.coordinator.GetClientRingState(clientID)
	return json.Marshal(checkPoint)
}

func (s *Sharder) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint sharderCheckpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	state := s.stateFor(clientID)
	if checkPoint.PerShard != nil {
		state.perShard = checkPoint.PerShard
	}
	s.coordinator.RestoreClientRingState(clientID, checkPoint.Ring)
	return nil
}

func (s *Sharder) CleanupClient(clientID inner.ClientID) {
	delete(s.state, clientID)
	s.coordinator.CleanupClient(clientID)
}

func (s *Sharder) stateFor(clientID inner.ClientID) *clientState {
	st, ok := s.state[clientID]
	if !ok {
		st = &clientState{perShard: map[string]uint64{}}
		s.state[clientID] = st
	}
	if st.perShard == nil {
		st.perShard = map[string]uint64{}
	}
	return st
}

func accountKey(bank uint32, account string) string {
	return fmt.Sprintf("%d|%s", bank, account)
}
