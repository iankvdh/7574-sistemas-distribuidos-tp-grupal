// Package sharder implements the first stage of the Query 4 pipeline. For
// each transaction it emits two ShardedTx messages, one shardeada por la
// cuenta origen y otra por la cuenta destino, hacia el direct exchange que
// alimenta al Path-Finder. El sharding se hace con FNV-1a sobre la cadena
// "bank|account". EOFs entre Sharder y Path-Finder usan el modo broadcast
// del RingCoordinator: cada réplica emite un EOF a CADA réplica del
// Path-Finder cuando se cierra el anillo.
package sharder

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

const queryID uint8 = 4

type Sharder struct {
	cfg           strategy.StrategyConfig
	kPathFinders  int
	coordinator   *eof.RingCoordinator
	state         map[inner.ClientID]*clientState
	rkCache       []string
}

type clientState struct {
	processed uint64
}

func New() *Sharder {
	return &Sharder{state: map[inner.ClientID]*clientState{}}
}

func (s *Sharder) Name() string { return "sharder_q4" }

func (s *Sharder) Init(cfg strategy.StrategyConfig) error {
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("sharder_q4 requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	if cfg.OutputCount != 1 {
		return fmt.Errorf("sharder_q4 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	k, err := env.RequiredInt("K_PATH_FINDERS", true)
	if err != nil {
		return err
	}
	s.cfg = cfg
	s.kPathFinders = k
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
	tx, err := external.DeserializeTransaction(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
	}

	st := s.stateFor(env.ClientID)
	st.processed++

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

	rkSource := s.rkCache[hashing.Shard(accountKey(tx.FromBank, tx.FromAccount), s.kPathFinders)]
	rkDest := s.rkCache[hashing.Shard(accountKey(tx.ToBank, tx.ToAccount), s.kPathFinders)]

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
	st := s.stateFor(env.ClientID)
	action, _ := s.coordinator.OnUpstreamEOF(env.ClientID, env.Total, st.processed, 0)
	return s.outcomeFor(env.ClientID, action), nil
}

func (s *Sharder) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	st := s.stateFor(token.ClientID)
	action, _ := s.coordinator.OnRingToken(token, st.processed, 0)
	return s.outcomeFor(token.ClientID, action), nil
}

func (s *Sharder) outcomeFor(clientID inner.ClientID, action eof.Action) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		outcome.EOFs = s.buildEOFEmits()
		delete(s.state, clientID)
	}
	return outcome
}

func (s *Sharder) buildEOFEmits() []eof.EOFEmit {
	emits := make([]eof.EOFEmit, 0, s.kPathFinders)
	for i := 0; i < s.kPathFinders; i++ {
		emits = append(emits, eof.EOFEmit{
			OutputIndex: 0,
			RoutingKey:  s.rkCache[i],
		})
	}
	return emits
}

func (s *Sharder) stateFor(clientID inner.ClientID) *clientState {
	st, ok := s.state[clientID]
	if !ok {
		st = &clientState{}
		s.state[clientID] = st
	}
	return st
}

func accountKey(bank uint32, account string) string {
	return fmt.Sprintf("%d|%s", bank, account)
}
