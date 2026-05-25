package scatteragg

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

type accountKey struct {
	Bank    uint32
	Account string
}

type clientState struct {
	edges map[accountKey]map[accountKey]struct{} // source → set of dests
}

type ScatterAgg struct {
	cfg             strategy.StrategyConfig
	kPathFinders    int
	minScatterDests int
	coordinator     *eof.JoinerAccumulateCoordinator
	state           map[inner.ClientID]*clientState
	rkCache         []string
}

func New() *ScatterAgg {
	return &ScatterAgg{state: map[inner.ClientID]*clientState{}}
}

func (s *ScatterAgg) Name() string { return "scatter_agg_q4" }

func (s *ScatterAgg) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("scatter_agg_q4 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	k, err := env.RequiredInt("K_PATH_FINDERS", true)
	if err != nil {
		return err
	}
	minDest, err := env.IntWithDefault("MIN_SCATTER_DESTS", 5, true)
	if err != nil {
		return err
	}
	s.cfg = cfg
	s.kPathFinders = k
	s.minScatterDests = minDest
	s.coordinator = eof.NewJoinerAccumulateCoordinator(1, 1)

	s.rkCache = make([]string, k)
	for i := 0; i < k; i++ {
		s.rkCache[i] = strconv.Itoa(i)
	}
	return nil
}

func (s *ScatterAgg) ProcessMessage(env *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if env.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("scatter_agg_q4 expects TransactionMessage, got kind=%d", env.Kind)
	}
	tx, err := external.DeserializeTransaction(env.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
	}

	source := accountKey{Bank: tx.FromBank, Account: tx.FromAccount}
	dest := accountKey{Bank: tx.ToBank, Account: tx.ToAccount}

	st := s.stateFor(env.ClientID)
	if st.edges[source] == nil {
		st.edges[source] = map[accountKey]struct{}{}
	}
	st.edges[source][dest] = struct{}{}

	return nil, strategy.LocalCounts{Processed: 1}, nil
}

func (s *ScatterAgg) OnUpstreamEOF(env *inner.Envelope) (strategy.EOFOutcome, error) {
	action := s.coordinator.OnUpstreamEOF(env.ClientID, env.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	st := s.stateFor(env.ClientID)
	outputs := s.buildOutputs(env.ClientID, st)
	delete(s.state, env.ClientID)
	return strategy.EOFOutcome{
		Action:  eof.Action{Kind: eof.ActionEmitEOFs},
		EOFs:    s.buildEOFEmits(),
		Outputs: outputs,
	}, nil
}

func (s *ScatterAgg) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (s *ScatterAgg) buildOutputs(clientID inner.ClientID, st *clientState) []strategy.OutputMessage {
	scatterSources := make(map[accountKey]struct{})
	for source, dests := range st.edges {
		if len(dests) >= s.minScatterDests {
			scatterSources[source] = struct{}{}
		}
	}

	var outputs []strategy.OutputMessage
	for source, dests := range st.edges {
		_, isScatter := scatterSources[source]
		for dest := range dests {
			bodyBySource, err := inner.SerializeShardedTx(&inner.ShardedTx{
				FromBank: source.Bank, FromAccount: source.Account,
				ToBank: dest.Bank, ToAccount: dest.Account,
				ShardedBySource: true,
			})
			if err == nil {
				rkSource := s.rkCache[hashing.Shard(accKey(source.Bank, source.Account), s.kPathFinders)]
				outputs = append(outputs, strategy.OutputMessage{
					OutputIndices: []int{0},
					Body:          bodyBySource,
					ClientID:      clientID,
					RoutingKey:    rkSource,
					BatchItemKind: inner.ShardedTxMessage,
					BatchQueryID:  queryID,
				})
			}
			if isScatter {
				bodyByDest, err := inner.SerializeShardedTx(&inner.ShardedTx{
					FromBank: source.Bank, FromAccount: source.Account,
					ToBank: dest.Bank, ToAccount: dest.Account,
					ShardedBySource: false,
				})
				if err == nil {
					rkDest := s.rkCache[hashing.Shard(accKey(dest.Bank, dest.Account), s.kPathFinders)]
					outputs = append(outputs, strategy.OutputMessage{
						OutputIndices: []int{0},
						Body:          bodyByDest,
						ClientID:      clientID,
						RoutingKey:    rkDest,
						BatchItemKind: inner.ShardedTxMessage,
						BatchQueryID:  queryID,
					})
				}
			}
		}
	}
	return outputs
}

func (s *ScatterAgg) buildEOFEmits() []eof.EOFEmit {
	emits := make([]eof.EOFEmit, 0, s.kPathFinders)
	for i := 0; i < s.kPathFinders; i++ {
		emits = append(emits, eof.EOFEmit{
			OutputIndex: 0,
			RoutingKey:  s.rkCache[i],
		})
	}
	return emits
}

func (s *ScatterAgg) stateFor(clientID inner.ClientID) *clientState {
	st, ok := s.state[clientID]
	if !ok {
		st = &clientState{edges: map[accountKey]map[accountKey]struct{}{}}
		s.state[clientID] = st
	}
	return st
}

func accKey(bank uint32, account string) string {
	return fmt.Sprintf("%d|%s", bank, account)
}
