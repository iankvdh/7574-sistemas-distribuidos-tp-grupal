// Package sharder_q1 implements the terminal node of the Query 1 pipeline.
// For each USD transaction below the configured amount threshold (already
// filtered upstream) it emits a Query1Row item to the `results` direct
// exchange, sharded by client. Each client maps to exactly one final_joiner
// shard via FNV-1a mod N_FINAL_JOINERS — so on EOF only that shard's routing
// key is emitted (unlike sharder_q4, whose payloads are spread across all
// downstream replicas).
package sharder_q1

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

const queryID uint8 = 1

type clientState struct {
	processed uint64
}

type Sharder struct {
	cfg             strategy.StrategyConfig
	nFinalJoiners   int
	ringCoordinator *eof.RingCoordinator
	accumulator     *eof.JoinerAccumulateCoordinator
	state           map[inner.ClientID]*clientState
	rkCache         []string
}

func New() *Sharder {
	return &Sharder{state: map[inner.ClientID]*clientState{}}
}

func (s *Sharder) Name() string { return "sharder_q1" }

func (s *Sharder) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("sharder_q1 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("sharder_q1 requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	n, err := env.RequiredInt("N_FINAL_JOINERS", true)
	if err != nil {
		return err
	}
	s.cfg = cfg
	s.nFinalJoiners = n
	s.rkCache = make([]string, n)
	for i := 0; i < n; i++ {
		s.rkCache[i] = strconv.Itoa(i)
	}
	if cfg.NReplicas > 1 {
		s.ringCoordinator = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	} else {
		// Single replica: filter_amount_lt_50 sends 1 EOF, we forward 1 EOF
		// to the routing key matching the client's shard.
		s.accumulator = eof.NewJoinerAccumulateCoordinator(1, 1)
	}
	return nil
}

func (s *Sharder) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.TransactionMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("sharder_q1 expects TransactionMessage, got kind=%d", envelope.Kind)
	}
	tx, err := external.DeserializeTransaction(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
	}

	row := &inner.Query1Row{
		SourceBank:    tx.FromBank,
		SourceAccount: tx.FromAccount,
		DestBank:      tx.ToBank,
		DestAccount:   tx.ToAccount,
		Amount:        tx.AmountPaid,
	}
	body, err := inner.SerializeQuery1Row(row)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("serialize query1 row: %w", err)
	}

	s.stateFor(envelope.ClientID).processed++
	return []strategy.OutputMessage{{
		OutputIndices: []int{0},
		Body:          body,
		ClientID:      envelope.ClientID,
		RoutingKey:    s.routingKeyFor(envelope.ClientID),
		BatchItemKind: inner.Query1RowItem,
		BatchQueryID:  queryID,
	}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (s *Sharder) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	if s.ringCoordinator != nil {
		st := s.stateFor(envelope.ClientID)
		action, _ := s.ringCoordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.processed, 0)
		return s.outcomeFor(envelope.ClientID, action), nil
	}
	action := s.accumulator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	return s.outcomeFor(envelope.ClientID, action), nil
}

func (s *Sharder) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	if s.ringCoordinator == nil {
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	st := s.stateFor(token.ClientID)
	action, _ := s.ringCoordinator.OnRingToken(token, st.processed, 0)
	return s.outcomeFor(token.ClientID, action), nil
}

func (s *Sharder) outcomeFor(clientID inner.ClientID, action eof.Action) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		outcome.EOFs = []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  s.routingKeyFor(clientID),
			QueryID:     queryID,
		}}
		delete(s.state, clientID)
	}
	return outcome
}

func (s *Sharder) routingKeyFor(clientID inner.ClientID) string {
	return s.rkCache[hashing.Shard(string(clientID), s.nFinalJoiners)]
}

func (s *Sharder) stateFor(clientID inner.ClientID) *clientState {
	st, ok := s.state[clientID]
	if !ok {
		st = &clientState{}
		s.state[clientID] = st
	}
	return st
}
