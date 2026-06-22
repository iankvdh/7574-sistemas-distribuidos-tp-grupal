package sharder_q1

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID uint8 = 1
const defaultAmountThreshold = 50.0

type sharderCheckpoint struct {
	Ring    eof.RingStateSnapshot `json:"ring"`
	Matched uint64                `json:"matched"`
}

type Sharder struct {
	strategy.NoopValidator
	cfg             strategy.StrategyConfig
	nFinalJoiners   int
	amountThreshold float64
	ringCoordinator *eof.RingCoordinator
	rkCache         []string
	matchedCount    map[inner.ClientID]uint64
}

func New() *Sharder {
	return &Sharder{}
}

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
	threshold := defaultAmountThreshold
	if raw := os.Getenv("AMOUNT_THRESHOLD_USD"); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || v <= 0 {
			return fmt.Errorf("invalid AMOUNT_THRESHOLD_USD=%q — expected a positive number", raw)
		}
		threshold = v
	}
	s.cfg = cfg
	s.nFinalJoiners = n
	s.amountThreshold = threshold
	s.rkCache = make([]string, n)
	for i := 0; i < n; i++ {
		s.rkCache[i] = strconv.Itoa(i)
	}
	s.ringCoordinator = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	s.matchedCount = make(map[inner.ClientID]uint64)
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

	if tx.AmountPaid >= s.amountThreshold {
		return nil, strategy.LocalCounts{Processed: 1}, nil
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

	s.matchedCount[envelope.ClientID]++
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
	localMatched := s.matchedCount[envelope.ClientID]
	localNotMatched := envelope.LocalCount - localMatched
	action, _ := s.ringCoordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total, localMatched, localNotMatched)
	return s.outcomeFor(envelope.ClientID, action, localMatched), nil
}

func (s *Sharder) OnRingToken(token *eof.Token, localCount uint64) (strategy.EOFOutcome, error) {
	if s.ringCoordinator == nil {
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	localMatched := s.matchedCount[token.ClientID]
	localNotMatched := localCount - localMatched
	action, _ := s.ringCoordinator.OnRingToken(token, localMatched, localNotMatched)
	return s.outcomeFor(token.ClientID, action, localMatched), nil
}

func (s *Sharder) outcomeFor(clientID inner.ClientID, action eof.Action, localMatched uint64) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		outcome.EOFs = []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  s.routingKeyFor(clientID),
			QueryID:     queryID,
			Total:       uint32(localMatched),
		}}
		outcome.ClientCompleted = true
	}
	return outcome
}

func (s *Sharder) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	checkPoint := sharderCheckpoint{
		Matched: s.matchedCount[clientID],
	}
	checkPoint.Ring, _ = s.ringCoordinator.GetClientRingState(clientID)
	return json.Marshal(checkPoint)
}

func (s *Sharder) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint sharderCheckpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	s.matchedCount[clientID] = checkPoint.Matched
	s.ringCoordinator.RestoreClientRingState(clientID, checkPoint.Ring)
	return nil
}

func (s *Sharder) CleanupClient(clientID inner.ClientID) {
	delete(s.matchedCount, clientID)
	s.ringCoordinator.CleanupClient(clientID)
}

func (s *Sharder) routingKeyFor(clientID inner.ClientID) string {
	return s.rkCache[hashing.Shard(string(clientID), s.nFinalJoiners)]
}
