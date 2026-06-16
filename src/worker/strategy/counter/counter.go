package counter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const defaultMinIntermediates = 5
const queryID = 4

type accountKey struct {
	Bank    uint32
	Account string
}

type pairKey struct {
	Source accountKey
	Dest   accountKey
}

type pairState struct {
	intermediaries map[accountKey]struct{}
	emitted        bool
}

type clientState struct {
	pairs map[pairKey]*pairState
}

type pairStateJSON struct {
	Intermediaries []string `json:"intermediaries"`
	Emitted        bool     `json:"emitted"`
}

type counterCheckpoint struct {
	Pairs map[string]*pairStateJSON `json:"pairs"`
	JAC   eof.JACStateSnapshot      `json:"jac"`
}

type Counter struct {
	strategy.NoopValidator
	cfg              strategy.StrategyConfig
	nPathFinders     int
	nFinalJoiners    int
	minIntermediates int
	coordinator      *eof.JoinerAccumulateCoordinator
	state            map[inner.ClientID]*clientState
}

func New() *Counter {
	return &Counter{state: map[inner.ClientID]*clientState{}}
}

func (c *Counter) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("counter_q4 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	n, err := env.RequiredInt("N_PATH_FINDERS", true)
	if err != nil {
		return err
	}
	finalJoiners, err := env.RequiredInt("N_FINAL_JOINERS", true)
	if err != nil {
		return err
	}
	minInterm, err := env.IntWithDefault("MIN_INTERMEDIATES", defaultMinIntermediates, true)
	if err != nil {
		return err
	}
	c.cfg = cfg
	c.nPathFinders = n
	c.nFinalJoiners = finalJoiners
	c.minIntermediates = minInterm
	c.coordinator = eof.NewJoinerAccumulateCoordinator(n, 1)
	return nil
}

func (c *Counter) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.SuspiciousPathMessage {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	path, err := inner.DeserializeSuspiciousPath(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize suspicious path: %w", err)
	}

	source := accountKey{Bank: path.SourceBank, Account: path.SourceAccount}
	intermediate := accountKey{Bank: path.IntermediateBank, Account: path.IntermediateAccount}
	dest := accountKey{Bank: path.DestBank, Account: path.DestAccount}

	key := pairKey{Source: source, Dest: dest}
	state := c.stateFor(envelope.ClientID)
	ps := state.pairFor(key)

	if ps.emitted {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}
	ps.intermediaries[intermediate] = struct{}{}
	if len(ps.intermediaries) < c.minIntermediates {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}

	ps.emitted = true
	ps.intermediaries = nil

	pair := &inner.Query4Pair{
		SourceBank:    source.Bank,
		SourceAccount: source.Account,
		DestBank:      dest.Bank,
		DestAccount:   dest.Account,
	}
	body, err := inner.SerializeQuery4Pair(pair)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("serialize query4 pair: %w", err)
	}

	qid := envelope.QueryID
	if qid == 0 {
		qid = 4
	}
	return []strategy.OutputMessage{{
		OutputIndices: []int{0},
		Body:          body,
		ClientID:      envelope.ClientID,
		RoutingKey:    c.routingKeyFor(envelope.ClientID),
		BatchItemKind: inner.Query4PairItem,
		BatchQueryID:  qid,
	}}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (c *Counter) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := c.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	return strategy.EOFOutcome{
		Action:          eof.Action{Kind: eof.ActionEmitEOFs},
		ClientCompleted: true,
		EOFs: []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  c.routingKeyFor(envelope.ClientID),
			QueryID:     queryID,
		}},
	}, nil
}

func (c *Counter) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (c *Counter) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	state := c.state[clientID]
	checkPoint := counterCheckpoint{
		JAC: c.coordinator.GetClientJACState(clientID),
	}
	if state != nil {
		checkPoint.Pairs = make(map[string]*pairStateJSON, len(state.pairs))
		for k, ps := range state.pairs {
			pj := &pairStateJSON{
				Emitted:        ps.emitted,
				Intermediaries: make([]string, 0, len(ps.intermediaries)),
			}
			for acc := range ps.intermediaries {
				pj.Intermediaries = append(pj.Intermediaries, encodeAccountKey(acc))
			}
			checkPoint.Pairs[encodePairKey(k)] = pj
		}
	}
	return json.Marshal(checkPoint)
}

func (c *Counter) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint counterCheckpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	state := c.stateFor(clientID)
	state.pairs = make(map[pairKey]*pairState, len(checkPoint.Pairs))
	for ks, pj := range checkPoint.Pairs {
		k, err := decodePairKey(ks)
		if err != nil {
			return fmt.Errorf("decode pair key %q: %w", ks, err)
		}
		ps := &pairState{
			emitted:        pj.Emitted,
			intermediaries: make(map[accountKey]struct{}, len(pj.Intermediaries)),
		}
		for _, raw := range pj.Intermediaries {
			acc, err := decodeAccountKey(raw)
			if err != nil {
				return fmt.Errorf("decode intermediary %q: %w", raw, err)
			}
			ps.intermediaries[acc] = struct{}{}
		}
		state.pairs[k] = ps
	}
	c.coordinator.RestoreClientJACState(clientID, checkPoint.JAC)
	return nil
}

func (c *Counter) CleanupClient(clientID inner.ClientID) {
	delete(c.state, clientID)
	c.coordinator.CleanupClient(clientID)
}

func (c *Counter) routingKeyFor(clientID inner.ClientID) string {
	return strconv.Itoa(hashing.Shard(string(clientID), c.nFinalJoiners))
}

func (c *Counter) stateFor(clientID inner.ClientID) *clientState {
	state, ok := c.state[clientID]
	if !ok {
		state = &clientState{pairs: map[pairKey]*pairState{}}
		c.state[clientID] = state
	}
	return state
}

func (s *clientState) pairFor(key pairKey) *pairState {
	ps, ok := s.pairs[key]
	if !ok {
		ps = &pairState{intermediaries: map[accountKey]struct{}{}}
		s.pairs[key] = ps
	}
	return ps
}

func encodeAccountKey(k accountKey) string {
	return fmt.Sprintf("%d|%s", k.Bank, k.Account)
}

func decodeAccountKey(s string) (accountKey, error) {
	idx := strings.IndexByte(s, '|')
	if idx < 0 {
		return accountKey{}, fmt.Errorf("missing separator in %q", s)
	}
	bank, err := strconv.ParseUint(s[:idx], 10, 32)
	if err != nil {
		return accountKey{}, fmt.Errorf("bad bank in %q: %w", s, err)
	}
	return accountKey{Bank: uint32(bank), Account: s[idx+1:]}, nil
}

func encodePairKey(k pairKey) string {
	return fmt.Sprintf("%s→%s", encodeAccountKey(k.Source), encodeAccountKey(k.Dest))
}

func decodePairKey(s string) (pairKey, error) {
	idx := strings.Index(s, "→")
	if idx < 0 {
		return pairKey{}, fmt.Errorf("missing arrow separator in %q", s)
	}
	src, err := decodeAccountKey(s[:idx])
	if err != nil {
		return pairKey{}, fmt.Errorf("decode source in pair %q: %w", s, err)
	}
	dst, err := decodeAccountKey(s[idx+len("→"):])
	if err != nil {
		return pairKey{}, fmt.Errorf("decode dest in pair %q: %w", s, err)
	}
	return pairKey{Source: src, Dest: dst}, nil
}
