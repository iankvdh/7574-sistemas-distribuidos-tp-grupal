package pathfinder

import (
	"cmp"
	"encoding/json"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID = 4

type accountKey struct {
	Bank    uint32
	Account string
}

type accountState struct {
	inSet  map[accountKey]struct{}
	outSet map[accountKey]struct{}
}

type clientState struct {
	accounts map[accountKey]*accountState
	pendingDelta *pfDelta
}

type pfEdge struct {
	M   string `json:"m"`
	P   string `json:"p"`
	Out bool   `json:"o,omitempty"`
}

type pfDelta struct {
	Edges []pfEdge `json:"e,omitempty"`
}

type accountStateJSON struct {
	InSet  []string `json:"in"`
	OutSet []string `json:"out"`
}

type pathFinderCheckpoint struct {
	Accounts map[string]*accountStateJSON `json:"accounts"`
	JAC      eof.JACStateSnapshot         `json:"jac"`
}

type PathFinder struct {
	strategy.NoopValidator
	cfg                strategy.StrategyConfig
	kCounters          int
	nSuspiciousFilters int
	coordinator        *eof.JoinerAccumulateCoordinator
	state              map[inner.ClientID]*clientState
	rkCache            []string
}

func New() *PathFinder {
	return &PathFinder{state: map[inner.ClientID]*clientState{}}
}

func (p *PathFinder) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("path_finder_q4 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	k, err := env.RequiredInt("K_COUNTERS", true)
	if err != nil {
		return err
	}
	n, err := env.RequiredInt("N_SUSPICIOUS_FILTERS", true)
	if err != nil {
		return err
	}
	p.cfg = cfg
	p.kCounters = k
	p.nSuspiciousFilters = n
	p.coordinator = eof.NewJoinerAccumulateCoordinator(n, 1)

	p.rkCache = make([]string, k)
	for i := 0; i < k; i++ {
		p.rkCache[i] = strconv.Itoa(i)
	}
	return nil
}

func (p *PathFinder) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.ShardedTxMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("path_finder_q4 expects ShardedTxMessage, got kind=%d", envelope.Kind)
	}
	stx, err := inner.DeserializeShardedTx(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize sharded tx: %w", err)
	}

	source := accountKey{Bank: stx.FromBank, Account: stx.FromAccount}
	dest := accountKey{Bank: stx.ToBank, Account: stx.ToAccount}
	state := p.stateFor(envelope.ClientID)

	if stx.ShardedBySource {
		state.addEdge(source, dest, true)
	} else {
		state.addEdge(dest, source, false)
	}
	return nil, strategy.LocalCounts{Processed: 1}, nil
}

func (s *clientState) addEdge(m, peer accountKey, out bool) {
	accSt := s.accountFor(m)
	set := accSt.inSet
	if out {
		set = accSt.outSet
	}
	if _, exists := set[peer]; exists {
		return
	}
	set[peer] = struct{}{}
	if s.pendingDelta == nil {
		s.pendingDelta = &pfDelta{}
	}
	s.pendingDelta.Edges = append(s.pendingDelta.Edges, pfEdge{
		M: encodeAccountKey(m), P: encodeAccountKey(peer), Out: out,
	})
}

func (p *PathFinder) TakeDelta(clientID inner.ClientID) []byte {
	st := p.state[clientID]
	if st == nil || st.pendingDelta == nil {
		return nil
	}
	data, err := json.Marshal(st.pendingDelta)
	st.pendingDelta = nil
	if err != nil {
		return nil
	}
	return data
}

func (p *PathFinder) ApplyDelta(clientID inner.ClientID, data []byte) error {
	var d pfDelta
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	state := p.stateFor(clientID)
	for _, e := range d.Edges {
		m, err := decodeAccountKey(e.M)
		if err != nil {
			return fmt.Errorf("decode delta M %q: %w", e.M, err)
		}
		peer, err := decodeAccountKey(e.P)
		if err != nil {
			return fmt.Errorf("decode delta P %q: %w", e.P, err)
		}
		accSt := state.accountFor(m)
		if e.Out {
			accSt.outSet[peer] = struct{}{}
		} else {
			accSt.inSet[peer] = struct{}{}
		}
	}
	return nil
}

func accountKeyCmp(a, b accountKey) int {
	if a.Bank != b.Bank {
		return cmp.Compare(a.Bank, b.Bank)
	}
	return cmp.Compare(a.Account, b.Account)
}

func sortedAccounts(set map[accountKey]struct{}) []accountKey {
	accs := make([]accountKey, 0, len(set))
	for acc := range set {
		accs = append(accs, acc)
	}
	slices.SortFunc(accs, accountKeyCmp)
	return accs
}

func (p *PathFinder) crossProductIterator(clientID inner.ClientID) iter.Seq[strategy.OutputMessage] {
	return func(yield func(strategy.OutputMessage) bool) {
		state := p.state[clientID]
		if state == nil {
			return
		}
		mids := make([]accountKey, 0, len(state.accounts))
		for m := range state.accounts {
			mids = append(mids, m)
		}
		slices.SortFunc(mids, accountKeyCmp)
		for _, m := range mids {
			accSt := state.accounts[m]
			for _, a := range sortedAccounts(accSt.inSet) {
				for _, b := range sortedAccounts(accSt.outSet) {
					if a == b {
						continue
					}
					om, ok := p.makeTriple(clientID, a, m, b)
					if !ok {
						continue
					}
					if !yield(om) {
						return
					}
				}
			}
		}
	}
}

func (p *PathFinder) makeTriple(clientID inner.ClientID, src, mid, dst accountKey) (strategy.OutputMessage, bool) {
	body, err := inner.SerializeSuspiciousPath(&inner.SuspiciousPath{
		SourceBank:          src.Bank,
		SourceAccount:       src.Account,
		IntermediateBank:    mid.Bank,
		IntermediateAccount: mid.Account,
		DestBank:            dst.Bank,
		DestAccount:         dst.Account,
	})
	if err != nil {
		return strategy.OutputMessage{}, false
	}
	shard := hashing.Shard(pairKey(src.Bank, src.Account, dst.Bank, dst.Account), p.kCounters)
	return strategy.OutputMessage{
		OutputIndices: []int{0},
		Body:          body,
		ClientID:      clientID,
		RoutingKey:    p.rkCache[shard],
		BatchItemKind: inner.SuspiciousPathMessage,
		BatchQueryID:  queryID,
	}, true
}

func (p *PathFinder) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := p.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total, envelope.LocalCount)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	return strategy.EOFOutcome{
		Action:          eof.Action{Kind: eof.ActionEmitEOFs},
		OutputsIterator: p.crossProductIterator(envelope.ClientID),
		EOFs:            p.buildEOFEmits(p.crossProductShardCounts(envelope.ClientID)),
		ClientCompleted: true,
	}, nil
}

func (p *PathFinder) crossProductShardCounts(clientID inner.ClientID) []uint32 {
	counts := make([]uint32, p.kCounters)
	state := p.state[clientID]
	if state == nil {
		return counts
	}
	for _, accSt := range state.accounts {
		for a := range accSt.inSet {
			for b := range accSt.outSet {
				if a == b {
					continue
				}
				counts[hashing.Shard(pairKey(a.Bank, a.Account, b.Bank, b.Account), p.kCounters)]++
			}
		}
	}
	return counts
}

func (p *PathFinder) OnRingToken(_ *eof.Token, _ uint64) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (p *PathFinder) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	state := p.state[clientID]
	checkPoint := pathFinderCheckpoint{
		JAC: p.coordinator.GetClientJACState(clientID),
	}
	if state != nil {
		checkPoint.Accounts = make(map[string]*accountStateJSON, len(state.accounts))
		for k, accSt := range state.accounts {
			aj := &accountStateJSON{
				InSet:  make([]string, 0, len(accSt.inSet)),
				OutSet: make([]string, 0, len(accSt.outSet)),
			}
			for acc := range accSt.inSet {
				aj.InSet = append(aj.InSet, encodeAccountKey(acc))
			}
			for acc := range accSt.outSet {
				aj.OutSet = append(aj.OutSet, encodeAccountKey(acc))
			}
			checkPoint.Accounts[encodeAccountKey(k)] = aj
		}
	}
	return json.Marshal(checkPoint)
}

func (p *PathFinder) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint pathFinderCheckpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	state := p.stateFor(clientID)
	state.accounts = make(map[accountKey]*accountState, len(checkPoint.Accounts))
	for ks, aj := range checkPoint.Accounts {
		k, err := decodeAccountKey(ks)
		if err != nil {
			return fmt.Errorf("decode account key %q: %w", ks, err)
		}
		accSt := &accountState{
			inSet:  make(map[accountKey]struct{}, len(aj.InSet)),
			outSet: make(map[accountKey]struct{}, len(aj.OutSet)),
		}
		for _, raw := range aj.InSet {
			acc, err := decodeAccountKey(raw)
			if err != nil {
				return fmt.Errorf("decode in key %q: %w", raw, err)
			}
			accSt.inSet[acc] = struct{}{}
		}
		for _, raw := range aj.OutSet {
			acc, err := decodeAccountKey(raw)
			if err != nil {
				return fmt.Errorf("decode out key %q: %w", raw, err)
			}
			accSt.outSet[acc] = struct{}{}
		}
		state.accounts[k] = accSt
	}
	p.coordinator.RestoreClientJACState(clientID, checkPoint.JAC)
	return nil
}

func (p *PathFinder) CleanupClient(clientID inner.ClientID) {
	delete(p.state, clientID)
	p.coordinator.CleanupClient(clientID)
}

func (p *PathFinder) buildEOFEmits(shardCounts []uint32) []eof.EOFEmit {
	emits := make([]eof.EOFEmit, 0, p.kCounters)
	for i := 0; i < p.kCounters; i++ {
		emits = append(emits, eof.EOFEmit{
			OutputIndex: 0,
			RoutingKey:  p.rkCache[i],
			Total:       shardCounts[i],
		})
	}
	return emits
}

func (p *PathFinder) stateFor(clientID inner.ClientID) *clientState {
	state, ok := p.state[clientID]
	if !ok {
		state = &clientState{accounts: map[accountKey]*accountState{}}
		p.state[clientID] = state
	}
	return state
}

func (s *clientState) accountFor(k accountKey) *accountState {
	a, ok := s.accounts[k]
	if !ok {
		a = &accountState{
			inSet:  map[accountKey]struct{}{},
			outSet: map[accountKey]struct{}{},
		}
		s.accounts[k] = a
	}
	return a
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

func pairKey(srcBank uint32, srcAcc string, dstBank uint32, dstAcc string) string {
	return fmt.Sprintf("%d|%s|%d|%s", srcBank, srcAcc, dstBank, dstAcc)
}
