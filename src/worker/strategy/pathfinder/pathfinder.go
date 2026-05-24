package pathfinder

import (
	"fmt"
	"strconv"

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
}

type PathFinder struct {
	cfg         strategy.StrategyConfig
	kCounters   int
	nSharders   int
	coordinator *eof.JoinerAccumulateCoordinator
	state       map[inner.ClientID]*clientState
	rkCache     []string
}

func New() *PathFinder {
	return &PathFinder{state: map[inner.ClientID]*clientState{}}
}

func (p *PathFinder) Name() string { return "path_finder_q4" }

func (p *PathFinder) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("path_finder_q4 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	k, err := env.RequiredInt("K_COUNTERS", true)
	if err != nil {
		return err
	}
	n, err := env.RequiredInt("N_SHARDERS", true)
	if err != nil {
		return err
	}
	p.cfg = cfg
	p.kCounters = k
	p.nSharders = n
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

	st := p.stateFor(envelope.ClientID)
	var suspiciousPaths []*inner.SuspiciousPath
	if stx.ShardedBySource {
		accSt := st.accountFor(source)
		if _, exists := accSt.outSet[dest]; !exists {
			accSt.outSet[dest] = struct{}{}
			for in := range accSt.inSet {
				suspiciousPaths = append(suspiciousPaths, &inner.SuspiciousPath{
					SourceBank:          in.Bank,
					SourceAccount:       in.Account,
					IntermediateBank:    source.Bank,
					IntermediateAccount: source.Account,
					DestBank:            dest.Bank,
					DestAccount:         dest.Account,
				})
			}
		}
	} else {
		accSt := st.accountFor(dest)
		if _, exists := accSt.inSet[source]; !exists {
			accSt.inSet[source] = struct{}{}
			for out := range accSt.outSet {
				suspiciousPaths = append(suspiciousPaths, &inner.SuspiciousPath{
					SourceBank:          source.Bank,
					SourceAccount:       source.Account,
					IntermediateBank:    dest.Bank,
					IntermediateAccount: dest.Account,
					DestBank:            out.Bank,
					DestAccount:         out.Account,
				})
			}
		}
	}

	if len(suspiciousPaths) == 0 {
		return nil, strategy.LocalCounts{Processed: 1}, nil
	}

	outputs := make([]strategy.OutputMessage, 0, len(suspiciousPaths))
	for _, c := range suspiciousPaths {
		body, err := inner.SerializeSuspiciousPath(c)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("serialize suspicious path: %w", err)
		}
		shard := hashing.Shard(pairKey(c.SourceBank, c.SourceAccount, c.DestBank, c.DestAccount), p.kCounters)
		outputs = append(outputs, strategy.OutputMessage{
			OutputIndices: []int{0},
			Body:          body,
			ClientID:      envelope.ClientID,
			RoutingKey:    p.rkCache[shard],
			BatchItemKind: inner.SuspiciousPathMessage,
			BatchQueryID:  queryID,
		})
	}
	return outputs, strategy.LocalCounts{Processed: 1}, nil
}

func (p *PathFinder) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := p.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	delete(p.state, envelope.ClientID)
	return strategy.EOFOutcome{
		Action: eof.Action{Kind: eof.ActionEmitEOFs},
		EOFs:   p.buildEOFEmits(),
	}, nil
}

func (p *PathFinder) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (p *PathFinder) buildEOFEmits() []eof.EOFEmit {
	emits := make([]eof.EOFEmit, 0, p.kCounters)
	for i := 0; i < p.kCounters; i++ {
		emits = append(emits, eof.EOFEmit{
			OutputIndex: 0,
			RoutingKey:  p.rkCache[i],
		})
	}
	return emits
}

func (p *PathFinder) stateFor(clientID inner.ClientID) *clientState {
	st, ok := p.state[clientID]
	if !ok {
		st = &clientState{accounts: map[accountKey]*accountState{}}
		p.state[clientID] = st
	}
	return st
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

func pairKey(srcBank uint32, srcAcc string, dstBank uint32, dstAcc string) string {
	return fmt.Sprintf("%d-%s-%d-%s", srcBank, srcAcc, dstBank, dstAcc)
}
