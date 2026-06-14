package suspicious_filter

import (
	"fmt"
	"runtime"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const (
	queryID                    uint8 = 4
	defaultSuspiciousThreshold int   = 5
)

type accountKey struct {
	Bank    uint32
	Account string
}

type accountState struct {
	outAccounts   map[accountKey]struct{}
	inAccounts    map[accountKey]struct{}
	outSuspicious bool
	inSuspicious  bool
}

type clientState struct {
	accounts map[accountKey]*accountState
}

type SuspiciousFilter struct {
	strategy.NoopValidator
	cfg          strategy.StrategyConfig
	nSharders    int
	kPathFinders int
	threshold    int
	coordinator  *eof.JoinerAccumulateCoordinator
	state        map[inner.ClientID]*clientState
	rkCache      []string
}

func New() *SuspiciousFilter {
	return &SuspiciousFilter{state: map[inner.ClientID]*clientState{}}
}

func (s *SuspiciousFilter) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("suspicious_account_filter expects exactly 1 output, got %d", cfg.OutputCount)
	}
	n, err := env.RequiredInt("N_SHARDERS", true)
	if err != nil {
		return err
	}
	k, err := env.RequiredInt("K_PATH_FINDERS", true)
	if err != nil {
		return err
	}
	threshold, err := env.IntWithDefault("SUSPICIOUS_THRESHOLD", defaultSuspiciousThreshold, true)
	if err != nil {
		return err
	}
	s.cfg = cfg
	s.nSharders = n
	s.kPathFinders = k
	s.threshold = threshold
	s.coordinator = eof.NewJoinerAccumulateCoordinator(n, 1)

	s.rkCache = make([]string, k)
	for i := 0; i < k; i++ {
		s.rkCache[i] = strconv.Itoa(i)
	}
	return nil
}

func (s *SuspiciousFilter) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.Kind != inner.ShardedTxMessage {
		return nil, strategy.LocalCounts{}, fmt.Errorf("suspicious_account_filter expects ShardedTxMessage, got kind=%d", envelope.Kind)
	}
	stx, err := inner.DeserializeShardedTx(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize sharded tx: %w", err)
	}

	source := accountKey{Bank: stx.FromBank, Account: stx.FromAccount}
	dest := accountKey{Bank: stx.ToBank, Account: stx.ToAccount}

	cst := s.stateFor(envelope.ClientID)

	var key, peer accountKey
	if stx.ShardedBySource {
		key, peer = source, dest
	} else {
		key, peer = dest, source
	}
	accSt := cst.accountFor(key)

	outputs, err := s.processSide(stx.ShardedBySource, accSt, source, dest, peer, envelope.ClientID)
	if err != nil {
		return nil, strategy.LocalCounts{}, err
	}
	return outputs, strategy.LocalCounts{Processed: 1}, nil
}

func (s *SuspiciousFilter) processSide(
	shardedBySource bool,
	accSt *accountState,
	source, dest, peer accountKey,
	clientID inner.ClientID,
) ([]strategy.OutputMessage, error) {
	suspicious := accSt.outSuspicious
	set := accSt.outAccounts
	if !shardedBySource {
		suspicious = accSt.inSuspicious
		set = accSt.inAccounts
	}

	// Si la cuenta ya fue marcada suspicious en esta dirección, reenviamos la
	// tx 1-a-1 con el flag invertido. Sin dedup: el path_finder ya dedupea.
	if suspicious {
		out, err := s.buildOutput(source, dest, !shardedBySource, clientID)
		if err != nil {
			return nil, err
		}
		return []strategy.OutputMessage{out}, nil
	}

	if _, exists := set[peer]; exists {
		return nil, nil
	}

	set[peer] = struct{}{}
	if len(set) < s.threshold {
		return nil, nil
	}

	// Umbral alcanzado: marcar suspicious y emitir todas las ShardedTx
	// acumuladas (incluyendo la que disparó el llenado) con el flag invertido.
	outputs := make([]strategy.OutputMessage, 0, s.threshold)
	for acc := range set {
		var emitSource, emitDest accountKey
		if shardedBySource {
			emitSource, emitDest = source, acc
		} else {
			emitSource, emitDest = acc, dest
		}
		out, err := s.buildOutput(emitSource, emitDest, !shardedBySource, clientID)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, out)
	}

	if shardedBySource {
		accSt.outSuspicious = true
		accSt.outAccounts = nil
	} else {
		accSt.inSuspicious = true
		accSt.inAccounts = nil
	}
	return outputs, nil
}

func (s *SuspiciousFilter) buildOutput(source, dest accountKey, outShardedBySource bool, clientID inner.ClientID) (strategy.OutputMessage, error) {
	stx := &inner.ShardedTx{
		FromBank:        source.Bank,
		FromAccount:     source.Account,
		ToBank:          dest.Bank,
		ToAccount:       dest.Account,
		ShardedBySource: outShardedBySource,
	}
	body, err := inner.SerializeShardedTx(stx)
	if err != nil {
		return strategy.OutputMessage{}, fmt.Errorf("serialize sharded tx: %w", err)
	}
	rkAccount := dest
	if outShardedBySource {
		rkAccount = source
	}
	shard := hashing.Shard(accountKeyString(rkAccount.Bank, rkAccount.Account), s.kPathFinders)
	return strategy.OutputMessage{
		OutputIndices: []int{0},
		Body:          body,
		ClientID:      clientID,
		RoutingKey:    s.rkCache[shard],
		BatchItemKind: inner.ShardedTxMessage,
		BatchQueryID:  queryID,
	}, nil
}

func (s *SuspiciousFilter) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	action := s.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		return strategy.EOFOutcome{Action: action}, nil
	}
	delete(s.state, envelope.ClientID)
	runtime.GC()
	return strategy.EOFOutcome{
		Action: eof.Action{Kind: eof.ActionEmitEOFs},
		EOFs:   s.buildEOFEmits(),
	}, nil
}

func (s *SuspiciousFilter) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

func (s *SuspiciousFilter) buildEOFEmits() []eof.EOFEmit {
	emits := make([]eof.EOFEmit, 0, s.kPathFinders)
	for i := 0; i < s.kPathFinders; i++ {
		emits = append(emits, eof.EOFEmit{
			OutputIndex: 0,
			RoutingKey:  s.rkCache[i],
		})
	}
	return emits
}

func (s *SuspiciousFilter) stateFor(clientID inner.ClientID) *clientState {
	st, ok := s.state[clientID]
	if !ok {
		st = &clientState{accounts: map[accountKey]*accountState{}}
		s.state[clientID] = st
	}
	return st
}

func (c *clientState) accountFor(k accountKey) *accountState {
	a, ok := c.accounts[k]
	if !ok {
		a = &accountState{
			outAccounts: map[accountKey]struct{}{},
			inAccounts:  map[accountKey]struct{}{},
		}
		c.accounts[k] = a
	}
	return a
}

func accountKeyString(bank uint32, account string) string {
	return fmt.Sprintf("%d|%s", bank, account)
}
