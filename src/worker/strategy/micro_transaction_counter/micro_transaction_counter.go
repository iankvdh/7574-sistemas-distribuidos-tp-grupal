package micro_transaction_counter

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const (
	queryID             uint8 = 5
	inputIndexConv      int   = 0
	inputIndexTxs       int   = 1
	defaultMaxAmountUSD       = "1"
	usdCurrencyName           = "US Dollar"
)

var currencyNameToISO = map[string]string{
	"US Dollar":         "USD",
	"Euro":              "EUR",
	"UK Pound":          "GBP",
	"Yen":               "JPY",
	"Yuan":              "CNY",
	"Swiss Franc":       "CHF",
	"Canadian Dollar":   "CAD",
	"Australian Dollar": "AUD",
	"Brazil Real":       "BRL",
	"Mexican Peso":      "MXN",
	"Rupee":             "INR",
	"Shekel":            "ILS",
	"Ruble":             "RUB",
	"Saudi Riyal":       "SAR",
}

type clientState struct {
	count uint64
	seen  uint64
}

type MicroTransactionCounter struct {
	cfg             strategy.StrategyConfig
	rates           map[uint32]map[string]float64
	maxConvertedUSD float64
	nAggregators    int
	state           map[inner.ClientID]*clientState
	txRing          *eof.RingCoordinator
	rkCache         []string
}

func New() *MicroTransactionCounter {
	return &MicroTransactionCounter{
		state: map[inner.ClientID]*clientState{},
	}
}

func (m *MicroTransactionCounter) DeferredInputs() []int {
	return []int{inputIndexTxs}
}

func (m *MicroTransactionCounter) Init(cfg strategy.StrategyConfig) error {
	if cfg.NumInputs != 2 {
		return fmt.Errorf("micro_transaction_counter expects exactly 2 inputs (conversions + period1_wire_ach), got %d", cfg.NumInputs)
	}
	if cfg.OutputCount != 1 {
		return fmt.Errorf("micro_transaction_counter expects exactly 1 output, got %d", cfg.OutputCount)
	}
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("micro_transaction_counter requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	maxStr := env.StringWithDefault("MAX_CONVERTED_AMOUNT_USD", defaultMaxAmountUSD)
	maxAmount, err := strconv.ParseFloat(maxStr, 64)
	if err != nil || maxAmount <= 0 {
		return fmt.Errorf("MAX_CONVERTED_AMOUNT_USD must be a positive float, got %q", maxStr)
	}
	k, err := env.RequiredInt("N_AGGREGATOR_Q5", true)
	if err != nil {
		return err
	}
	m.cfg = cfg
	m.maxConvertedUSD = maxAmount
	m.nAggregators = k
	m.rkCache = make([]string, k)
	for i := 0; i < k; i++ {
		m.rkCache[i] = strconv.Itoa(i)
	}
	m.txRing = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	return nil
}

func (m *MicroTransactionCounter) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	switch envelope.InputIndex {
	case inputIndexConv:
		if envelope.Kind != inner.Q5ConversionsItem {
			return nil, strategy.LocalCounts{}, fmt.Errorf("micro_transaction_counter conversions input: unexpected kind=%d", envelope.Kind)
		}
		conv, err := inner.DeserializeQ5Conversions(envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q5Conversions: %w", err)
		}
		m.mergeRates(conv.Rates)
		return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil

	case inputIndexTxs:
		if envelope.Kind != inner.TransactionMessage {
			return nil, strategy.LocalCounts{}, fmt.Errorf("micro_transaction_counter txs input: unexpected kind=%d", envelope.Kind)
		}
		matched, err := m.processTxPayload(envelope.ClientID, envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, err
		}
		if matched {
			return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
		}
		return nil, strategy.LocalCounts{Processed: 1, NotMatched: 1}, nil

	default:
		return nil, strategy.LocalCounts{}, fmt.Errorf("micro_transaction_counter unexpected InputIndex=%d", envelope.InputIndex)
	}
}

func (m *MicroTransactionCounter) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	if envelope.InputIndex == inputIndexConv {
		return strategy.EOFOutcome{
			Action:              eof.Action{Kind: eof.ActionNone},
			StartDeferredInputs: []int{inputIndexTxs},
		}, nil
	}

	st := m.stateFor(envelope.ClientID)
	action, _ := m.txRing.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.seen, 0)
	return m.outcomeFor(envelope.ClientID, action), nil
}

func (m *MicroTransactionCounter) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	if m.txRing == nil {
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}
	st := m.stateFor(token.ClientID)
	action, _ := m.txRing.OnRingToken(token, st.seen, 0)
	return m.outcomeFor(token.ClientID, action), nil
}

func (m *MicroTransactionCounter) outcomeFor(clientID inner.ClientID, action eof.Action) strategy.EOFOutcome {
	outcome := strategy.EOFOutcome{Action: action}
	switch action.Kind {
	case eof.ActionEmitEOFs, eof.ActionEmitEOFsAndForwardToken:
		st := m.stateFor(clientID)
		rk := m.routingKeyFor(clientID)
		var outputs []strategy.OutputMessage
		if st.count > 0 {
			body, err := inner.SerializeQ5PartialCount(&inner.Q5PartialCount{Count: st.count})
			if err == nil {
				outputs = []strategy.OutputMessage{{
					OutputIndices: []int{0},
					Body:          body,
					ClientID:      clientID,
					RoutingKey:    rk,
					BatchItemKind: inner.Q5PartialCountItem,
					BatchQueryID:  queryID,
				}}
			}
		}
		outcome.Outputs = outputs
		outcome.EOFs = []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
		}}
		delete(m.state, clientID)
	}
	return outcome
}

func (m *MicroTransactionCounter) processTxPayload(clientID inner.ClientID, payload []byte) (bool, error) {
	tx, err := external.DeserializeTransaction(payload)
	if err != nil {
		return false, fmt.Errorf("deserialize transaction: %w", err)
	}
	return m.applyTx(clientID, tx), nil
}

func (m *MicroTransactionCounter) applyTx(clientID inner.ClientID, tx *transaction.Transaction) bool {
	st := m.stateFor(clientID)
	st.seen++
	converted, ok := m.convertToUSD(tx)
	if !ok {
		return false
	}
	if converted >= m.maxConvertedUSD {
		return false
	}
	st.count++
	return true
}

func (m *MicroTransactionCounter) convertToUSD(tx *transaction.Transaction) (float64, bool) {
	if strings.EqualFold(tx.PaymentCurrency, usdCurrencyName) {
		return tx.AmountPaid, true
	}
	iso, ok := currencyNameToISO[tx.PaymentCurrency]
	if !ok {
		return 0, false
	}
	dayRates, ok := m.rates[tx.Date]
	if !ok {
		return 0, false
	}
	rate, ok := dayRates[iso]
	if !ok || rate == 0 {
		return 0, false
	}
	return tx.AmountPaid / rate, true
}

func (m *MicroTransactionCounter) mergeRates(rates map[uint32]map[string]float64) {
	if m.rates == nil {
		m.rates = make(map[uint32]map[string]float64, len(rates))
	}
	for date, perCurrency := range rates {
		if _, ok := m.rates[date]; !ok {
			m.rates[date] = make(map[string]float64, len(perCurrency))
		}
		for currency, rate := range perCurrency {
			m.rates[date][currency] = rate
		}
	}
}

func (m *MicroTransactionCounter) routingKeyFor(clientID inner.ClientID) string {
	return m.rkCache[hashing.Shard(string(clientID), m.nAggregators)]
}

func (m *MicroTransactionCounter) stateFor(clientID inner.ClientID) *clientState {
	st, ok := m.state[clientID]
	if !ok {
		st = &clientState{}
		m.state[clientID] = st
	}
	return st
}
