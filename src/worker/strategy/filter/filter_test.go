package filter

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/dates"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func makeStrategyConfig(matchOutputs, nomatchOutputs int) strategy.StrategyConfig {
	return strategy.StrategyConfig{
		OutputCount: matchOutputs + nomatchOutputs,
		MatchCount:  matchOutputs,
		NReplicas:   1,
	}
}

func envelopeForTransaction(t *testing.T, tx transaction.Transaction) *inner.Envelope {
	t.Helper()
	payload, err := external.SerializeTransaction(&tx)
	if err != nil {
		t.Fatalf("SerializeTransaction: %v", err)
	}
	msg, err := inner.SerializeTransactionMessage(1, "client-x", payload)
	if err != nil {
		t.Fatalf("SerializeTransactionMessage: %v", err)
	}
	env, err := inner.DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope: %v", err)
	}
	return env
}

func TestFilterMatchRoutesToMatchOutputs(t *testing.T) {
	f := New("filter_period1", func(tx transaction.Transaction) bool {
		return dates.InPeriod1(tx.Date)
	})
	if err := f.Init(makeStrategyConfig(1, 1)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tx := transaction.Transaction{Date: 20220903, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "b", AmountPaid: 12.0, PaymentCurrency: "USD", PaymentFormat: "WIRE"}
	env := envelopeForTransaction(t, tx)

	outputMessages, counts, err := f.ProcessMessage(env)
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	if len(outputMessages) != 1 || len(outputMessages[0].OutputIndices) != 1 || outputMessages[0].OutputIndices[0] != 0 {
		t.Fatalf("expected single output message to output 0 (match), got %+v", outputMessages)
	}
	if counts.Matched != 1 || counts.NotMatched != 0 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
}

func TestFilterNoMatchRoutesToNomatchOutputs(t *testing.T) {
	f := New("filter_period1", func(tx transaction.Transaction) bool {
		return dates.InPeriod1(tx.Date)
	})
	if err := f.Init(makeStrategyConfig(1, 1)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tx := transaction.Transaction{Date: 20220601, PaymentCurrency: "USD", PaymentFormat: "WIRE"}
	env := envelopeForTransaction(t, tx)

	outputMessages, counts, err := f.ProcessMessage(env)
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	if len(outputMessages) != 1 || len(outputMessages[0].OutputIndices) != 1 || outputMessages[0].OutputIndices[0] != 1 {
		t.Fatalf("expected single output message to output 1 (nomatch), got %+v", outputMessages)
	}
	if counts.NotMatched != 1 || counts.Matched != 0 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
}

func TestFilterNoMatchDiscardsWhenNoNomatchOutput(t *testing.T) {
	f := New("filter_currency_usd", func(tx transaction.Transaction) bool {
		return tx.PaymentCurrency == "US Dollar"
	})
	if err := f.Init(makeStrategyConfig(1, 0)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tx := transaction.Transaction{Date: 20220903, PaymentCurrency: "EUR"}
	env := envelopeForTransaction(t, tx)

	outputMessages, counts, err := f.ProcessMessage(env)
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	if len(outputMessages) != 0 {
		t.Fatalf("expected 0 output messages when no-match output is absent, got %+v", outputMessages)
	}
	if counts.NotMatched != 1 {
		t.Fatalf("counter should still grow: %+v", counts)
	}
}

func TestFilterEOFEmitsCountsPerOutput(t *testing.T) {
	f := New("filter_period1", func(tx transaction.Transaction) bool {
		return dates.InPeriod1(tx.Date)
	})
	if err := f.Init(makeStrategyConfig(1, 1)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	matches := []uint32{20220901, 20220903, 20220905}
	nonMatches := []uint32{20220601, 20220906}
	for _, d := range matches {
		_, _, err := f.ProcessMessage(envelopeForTransaction(t, transaction.Transaction{Date: d}))
		if err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
	}
	for _, d := range nonMatches {
		_, _, err := f.ProcessMessage(envelopeForTransaction(t, transaction.Transaction{Date: d}))
		if err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
	}

	eofMsg, err := inner.SerializeAllTransactionsEOF(1, "client-x", uint32(len(matches)+len(nonMatches)))
	if err != nil {
		t.Fatalf("SerializeAllTransactionsEOF: %v", err)
	}
	eofEnv, err := inner.DeserializeEnvelope(eofMsg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope: %v", err)
	}

	outcome, err := f.OnUpstreamEOF(eofEnv)
	if err != nil {
		t.Fatalf("OnUpstreamEOF: %v", err)
	}
	if len(outcome.EOFs) != 2 {
		t.Fatalf("expected 2 EOFs, got %d", len(outcome.EOFs))
	}
	if outcome.EOFs[0].OutputIndex != 0 || outcome.EOFs[0].Total != uint32(len(matches)) {
		t.Fatalf("match EOF wrong: %+v", outcome.EOFs[0])
	}
	if outcome.EOFs[1].OutputIndex != 1 || outcome.EOFs[1].Total != uint32(len(nonMatches)) {
		t.Fatalf("nomatch EOF wrong: %+v", outcome.EOFs[1])
	}
}
