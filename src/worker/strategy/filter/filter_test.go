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

func TestProjectionDropsFieldOnMatch(t *testing.T) {
	f := New("filter_wire_ach", IsWireOrACH)
	f.WithMatchProjection(WithoutPaymentFormat)
	if err := f.Init(makeStrategyConfig(1, 0)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tx := transaction.Transaction{Date: 20220901, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "b", AmountPaid: 10.0, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"}
	env := envelopeForTransaction(t, tx)

	outputMessages, _, err := f.ProcessMessage(env)
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	if len(outputMessages) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(outputMessages))
	}
	t.Logf("payload before projection: %d bytes", len(env.Payload))
	t.Logf("payload after  projection: %d bytes", len(outputMessages[0].Body))
	result, err := external.DeserializeTransaction(outputMessages[0].Body)
	if err != nil {
		t.Fatalf("DeserializeTransaction: %v", err)
	}
	if result.PaymentFormat != "" {
		t.Fatalf("expected PaymentFormat to be dropped, got %q", result.PaymentFormat)
	}
	if result.AmountPaid != tx.AmountPaid {
		t.Fatalf("expected AmountPaid to be preserved, got %v", result.AmountPaid)
	}
}

func TestProjectionDropsFieldOnNoMatch(t *testing.T) {
	f := New("filter_period1", func(tx transaction.Transaction) bool {
		return dates.InPeriod1(tx.Date)
	})
	f.WithNoMatchProjection(WithoutPaymentFormat)
	if err := f.Init(makeStrategyConfig(1, 1)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tx := transaction.Transaction{Date: 20220601, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "b", AmountPaid: 5.0, PaymentCurrency: "US Dollar", PaymentFormat: "ACH"}
	env := envelopeForTransaction(t, tx)

	outputMessages, _, err := f.ProcessMessage(env)
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	if len(outputMessages) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(outputMessages))
	}
	t.Logf("payload before projection: %d bytes", len(env.Payload))
	t.Logf("payload after  projection: %d bytes", len(outputMessages[0].Body))
	result, err := external.DeserializeTransaction(outputMessages[0].Body)
	if err != nil {
		t.Fatalf("DeserializeTransaction: %v", err)
	}
	if result.PaymentFormat != "" {
		t.Fatalf("expected PaymentFormat to be dropped, got %q", result.PaymentFormat)
	}
	if result.Date != tx.Date {
		t.Fatalf("expected Date to be preserved, got %v", result.Date)
	}
}

func TestNoProjectionPassesThroughPayloadUnchanged(t *testing.T) {
	f := New("filter_period1", func(tx transaction.Transaction) bool {
		return dates.InPeriod1(tx.Date)
	})
	if err := f.Init(makeStrategyConfig(1, 1)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tx := transaction.Transaction{Date: 20220901, PaymentFormat: "Wire", PaymentCurrency: "US Dollar"}
	env := envelopeForTransaction(t, tx)

	outputMessages, _, err := f.ProcessMessage(env)
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	if len(outputMessages) != 1 {
		t.Fatalf("expected 1 output message, got %d", len(outputMessages))
	}
	if string(outputMessages[0].Body) != string(env.Payload) {
		t.Fatal("expected body to be identical to original payload when no projection is set")
	}
}

func TestProjectionsAreChainable(t *testing.T) {
	// First filter drops PaymentFormat on match.
	f1 := New("filter_wire_ach", IsWireOrACH)
	f1.WithMatchProjection(WithoutPaymentFormat)
	if err := f1.Init(makeStrategyConfig(1, 0)); err != nil {
		t.Fatalf("f1 Init: %v", err)
	}

	// Second filter drops PaymentCurrency on match (simulates filter_currency_usd after wire/ach).
	f2 := New("filter_currency_usd", func(tx transaction.Transaction) bool {
		return tx.PaymentCurrency == "US Dollar"
	})
	f2.WithMatchProjection(WithoutPaymentCurrency)
	if err := f2.Init(makeStrategyConfig(1, 0)); err != nil {
		t.Fatalf("f2 Init: %v", err)
	}

	tx := transaction.Transaction{Date: 20220901, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "b", AmountPaid: 10.0, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"}
	env1 := envelopeForTransaction(t, tx)

	out1, _, err := f1.ProcessMessage(env1)
	if err != nil || len(out1) != 1 {
		t.Fatalf("f1 ProcessMessage: err=%v, out=%v", err, out1)
	}

	// Feed f1 output into f2.
	msg2, err := inner.SerializeTransactionMessage(1, "client-x", out1[0].Body)
	if err != nil {
		t.Fatalf("SerializeTransactionMessage: %v", err)
	}
	env2, err := inner.DeserializeEnvelope(msg2)
	if err != nil {
		t.Fatalf("DeserializeEnvelope: %v", err)
	}

	out2, _, err := f2.ProcessMessage(env2)
	if err != nil || len(out2) != 1 {
		t.Fatalf("f2 ProcessMessage: err=%v, out=%v", err, out2)
	}

	result, err := external.DeserializeTransaction(out2[0].Body)
	if err != nil {
		t.Fatalf("DeserializeTransaction: %v", err)
	}
	if result.PaymentFormat != "" {
		t.Fatalf("expected PaymentFormat dropped by f1, got %q", result.PaymentFormat)
	}
	if result.PaymentCurrency != "" {
		t.Fatalf("expected PaymentCurrency dropped by f2, got %q", result.PaymentCurrency)
	}
	if result.AmountPaid != tx.AmountPaid {
		t.Fatalf("expected AmountPaid preserved, got %v", result.AmountPaid)
	}
}
