package messagehandler

import (
	"errors"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func TestSerializeTransactionBatchToInnerBatch(t *testing.T) {
	handler := NewMessageHandler(1, "client-15")

	txs := []transaction.Transaction{{
		Date:            20220912,
		FromBank:        1,
		FromAccount:     "A",
		ToBank:          2,
		ToAccount:       "B",
		AmountPaid:      12.34,
		PaymentCurrency: "USD",
		PaymentFormat:   "WIRE",
	}}

	msg, err := handler.SerializeTransactionBatch(txs)
	if err != nil {
		t.Fatalf("SerializeTransactionBatch returned error: %v", err)
	}
	if msg == nil {
		t.Fatalf("expected non-nil batch message")
	}
	envelope, err := inner.DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if envelope.Kind != inner.InnerBatch {
		t.Fatalf("unexpected envelope kind: %d", envelope.Kind)
	}
	if envelope.GatewayID != 1 || envelope.ClientID != "client-15" {
		t.Fatalf("unexpected envelope header: gw=%d client=%q", envelope.GatewayID, envelope.ClientID)
	}

	itemKind, items, err := inner.DeserializeInnerBatch(envelope)
	if err != nil {
		t.Fatalf("DeserializeInnerBatch returned error: %v", err)
	}
	if itemKind != inner.TransactionMessage {
		t.Fatalf("unexpected item kind: %d", itemKind)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	tx, err := external.DeserializeTransaction(items[0])
	if err != nil {
		t.Fatalf("DeserializeTransaction returned error: %v", err)
	}
	if tx.FromAccount != "A" || tx.ToAccount != "B" {
		t.Fatalf("unexpected deserialized transaction: %+v", tx)
	}
}

func TestSerializeAccountBatchToInnerBatch(t *testing.T) {
	handler := NewMessageHandler(2, "client-22")

	accounts := []account.Account{{
		BankName:      "Bank",
		BankID:        10,
		AccountNumber: "123",
		EntityID:      "E-1",
		EntityName:    "Name",
	}}
	msg, err := handler.SerializeAccountBatch(accounts)
	if err != nil {
		t.Fatalf("SerializeAccountBatch returned error: %v", err)
	}
	envelope, err := inner.DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if envelope.Kind != inner.InnerBatch {
		t.Fatalf("unexpected envelope kind: %d", envelope.Kind)
	}
	itemKind, items, err := inner.DeserializeInnerBatch(envelope)
	if err != nil {
		t.Fatalf("DeserializeInnerBatch returned error: %v", err)
	}
	if itemKind != inner.AccountMessage {
		t.Fatalf("unexpected item kind: %d", itemKind)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if _, err := external.DeserializeAccount(items[0]); err != nil {
		t.Fatalf("DeserializeAccount returned error: %v", err)
	}
}

func TestSerializeEmptyBatchReturnsNil(t *testing.T) {
	handler := NewMessageHandler(1, "client-x")
	if msg, err := handler.SerializeTransactionBatch(nil); err != nil || msg != nil {
		t.Fatalf("empty tx batch: msg=%v err=%v", msg, err)
	}
	if msg, err := handler.SerializeAccountBatch(nil); err != nil || msg != nil {
		t.Fatalf("empty account batch: msg=%v err=%v", msg, err)
	}
}

func TestDeserializeFinalBatch(t *testing.T) {
	input := []inner.QueryResultItem{
		{QueryID: 2, Data: "row-data"},
		{QueryID: 2, Data: "EOF"},
	}
	message, err := inner.SerializeFinalQueryResultBatch(4, "client-4", input)
	if err != nil {
		t.Fatalf("SerializeFinalQueryResultBatch returned error: %v", err)
	}

	parsed, err := DeserializeFinalBatch(message)
	if err != nil {
		t.Fatalf("DeserializeFinalBatch returned error: %v", err)
	}
	if parsed.GatewayID != 4 || parsed.ClientID != "client-4" {
		t.Fatalf("unexpected header: %+v", parsed)
	}
	if len(parsed.Items) != len(input) {
		t.Fatalf("unexpected item count: got %d want %d", len(parsed.Items), len(input))
	}
	for i, want := range input {
		if parsed.Items[i].QueryID != want.QueryID || parsed.Items[i].Data != want.Data {
			t.Fatalf("item %d mismatch: got %+v want %+v", i, parsed.Items[i], want)
		}
	}
}

func TestDeserializeFinalBatchUnexpectedKind(t *testing.T) {
	msg, err := inner.SerializeAllTransactionsEOF(5, "client-5", 10)
	if err != nil {
		t.Fatalf("SerializeAllTransactionsEOF returned error: %v", err)
	}

	_, err = DeserializeFinalBatch(msg)
	if !errors.Is(err, inner.ErrUnexpectedKind) {
		t.Fatalf("expected ErrUnexpectedKind, got %v", err)
	}
}

func TestDeserializeFinalBatchMalformed(t *testing.T) {
	msg := &middleware.Message{Body: "invalid-json"}

	_, err := DeserializeFinalBatch(msg)
	if !errors.Is(err, inner.ErrMalformedEnvelope) {
		t.Fatalf("expected ErrMalformedEnvelope, got %v", err)
	}
}

func TestEOFTotalsAccumulateAcrossBatches(t *testing.T) {
	handler := NewMessageHandler(11, "client-acc")

	txBatch1 := []transaction.Transaction{{Date: 20220101, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "b", AmountPaid: 0.01, PaymentCurrency: "USD", PaymentFormat: "WIRE"}}
	txBatch2 := []transaction.Transaction{
		{Date: 20220102, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "c", AmountPaid: 0.02, PaymentCurrency: "USD", PaymentFormat: "ACH"},
		{Date: 20220103, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "d", AmountPaid: 0.03, PaymentCurrency: "USD", PaymentFormat: "ACH"},
	}

	if _, err := handler.SerializeTransactionBatch(txBatch1); err != nil {
		t.Fatalf("SerializeTransactionBatch batch1 returned error: %v", err)
	}
	if _, err := handler.SerializeTransactionBatch(txBatch2); err != nil {
		t.Fatalf("SerializeTransactionBatch batch2 returned error: %v", err)
	}

	txEOF, err := handler.SerializeTransactionEOFMessage()
	if err != nil {
		t.Fatalf("SerializeTransactionEOFMessage returned error: %v", err)
	}
	txEnvelope, err := inner.DeserializeEnvelope(txEOF)
	if err != nil {
		t.Fatalf("DeserializeEnvelope tx eof returned error: %v", err)
	}
	if txEnvelope.Total != 3 {
		t.Fatalf("unexpected transaction total: got %d want %d", txEnvelope.Total, 3)
	}

	accBatch1 := []account.Account{{BankName: "B1", BankID: 1, AccountNumber: "1", EntityID: "E1", EntityName: "N1"}}
	accBatch2 := []account.Account{{BankName: "B2", BankID: 2, AccountNumber: "2", EntityID: "E2", EntityName: "N2"}}

	if _, err := handler.SerializeAccountBatch(accBatch1); err != nil {
		t.Fatalf("SerializeAccountBatch batch1 returned error: %v", err)
	}
	if _, err := handler.SerializeAccountBatch(accBatch2); err != nil {
		t.Fatalf("SerializeAccountBatch batch2 returned error: %v", err)
	}

	accEOF, err := handler.SerializeAccountEOFMessage()
	if err != nil {
		t.Fatalf("SerializeAccountEOFMessage returned error: %v", err)
	}
	accEnvelope, err := inner.DeserializeEnvelope(accEOF)
	if err != nil {
		t.Fatalf("DeserializeEnvelope acc eof returned error: %v", err)
	}
	if accEnvelope.Total != 2 {
		t.Fatalf("unexpected account total: got %d want %d", accEnvelope.Total, 2)
	}
}
