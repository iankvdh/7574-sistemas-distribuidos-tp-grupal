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

func TestSerializeIndividualMessagesToInnerEnvelope(t *testing.T) {
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

	txMsgs, err := handler.SerializeTransactionMessages(txs)
	if err != nil {
		t.Fatalf("SerializeTransactionMessages returned error: %v", err)
	}
	if len(txMsgs) != 1 {
		t.Fatalf("expected 1 tx message, got %d", len(txMsgs))
	}
	txEnvelope, err := inner.DeserializeEnvelope(&txMsgs[0])
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if txEnvelope.Kind != inner.TransactionMessage {
		t.Fatalf("unexpected tx kind: %d", txEnvelope.Kind)
	}
	if txEnvelope.GatewayID != 1 {
		t.Fatalf("unexpected gateway id in tx envelope: %d", txEnvelope.GatewayID)
	}

	deserializedTx, err := external.DeserializeTransaction(txEnvelope.Payload)
	if err != nil {
		t.Fatalf("DeserializeTransaction returned error: %v", err)
	}
	if deserializedTx.FromAccount != "A" || deserializedTx.ToAccount != "B" {
		t.Fatalf("unexpected deserialized transaction: %+v", deserializedTx)
	}

	txEOF, err := handler.SerializeTransactionEOFMessage()
	if err != nil {
		t.Fatalf("SerializeTransactionEOFMessage returned error: %v", err)
	}
	txEOFEnvelope, err := inner.DeserializeEnvelope(txEOF)
	if err != nil {
		t.Fatalf("DeserializeEnvelope for tx EOF returned error: %v", err)
	}
	if txEOFEnvelope.Total != 1 {
		t.Fatalf("unexpected transaction total: %d", txEOFEnvelope.Total)
	}
	if txEOFEnvelope.GatewayID != 1 {
		t.Fatalf("unexpected gateway id in tx EOF envelope: %d", txEOFEnvelope.GatewayID)
	}

	accounts := []account.Account{{
		BankName:      "Bank",
		BankID:        10,
		AccountNumber: "123",
		EntityID:      "E-1",
		EntityName:    "Name",
	}}
	accMsgs, err := handler.SerializeAccountMessages(accounts)
	if err != nil {
		t.Fatalf("SerializeAccountMessages returned error: %v", err)
	}
	if len(accMsgs) != 1 {
		t.Fatalf("expected 1 account message, got %d", len(accMsgs))
	}
	accEnvelope, err := inner.DeserializeEnvelope(&accMsgs[0])
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if accEnvelope.Kind != inner.AccountMessage {
		t.Fatalf("unexpected account kind: %d", accEnvelope.Kind)
	}
	if accEnvelope.GatewayID != 1 {
		t.Fatalf("unexpected gateway id in account envelope: %d", accEnvelope.GatewayID)
	}
	if _, err := external.DeserializeAccount(accEnvelope.Payload); err != nil {
		t.Fatalf("DeserializeAccount returned error: %v", err)
	}

	accEOF, err := handler.SerializeAccountEOFMessage()
	if err != nil {
		t.Fatalf("SerializeAccountEOFMessage returned error: %v", err)
	}
	accEOFEnvelope, err := inner.DeserializeEnvelope(accEOF)
	if err != nil {
		t.Fatalf("DeserializeEnvelope for account EOF returned error: %v", err)
	}
	if accEOFEnvelope.Total != 1 {
		t.Fatalf("unexpected account total: %d", accEOFEnvelope.Total)
	}
	if accEOFEnvelope.GatewayID != 1 {
		t.Fatalf("unexpected gateway id in account EOF envelope: %d", accEOFEnvelope.GatewayID)
	}
}

func TestDeserializeFinalMessage(t *testing.T) {
	message, err := inner.SerializeFinalQueryResult(4, "client-4", 2, "ACK")
	if err != nil {
		t.Fatalf("SerializeFinalQueryResult returned error: %v", err)
	}

	parsed, err := DeserializeFinalMessage(message)
	if err != nil {
		t.Fatalf("DeserializeFinalMessage returned error: %v", err)
	}
	if parsed.GatewayID != 4 || parsed.ClientID != "client-4" || parsed.QueryID != 2 || parsed.Status != "ACK" {
		t.Fatalf("unexpected parsed final message: %+v", parsed)
	}
}

func TestDeserializeFinalMessageUnexpectedKind(t *testing.T) {
	msg, err := inner.SerializeAllTransactionsEOF(5, "client-5", 10)
	if err != nil {
		t.Fatalf("SerializeAllTransactionsEOF returned error: %v", err)
	}

	_, err = DeserializeFinalMessage(msg)
	if !errors.Is(err, inner.ErrUnexpectedKind) {
		t.Fatalf("expected ErrUnexpectedKind, got %v", err)
	}
}

func TestDeserializeFinalMessageMalformed(t *testing.T) {
	msg := &middleware.Message{Body: "invalid-json"}

	_, err := DeserializeFinalMessage(msg)
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

	if _, err := handler.SerializeTransactionMessages(txBatch1); err != nil {
		t.Fatalf("SerializeTransactionMessages batch1 returned error: %v", err)
	}
	if _, err := handler.SerializeTransactionMessages(txBatch2); err != nil {
		t.Fatalf("SerializeTransactionMessages batch2 returned error: %v", err)
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

	if _, err := handler.SerializeAccountMessages(accBatch1); err != nil {
		t.Fatalf("SerializeAccountMessages batch1 returned error: %v", err)
	}
	if _, err := handler.SerializeAccountMessages(accBatch2); err != nil {
		t.Fatalf("SerializeAccountMessages batch2 returned error: %v", err)
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
