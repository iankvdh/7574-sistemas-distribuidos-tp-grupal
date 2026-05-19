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

func TestSerializeBatchesToInnerEnvelope(t *testing.T) {
	handler := NewMessageHandler("gw-1", "client-15")

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

	txMsg, err := handler.SerializeTransactionBatchMessage(txs)
	if err != nil {
		t.Fatalf("SerializeTransactionBatchMessage returned error: %v", err)
	}
	txEnvelope, err := inner.DeserializeEnvelope(txMsg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if txEnvelope.Kind != inner.AllTransactionsBatch {
		t.Fatalf("unexpected tx kind: %d", txEnvelope.Kind)
	}
	if txEnvelope.GatewayID != "gw-1" {
		t.Fatalf("unexpected gateway id in tx envelope: %s", txEnvelope.GatewayID)
	}

	deserializedTxs, err := external.DeserializeTransactionBatchPayload(txEnvelope.Payload)
	if err != nil {
		t.Fatalf("DeserializeTransactionBatchPayload returned error: %v", err)
	}
	if len(deserializedTxs) != 1 {
		t.Fatalf("unexpected tx batch size: %d", len(deserializedTxs))
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
	if txEOFEnvelope.GatewayID != "gw-1" {
		t.Fatalf("unexpected gateway id in tx EOF envelope: %s", txEOFEnvelope.GatewayID)
	}

	accounts := []account.Account{{
		BankName:      "Bank",
		BankID:        10,
		AccountNumber: "123",
		EntityID:      "E-1",
		EntityName:    "Name",
	}}
	accMsg, err := handler.SerializeAccountBatchMessage(accounts)
	if err != nil {
		t.Fatalf("SerializeAccountBatchMessage returned error: %v", err)
	}
	accEnvelope, err := inner.DeserializeEnvelope(accMsg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if accEnvelope.Kind != inner.AllAccountsBatch {
		t.Fatalf("unexpected account kind: %d", accEnvelope.Kind)
	}
	if accEnvelope.GatewayID != "gw-1" {
		t.Fatalf("unexpected gateway id in account envelope: %s", accEnvelope.GatewayID)
	}
	if _, err := external.DeserializeAccountBatchPayload(accEnvelope.Payload); err != nil {
		t.Fatalf("DeserializeAccountBatchPayload returned error: %v", err)
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
	if accEOFEnvelope.GatewayID != "gw-1" {
		t.Fatalf("unexpected gateway id in account EOF envelope: %s", accEOFEnvelope.GatewayID)
	}
}

func TestDeserializeFinalMessage(t *testing.T) {
	message, err := inner.SerializeFinalQueryResult("gw-4", "client-4", 2, "ACK")
	if err != nil {
		t.Fatalf("SerializeFinalQueryResult returned error: %v", err)
	}

	parsed, err := DeserializeFinalMessage(message)
	if err != nil {
		t.Fatalf("DeserializeFinalMessage returned error: %v", err)
	}
	if parsed.GatewayID != "gw-4" || parsed.ClientID != "client-4" || parsed.QueryID != 2 || parsed.Status != "ACK" {
		t.Fatalf("unexpected parsed final message: %+v", parsed)
	}
}

func TestDeserializeFinalMessageUnexpectedKind(t *testing.T) {
	msg, err := inner.SerializeAllTransactionsEOF("gw-5", "client-5", 10)
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
	handler := NewMessageHandler("gw-acc", "client-acc")

	txBatch1 := []transaction.Transaction{{Date: 20220101, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "b", AmountPaid: 0.01, PaymentCurrency: "USD", PaymentFormat: "WIRE"}}
	txBatch2 := []transaction.Transaction{
		{Date: 20220102, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "c", AmountPaid: 0.02, PaymentCurrency: "USD", PaymentFormat: "ACH"},
		{Date: 20220103, FromBank: 1, FromAccount: "a", ToBank: 2, ToAccount: "d", AmountPaid: 0.03, PaymentCurrency: "USD", PaymentFormat: "ACH"},
	}

	if _, err := handler.SerializeTransactionBatchMessage(txBatch1); err != nil {
		t.Fatalf("SerializeTransactionBatchMessage batch1 returned error: %v", err)
	}
	if _, err := handler.SerializeTransactionBatchMessage(txBatch2); err != nil {
		t.Fatalf("SerializeTransactionBatchMessage batch2 returned error: %v", err)
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

	if _, err := handler.SerializeAccountBatchMessage(accBatch1); err != nil {
		t.Fatalf("SerializeAccountBatchMessage batch1 returned error: %v", err)
	}
	if _, err := handler.SerializeAccountBatchMessage(accBatch2); err != nil {
		t.Fatalf("SerializeAccountBatchMessage batch2 returned error: %v", err)
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
