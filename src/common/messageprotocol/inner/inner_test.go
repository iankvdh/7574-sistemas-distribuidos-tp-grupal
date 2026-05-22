package inner

import (
	"errors"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
)

func TestSerializeDeserializeEnvelope(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	msg, err := SerializeEnvelope(AllTransactionsBatch, 1, "client-77", 0, payload)
	if err != nil {
		t.Fatalf("SerializeEnvelope returned error: %v", err)
	}
	envelope, err := DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}

	if envelope.Kind != AllTransactionsBatch {
		t.Fatalf("unexpected kind: got %d", envelope.Kind)
	}
	if envelope.ClientID != "client-77" {
		t.Fatalf("unexpected client id: got %s", envelope.ClientID)
	}
	if envelope.GatewayID != 1 {
		t.Fatalf("unexpected gateway id: got %d", envelope.GatewayID)
	}
	if len(envelope.Payload) != len(payload) {
		t.Fatalf("unexpected payload len: got %d expected %d", len(envelope.Payload), len(payload))
	}
	for i := range payload {
		if envelope.Payload[i] != payload[i] {
			t.Fatalf("payload mismatch at %d", i)
		}
	}
}

func TestFinalQueryResultBatchRoundTrip(t *testing.T) {
	input := []QueryResultItem{
		{QueryID: 1, Data: "2022-09-01,Alice,100.00"},
		{QueryID: 1, Data: "EOF"},
		{QueryID: 3, Data: "EOF"},
	}
	msg, err := SerializeFinalQueryResultBatch(2, "client-10", input)
	if err != nil {
		t.Fatalf("SerializeFinalQueryResultBatch returned error: %v", err)
	}

	envelope, err := DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if envelope.Kind != FinalQueryResultBatch {
		t.Fatalf("unexpected kind: got %d", envelope.Kind)
	}
	if envelope.ClientID != "client-10" {
		t.Fatalf("unexpected client id: got %s", envelope.ClientID)
	}
	if envelope.GatewayID != 2 {
		t.Fatalf("unexpected gateway id: got %d", envelope.GatewayID)
	}

	items, err := DeserializeFinalQueryResultBatch(envelope)
	if err != nil {
		t.Fatalf("DeserializeFinalQueryResultBatch returned error: %v", err)
	}
	if len(items) != len(input) {
		t.Fatalf("unexpected item count: got %d want %d", len(items), len(input))
	}
	for i, want := range input {
		if items[i].QueryID != want.QueryID {
			t.Fatalf("item %d: QueryID got %d want %d", i, items[i].QueryID, want.QueryID)
		}
		if items[i].Data != want.Data {
			t.Fatalf("item %d: Data got %q want %q", i, items[i].Data, want.Data)
		}
	}
}

func TestSerializeDeserializeEnvelopeBinaryPayload(t *testing.T) {
	// Payload intentionally includes bytes that are not valid UTF-8.
	payload := []byte{0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF}

	msg, err := SerializeAllTransactionsBatch(7, "client-bin", payload)
	if err != nil {
		t.Fatalf("SerializeAllTransactionsBatch returned error: %v", err)
	}
	envelope, err := DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}

	if envelope.Kind != AllTransactionsBatch {
		t.Fatalf("unexpected kind: got %d", envelope.Kind)
	}
	if envelope.ClientID != "client-bin" {
		t.Fatalf("unexpected client id: got %s", envelope.ClientID)
	}
	if envelope.GatewayID != 7 {
		t.Fatalf("unexpected gateway id: got %d", envelope.GatewayID)
	}
	if len(envelope.Payload) != len(payload) {
		t.Fatalf("unexpected payload len: got %d expected %d", len(envelope.Payload), len(payload))
	}
	for i := range payload {
		if envelope.Payload[i] != payload[i] {
			t.Fatalf("payload mismatch at %d", i)
		}
	}
}

func TestSerializeDeserializeEOFRoundTrip(t *testing.T) {
	msg, err := SerializeAllAccountsEOF(9, "client-eof", 123)
	if err != nil {
		t.Fatalf("SerializeAllAccountsEOF returned error: %v", err)
	}
	envelope, err := DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}

	if envelope.Kind != AllAccountsEOF {
		t.Fatalf("unexpected kind: got %d", envelope.Kind)
	}
	if envelope.ClientID != "client-eof" {
		t.Fatalf("unexpected client id: got %s", envelope.ClientID)
	}
	if envelope.GatewayID != 9 {
		t.Fatalf("unexpected gateway id: got %d", envelope.GatewayID)
	}
	if envelope.Total != 123 {
		t.Fatalf("unexpected total: got %d", envelope.Total)
	}
	if len(envelope.Payload) != 0 {
		t.Fatalf("expected empty payload, got len=%d", len(envelope.Payload))
	}
}

func TestDeserializeEnvelopeMalformedCases(t *testing.T) {
	tests := []struct {
		name string
		msg  *middleware.Message
	}{
		{name: "nil message", msg: nil},
		{name: "empty body", msg: &middleware.Message{Body: ""}},
		{name: "invalid json", msg: &middleware.Message{Body: "{not-json"}},
		{name: "missing client", msg: &middleware.Message{Body: `{"g":1,"k":1}`}},
		{name: "missing gateway", msg: &middleware.Message{Body: `{"c":"abc","k":1}`}},
		{name: "missing kind", msg: &middleware.Message{Body: `{"g":1,"c":"abc"}`}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DeserializeEnvelope(tc.msg)
			if !errors.Is(err, ErrMalformedEnvelope) {
				t.Fatalf("expected ErrMalformedEnvelope, got %v", err)
			}
		})
	}
}

func TestSerializeDeserializeInnerBatch(t *testing.T) {
	items := [][]byte{
		[]byte("first"),
		[]byte("second-longer"),
		{},
		[]byte("\x00\x01\x02\x03"),
	}
	msg, err := SerializeInnerBatch(TransactionMessage, 7, "client-batch", items)
	if err != nil {
		t.Fatalf("SerializeInnerBatch returned error: %v", err)
	}
	envelope, err := DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if envelope.Kind != InnerBatch {
		t.Fatalf("unexpected envelope kind: %d", envelope.Kind)
	}
	itemKind, got, err := DeserializeInnerBatch(envelope)
	if err != nil {
		t.Fatalf("DeserializeInnerBatch returned error: %v", err)
	}
	if itemKind != TransactionMessage {
		t.Fatalf("unexpected item kind: %d", itemKind)
	}
	if len(got) != len(items) {
		t.Fatalf("unexpected item count: got %d want %d", len(got), len(items))
	}
	for i := range items {
		if string(got[i]) != string(items[i]) {
			t.Fatalf("item %d mismatch: got %q want %q", i, got[i], items[i])
		}
	}
}

func TestInnerBatchEmpty(t *testing.T) {
	if _, err := SerializeInnerBatch(TransactionMessage, 1, "c", nil); err == nil {
		t.Fatalf("expected error for empty batch")
	}
}

func TestInnerBatchInvalidItemKind(t *testing.T) {
	if _, err := SerializeInnerBatch(InternalEOF, 1, "c", [][]byte{{1}}); err == nil {
		t.Fatalf("expected error for non-payload item kind")
	}
}

func TestDeserializeInnerBatchRejectsWrongEnvelope(t *testing.T) {
	msg, err := SerializeAllTransactionsEOF(1, "c", 0)
	if err != nil {
		t.Fatalf("SerializeAllTransactionsEOF returned error: %v", err)
	}
	env, err := DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if _, _, err := DeserializeInnerBatch(env); !errors.Is(err, ErrUnexpectedKind) {
		t.Fatalf("expected ErrUnexpectedKind, got %v", err)
	}
}
