package inner

import (
	"errors"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
)

func TestSerializeDeserializeEnvelope(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	msg := SerializeEnvelope(AllTransactionsBatch, "client-77", 0, payload)
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
	if len(envelope.Payload) != len(payload) {
		t.Fatalf("unexpected payload len: got %d expected %d", len(envelope.Payload), len(payload))
	}
	for i := range payload {
		if envelope.Payload[i] != payload[i] {
			t.Fatalf("payload mismatch at %d", i)
		}
	}
}

func TestFinalQueryResultRoundTrip(t *testing.T) {
	msg, err := SerializeFinalQueryResult("client-10", 3, "ACK")
	if err != nil {
		t.Fatalf("SerializeFinalQueryResult returned error: %v", err)
	}

	envelope, err := DeserializeEnvelope(msg)
	if err != nil {
		t.Fatalf("DeserializeEnvelope returned error: %v", err)
	}
	if envelope.Kind != FinalQueryResult {
		t.Fatalf("unexpected kind: got %d", envelope.Kind)
	}
	if envelope.ClientID != "client-10" {
		t.Fatalf("unexpected client id: got %s", envelope.ClientID)
	}

	if envelope.QueryID != 3 {
		t.Fatalf("unexpected query id: got %d", envelope.QueryID)
	}
	if envelope.Status != "ACK" {
		t.Fatalf("unexpected status: got %s", envelope.Status)
	}
}

func TestSerializeDeserializeEnvelopeBinaryPayload(t *testing.T) {
	// Payload intentionally includes bytes that are not valid UTF-8.
	payload := []byte{0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF}

	msg := SerializeAllTransactionsBatch("client-bin", payload)
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
	msg := SerializeAllAccountsEOF("client-eof", 123)
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
		{name: "missing client", msg: &middleware.Message{Body: `{"k":1}`}},
		{name: "missing kind", msg: &middleware.Message{Body: `{"c":"abc"}`}},
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
