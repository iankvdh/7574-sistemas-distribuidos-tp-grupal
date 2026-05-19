package external

import (
	"bytes"
	"errors"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

func TestResultBatchRoundTrip(t *testing.T) {
	items := []ResultBatchItem{
		{QueryID: 1, Status: "row-1"},
		{QueryID: 2, Status: "EOF"},
	}

	var buffer bytes.Buffer
	if err := WriteResultBatch(&buffer, items); err != nil {
		t.Fatalf("WriteResultBatch returned error: %v", err)
	}

	msgType, err := ReadMsgType(&buffer)
	if err != nil {
		t.Fatalf("ReadMsgType returned error: %v", err)
	}
	if msgType != ResultBatch {
		t.Fatalf("unexpected msg type: got=%d want=%d", msgType, ResultBatch)
	}

	decoded, err := ReadResultBatch(&buffer)
	if err != nil {
		t.Fatalf("ReadResultBatch returned error: %v", err)
	}
	if len(decoded) != len(items) {
		t.Fatalf("unexpected decoded len: got=%d want=%d", len(decoded), len(items))
	}
	for i := range items {
		if decoded[i] != items[i] {
			t.Fatalf("decoded item mismatch at %d: got=%+v want=%+v", i, decoded[i], items[i])
		}
	}
}

func TestControlAcksHaveExpectedTypes(t *testing.T) {
	var buffer bytes.Buffer

	if err := WriteIngestAck(&buffer); err != nil {
		t.Fatalf("WriteIngestAck returned error: %v", err)
	}
	msgType, err := ReadMsgType(&buffer)
	if err != nil {
		t.Fatalf("ReadMsgType for IngestAck returned error: %v", err)
	}
	if msgType != IngestAck {
		t.Fatalf("unexpected msg type for ingest ack: got=%d want=%d", msgType, IngestAck)
	}

	if err := WriteResultBatchAck(&buffer); err != nil {
		t.Fatalf("WriteResultBatchAck returned error: %v", err)
	}
	msgType, err = ReadMsgType(&buffer)
	if err != nil {
		t.Fatalf("ReadMsgType for ResultBatchAck returned error: %v", err)
	}
	if msgType != ResultBatchAck {
		t.Fatalf("unexpected msg type for result batch ack: got=%d want=%d", msgType, ResultBatchAck)
	}
}

func TestResultBatchRejectsLongStatus(t *testing.T) {
	tooLong := make([]byte, 256)
	for i := range tooLong {
		tooLong[i] = 'a'
	}

	_, err := SerializeResultBatchPayload([]ResultBatchItem{
		{QueryID: 1, Status: string(tooLong)},
	})
	if !errors.Is(err, serializer.ErrStringTooLong) {
		t.Fatalf("expected ErrStringTooLong, got %v", err)
	}
}
