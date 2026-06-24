package inner

import (
	"reflect"
	"testing"
)

const testClientID = ClientID("550e8400-e29b-41d4-a716-446655440000")

func headerWith(branchPath []uint8) Header {
	return Header{
		GatewayID:       7,
		ClientID:        testClientID,
		SeqID:           42,
		SenderStageType: StageFilterPeriod1,
		SenderReplicaID: 3,
		MinterStageType: StageGateway,
		MinterReplicaID: 1,
		BranchPath:      branchPath,
	}
}

func roundTrip(t *testing.T, msg InternalMessage) InternalMessage {
	t.Helper()
	data, err := msg.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	parsed, err := NewFromSerializedData(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return parsed
}

func TestBatchRoundTrip(t *testing.T) {
	for _, branchPath := range [][]uint8{nil, {0}, {0, 2, 1}} {
		h := headerWith(branchPath)
		msg := &BatchMessage{
			Header:   h,
			ItemKind: TransactionMessage,
			Items: []BatchItem{
				{QueryID: 1, Payload: []byte("abc")},
				{QueryID: 3, Payload: []byte("xyz")},
			},
		}
		got, ok := roundTrip(t, msg).(*BatchMessage)
		if !ok {
			t.Fatalf("branchPath %v: wrong type", branchPath)
		}
		if !reflect.DeepEqual(got.Header, h) {
			t.Fatalf("branchPath %v: header mismatch\n got=%+v\nwant=%+v", branchPath, got.Header, h)
		}
		if got.ItemKind != msg.ItemKind || !reflect.DeepEqual(got.Items, msg.Items) {
			t.Fatalf("branchPath %v: body mismatch", branchPath)
		}
	}
}

func TestEmptyBatchRoundTrip(t *testing.T) {
	msg := &BatchMessage{Header: headerWith([]uint8{1}), ItemKind: TransactionMessage, Items: nil}
	got, ok := roundTrip(t, msg).(*BatchMessage)
	if !ok {
		t.Fatal("wrong type")
	}
	if len(got.Items) != 0 {
		t.Fatalf("expected empty batch, got %d items", len(got.Items))
	}
	if got.ItemKind != TransactionMessage {
		t.Fatalf("item kind mismatch: %d", got.ItemKind)
	}
}

func TestIDSpaceDistinguishesBranchesAndMinters(t *testing.T) {
	base := headerWith(nil)
	matched := base
	matched.BranchPath = []uint8{0}
	nomatched := base
	nomatched.BranchPath = []uint8{2}
	if IDSpaceOf(matched) == IDSpaceOf(nomatched) {
		t.Fatal("distinct branches must yield distinct idSpace")
	}

	reducerA := base
	reducerA.MinterStageType = StageCounterQ4
	reducerA.MinterReplicaID = 0
	reducerB := base
	reducerB.MinterStageType = StageCounterQ4
	reducerB.MinterReplicaID = 1
	if IDSpaceOf(reducerA) == IDSpaceOf(reducerB) {
		t.Fatal("distinct minter replicas must yield distinct idSpace")
	}
}

func TestEOFRoundTrip(t *testing.T) {
	h := headerWith([]uint8{2, 0})
	msg := &EOFMessage{Header: h, QueryID: 4, Total: 12345}
	got, ok := roundTrip(t, msg).(*EOFMessage)
	if !ok {
		t.Fatal("wrong type")
	}
	if !reflect.DeepEqual(got.Header, h) || got.QueryID != 4 || got.Total != 12345 {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestRingTokenRoundTrip(t *testing.T) {
	h := headerWith([]uint8{1})
	msg := &RingTokenMessage{Header: h, InitiatorID: 2, AggMatched: 100, AggNotMatched: 50, Phase: 1}
	got, ok := roundTrip(t, msg).(*RingTokenMessage)
	if !ok {
		t.Fatal("wrong type")
	}
	if !reflect.DeepEqual(got.Header, h) || got.InitiatorID != 2 || got.AggMatched != 100 ||
		got.AggNotMatched != 50 || got.Phase != 1 {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestBranchPathShiftsBody(t *testing.T) {
	empty := &BatchMessage{Header: headerWith(nil), ItemKind: ResultRow, Items: []BatchItem{{QueryID: 1, Payload: []byte("p")}}}
	withPath := &BatchMessage{Header: headerWith([]uint8{5, 6, 7, 8}), ItemKind: ResultRow, Items: []BatchItem{{QueryID: 1, Payload: []byte("p")}}}

	emptyData, _ := empty.Serialize()
	pathData, _ := withPath.Serialize()
	if len(pathData)-len(emptyData) != 4 {
		t.Fatalf("expected 4 extra header bytes, got %d", len(pathData)-len(emptyData))
	}

	got := roundTrip(t, withPath).(*BatchMessage)
	if !reflect.DeepEqual(got.Items, withPath.Items) {
		t.Fatalf("body corrupted by branchPath offset: %+v", got.Items)
	}
}

func TestHeaderWireSizeIsMinimum(t *testing.T) {
	if HeaderWireSize() != minHeaderSize || minHeaderSize != 34 {
		t.Fatalf("HeaderWireSize=%d minHeaderSize=%d, want 34", HeaderWireSize(), minHeaderSize)
	}
}

func TestTruncatedBufferRejected(t *testing.T) {
	msg := &BatchMessage{Header: headerWith([]uint8{1, 2, 3}), ItemKind: TransactionMessage, Items: []BatchItem{{QueryID: 1, Payload: []byte("p")}}}
	data, _ := msg.Serialize()
	if _, err := NewFromSerializedData(data[:minHeaderSize+1]); err == nil {
		t.Fatal("expected error on truncated branchPath")
	}
}
