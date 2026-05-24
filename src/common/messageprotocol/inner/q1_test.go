package inner

import (
	"math"
	"testing"
)

func TestSerializeDeserializeQuery1Row(t *testing.T) {
	cases := []struct {
		name string
		row  Query1Row
	}{
		{
			name: "typical",
			row:  Query1Row{SourceBank: 11, SourceAccount: "A100", DestBank: 12, DestAccount: "B200", Amount: 49.99},
		},
		{
			name: "empty accounts",
			row:  Query1Row{SourceBank: 0, SourceAccount: "", DestBank: 0, DestAccount: "", Amount: 0},
		},
		{
			name: "max bank, fractional",
			row:  Query1Row{SourceBank: math.MaxUint32, SourceAccount: "x", DestBank: math.MaxUint32, DestAccount: "y", Amount: 0.01},
		},
		{
			name: "negative amount",
			row:  Query1Row{SourceBank: 3, SourceAccount: "neg", DestBank: 4, DestAccount: "neg2", Amount: -42.5},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := SerializeQuery1Row(&tc.row)
			if err != nil {
				t.Fatalf("SerializeQuery1Row returned error: %v", err)
			}
			got, err := DeserializeQuery1Row(payload)
			if err != nil {
				t.Fatalf("DeserializeQuery1Row returned error: %v", err)
			}
			if got.SourceBank != tc.row.SourceBank ||
				got.SourceAccount != tc.row.SourceAccount ||
				got.DestBank != tc.row.DestBank ||
				got.DestAccount != tc.row.DestAccount ||
				got.Amount != tc.row.Amount {
				t.Fatalf("round-trip mismatch: got %+v want %+v", got, tc.row)
			}
		})
	}
}

func TestSerializeQuery1RowAccountTooLong(t *testing.T) {
	long := make([]byte, 256)
	for i := range long {
		long[i] = 'a'
	}
	row := Query1Row{SourceAccount: string(long), DestAccount: "ok"}
	if _, err := SerializeQuery1Row(&row); err == nil {
		t.Fatalf("expected ErrStringTooLong, got nil")
	}
}

func TestQuery1RowItemIsValidBatchKind(t *testing.T) {
	if !validInnerBatchItemKind(Query1RowItem) {
		t.Fatalf("Query1RowItem should be allowed inside InnerBatch")
	}
	if _, err := SerializeInnerBatch(1, Query1RowItem, 1, "c", [][]byte{{0xCC}}); err != nil {
		t.Fatalf("SerializeInnerBatch with Query1RowItem failed: %v", err)
	}
}
