package inner

import (
	"math"
	"testing"
)

func TestSerializeDeserializeQ2PartialMax(t *testing.T) {
	cases := []Q2PartialMax{
		{BankID: 11, FromAccount: "ACC-A", MaxAmount: 1234.56},
		{BankID: 0, FromAccount: "", MaxAmount: 0},
		{BankID: math.MaxUint32, FromAccount: "x", MaxAmount: 0.01},
	}
	for _, want := range cases {
		payload, err := SerializeQ2PartialMax(&want)
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		got, err := DeserializeQ2PartialMax(payload)
		if err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if got.BankID != want.BankID || got.FromAccount != want.FromAccount || got.MaxAmount != want.MaxAmount {
			t.Fatalf("round-trip mismatch: got %+v want %+v", *got, want)
		}
	}
}

func TestSerializeDeserializeQ2BankName(t *testing.T) {
	cases := []Q2BankName{
		{BankID: 7, BankName: "Banco Galicia"},
		{BankID: 0, BankName: ""},
		{BankID: math.MaxUint32, BankName: "BBVA"},
	}
	for _, want := range cases {
		payload, err := SerializeQ2BankName(&want)
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		got, err := DeserializeQ2BankName(payload)
		if err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if got.BankID != want.BankID || got.BankName != want.BankName {
			t.Fatalf("round-trip mismatch: got %+v want %+v", *got, want)
		}
	}
}

func TestSerializeDeserializeQ2Result(t *testing.T) {
	cases := []Q2Result{
		{BankID: 11, BankName: "Banco Galicia", FromAccount: "ACC-A", MaxAmount: 9999.99},
		{BankID: 0, BankName: "", FromAccount: "", MaxAmount: 0},
		{BankID: math.MaxUint32, BankName: "BBVA", FromAccount: "X", MaxAmount: math.MaxFloat64},
	}
	for _, want := range cases {
		payload, err := SerializeQ2Result(&want)
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		got, err := DeserializeQ2Result(payload)
		if err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if got.BankID != want.BankID || got.BankName != want.BankName || got.FromAccount != want.FromAccount || got.MaxAmount != want.MaxAmount {
			t.Fatalf("round-trip mismatch: got %+v want %+v", *got, want)
		}
	}
}

func TestQ2ItemsAreValidBatchKinds(t *testing.T) {
	for _, kind := range []MsgKind{Q2PartialMaxItem, Q2BankNameItem, Q2ResultItem} {
		if !validInnerBatchItemKind(kind) {
			t.Fatalf("Q2 kind %d should be allowed inside InnerBatch", kind)
		}
	}
}
