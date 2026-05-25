package queries

import (
	"reflect"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func TestQuery1Rows(t *testing.T) {
	txs := []transaction.Transaction{
		{FromBank: 10, FromAccount: "A1", ToBank: 20, ToAccount: "B1", AmountPaid: 49.99, PaymentCurrency: "US Dollar"},
		{FromBank: 11, FromAccount: "A2", ToBank: 21, ToAccount: "B2", AmountPaid: 50.00, PaymentCurrency: "US Dollar"},
		{FromBank: 12, FromAccount: "A3", ToBank: 22, ToAccount: "B3", AmountPaid: 10.00, PaymentCurrency: "Euro"},
	}

	got := Query1Rows(txs)
	want := []string{"10,A1,20,B1,49.99"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected Q1 rows:\n got=%v\nwant=%v", got, want)
	}
}

func TestQuery2Rows(t *testing.T) {
	txs := []transaction.Transaction{
		{FromBank: 20, FromAccount: "A1", AmountPaid: 10.00, PaymentCurrency: "US Dollar"},
		{FromBank: 20, FromAccount: "A2", AmountPaid: 30.00, PaymentCurrency: "US Dollar"},
		{FromBank: 31, FromAccount: "X1", AmountPaid: 25.00, PaymentCurrency: "US Dollar"},
	}
	accs := []account.Account{
		{BankID: 20, BankName: "Bank 20"},
	}

	got := Query2Rows(txs, accs)
	want := []string{
		"20,A2,Bank 20,30.00",
		"31,X1,UNKNOWN_31,25.00",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected Q2 rows:\n got=%v\nwant=%v", got, want)
	}
}

func TestQuery3Rows(t *testing.T) {
	txs := []transaction.Transaction{
		{Date: 20220901, FromBank: 1, FromAccount: "B1", AmountPaid: 100.00, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
		{Date: 20220902, FromBank: 1, FromAccount: "B2", AmountPaid: 300.00, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
		{Date: 20220906, FromBank: 9, FromAccount: "X1", AmountPaid: 1.99, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
		{Date: 20220907, FromBank: 9, FromAccount: "X2", AmountPaid: 2.00, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
	}

	got := Query3Rows(txs)
	want := []string{"9,X1,Wire,1.99"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected Q3 rows:\n got=%v\nwant=%v", got, want)
	}
}

func TestQuery4Rows(t *testing.T) {
	txs := []transaction.Transaction{
		{Date: 20220901, FromBank: 1, FromAccount: "SRC", ToBank: 2, ToAccount: "I1", PaymentCurrency: "US Dollar"},
		{Date: 20220901, FromBank: 1, FromAccount: "SRC", ToBank: 2, ToAccount: "I2", PaymentCurrency: "US Dollar"},
		{Date: 20220901, FromBank: 1, FromAccount: "SRC", ToBank: 2, ToAccount: "I3", PaymentCurrency: "US Dollar"},
		{Date: 20220901, FromBank: 1, FromAccount: "SRC", ToBank: 2, ToAccount: "I4", PaymentCurrency: "US Dollar"},
		{Date: 20220901, FromBank: 1, FromAccount: "SRC", ToBank: 2, ToAccount: "I5", PaymentCurrency: "US Dollar"},
		{Date: 20220902, FromBank: 2, FromAccount: "I1", ToBank: 9, ToAccount: "DST", PaymentCurrency: "US Dollar"},
		{Date: 20220902, FromBank: 2, FromAccount: "I2", ToBank: 9, ToAccount: "DST", PaymentCurrency: "US Dollar"},
		{Date: 20220902, FromBank: 2, FromAccount: "I3", ToBank: 9, ToAccount: "DST", PaymentCurrency: "US Dollar"},
		{Date: 20220902, FromBank: 2, FromAccount: "I4", ToBank: 9, ToAccount: "DST", PaymentCurrency: "US Dollar"},
		{Date: 20220902, FromBank: 2, FromAccount: "I5", ToBank: 9, ToAccount: "DST", PaymentCurrency: "US Dollar"},
	}

	got := Query4Rows(txs)
	want := []string{
		"1,SRC,9,DST",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected Q4 rows:\n got=%v\nwant=%v", got, want)
	}
}

func TestQuery5Rows(t *testing.T) {
	txs := []transaction.Transaction{
		{Date: 20220901, AmountPaid: 0.50, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
		{Date: 20220902, AmountPaid: 10.00, PaymentCurrency: "Yen", PaymentFormat: "ACH"},
		{Date: 20220902, AmountPaid: 10.00, PaymentCurrency: "Euro", PaymentFormat: "ACH"},
		{Date: 20220910, AmountPaid: 0.50, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
		{Date: 20220901, AmountPaid: 0.50, PaymentCurrency: "US Dollar", PaymentFormat: "Cheque"},
	}

	got := Query5Rows(txs)
	want := []string{"2"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected Q5 rows:\n got=%v\nwant=%v", got, want)
	}
}
