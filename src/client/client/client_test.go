package client

import (
	"reflect"
	"testing"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func TestBuildGatewayAddresses(t *testing.T) {
	config := ClientConfig{
		GatewayPrefix: "tp_grupal-gateway-",
		GatewayAmount: 3,
		GatewayPort:   "5678",
	}

	got := buildGatewayAddresses(config)
	want := []string{
		"tp_grupal-gateway-1:5678",
		"tp_grupal-gateway-2:5678",
		"tp_grupal-gateway-3:5678",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected gateway addresses:\n got: %v\nwant: %v", got, want)
	}
}

func TestComputeBackoffCap(t *testing.T) {
	base := 200 * time.Millisecond
	max := 3 * time.Second

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 200 * time.Millisecond},
		{attempt: 2, want: 400 * time.Millisecond},
		{attempt: 3, want: 800 * time.Millisecond},
		{attempt: 4, want: 1600 * time.Millisecond},
		{attempt: 5, want: 3 * time.Second},
		{attempt: 6, want: 3 * time.Second},
	}

	for _, tc := range tests {
		got := computeBackoffCap(base, max, tc.attempt)
		if got != tc.want {
			t.Fatalf("attempt %d: got %s want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestTransactionSize(t *testing.T) {
	tx := transaction.Transaction{
		Date:            20240101,
		FromBank:        1,
		FromAccount:     "abc",
		ToBank:          2,
		ToAccount:       "xyz",
		AmountPaid:      1.00,
		PaymentCurrency: "USD",
		PaymentFormat:   "WIRE",
	}

	got := transactionSize(tx)
	want := 37
	if got != want {
		t.Fatalf("unexpected transaction size: got %d want %d", got, want)
	}
}

func TestAccountSize(t *testing.T) {
	acc := account.Account{
		BankName:      "bank",
		BankID:        10,
		AccountNumber: "123",
		EntityID:      "e1",
		EntityName:    "alice",
	}

	got := accountSize(acc)
	want := 22
	if got != want {
		t.Fatalf("unexpected account size: got %d want %d", got, want)
	}
}
