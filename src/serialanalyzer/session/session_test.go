package session

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/serialanalyzer/queries"
)

func TestQueryReadinessAndEOFByClient(t *testing.T) {
	processor := NewProcessor()
	gatewayID := inner.GatewayID(1)
	clientID := inner.ClientID("client-a")

	txs := []transaction.Transaction{
		{Date: 20220901, FromBank: 20, FromAccount: "A", ToBank: 31, ToAccount: "B", AmountPaid: 10.00, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
	}
	accs := []account.Account{
		{BankID: 20, BankName: "Bank 20"},
	}

	outputs := processor.AddTransactions(gatewayID, clientID, txs)
	if len(outputs) != 1 {
		t.Fatalf("expected one incremental Q1 row, got %d", len(outputs))
	}
	if outputs[0].QueryID != queries.Query1ID {
		t.Fatalf("expected incremental output for Q1, got query=%d", outputs[0].QueryID)
	}
	if outputs[0].Data != "20,A,31,B,10.00" {
		t.Fatalf("unexpected Q1 incremental row: %q", outputs[0].Data)
	}
	outputs = processor.MarkTransactionsEOF(gatewayID, clientID, 1)

	if hasQueryEOF(outputs, queries.Query1ID) == false ||
		hasQueryEOF(outputs, queries.Query3ID) == false ||
		hasQueryEOF(outputs, queries.Query4ID) == false ||
		hasQueryEOF(outputs, queries.Query5ID) == false {
		t.Fatalf("expected EOF for Q1,Q3,Q4,Q5 after tx EOF, got outputs=%v", outputs)
	}
	if hasQueryEOF(outputs, queries.Query2ID) {
		t.Fatalf("did not expect Q2 EOF before account EOF, outputs=%v", outputs)
	}

	if outputs := processor.AddAccounts(gatewayID, clientID, accs); len(outputs) != 0 {
		t.Fatalf("expected no outputs while appending account batch, got %d", len(outputs))
	}
	outputs = processor.MarkAccountsEOF(gatewayID, clientID, 1)
	if !hasQueryEOF(outputs, queries.Query2ID) {
		t.Fatalf("expected Q2 EOF after account EOF, outputs=%v", outputs)
	}
}

func TestClientIsolation(t *testing.T) {
	processor := NewProcessor()

	gatewayA := inner.GatewayID(1)
	gatewayB := inner.GatewayID(2)
	clientA := inner.ClientID("same-client-id")
	clientB := inner.ClientID("same-client-id")

	processor.AddTransactions(gatewayA, clientA, []transaction.Transaction{
		{Date: 20220901, FromBank: 1, FromAccount: "A1", ToBank: 2, ToAccount: "B1", AmountPaid: 49.99, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
	})
	processor.AddTransactions(gatewayB, clientB, []transaction.Transaction{
		{Date: 20220901, FromBank: 9, FromAccount: "X1", ToBank: 8, ToAccount: "Y1", AmountPaid: 100.00, PaymentCurrency: "US Dollar", PaymentFormat: "Wire"},
	})

	outputsA := processor.MarkTransactionsEOF(gatewayA, clientA, 1)
	if !hasQueryEOF(outputsA, queries.Query1ID) {
		t.Fatalf("expected query outputs for client A, got %v", outputsA)
	}
	if hasQueryRow(outputsA, "9,X1,8,Y1,100.00") {
		t.Fatalf("unexpected row from client B leaked into client A outputs: %v", outputsA)
	}
}

func hasQueryEOF(outputs []QueryOutput, queryID uint8) bool {
	for _, out := range outputs {
		if out.QueryID == queryID && out.Data == EOFStatus {
			return true
		}
	}
	return false
}

func hasQueryRow(outputs []QueryOutput, row string) bool {
	for _, out := range outputs {
		if out.Data == row {
			return true
		}
	}
	return false
}
