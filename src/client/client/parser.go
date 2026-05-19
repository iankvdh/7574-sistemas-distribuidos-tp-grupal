package client

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

const (
	txColTimestamp = iota
	txColFromBank
	txColFromAccount
	txColToBank
	txColToAccount
	txColAmountReceived
	txColReceivingCurrency
	txColAmountPaid
	txColPaymentCurrency
	txColPaymentFormat
	txColIsLaundering
	txColumnCount
)

const (
	accColBankName = iota
	accColBankID
	accColAccountNumber
	accColEntityID
	accColEntityName
	accColumnCount
)

// Parsing constants.
const (
	dateStrLength = 10
)

func parseTransactionRow(row []string) (transaction.Transaction, error) {
	date, err := parseDate(row[txColTimestamp])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("date: %w", err)
	}
	fromBank, err := parseBankID(row[txColFromBank])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("from bank: %w", err)
	}
	toBank, err := parseBankID(row[txColToBank])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("to bank: %w", err)
	}
	amountPaid, err := parseAmount(row[txColAmountPaid])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("amount paid: %w", err)
	}

	return transaction.Transaction{
		Date:            date,
		FromBank:        fromBank,
		FromAccount:     row[txColFromAccount],
		ToBank:          toBank,
		ToAccount:       row[txColToAccount],
		AmountPaid:      amountPaid,
		PaymentCurrency: row[txColPaymentCurrency],
		PaymentFormat:   row[txColPaymentFormat],
	}, nil
}

func parseAccountRow(row []string) (account.Account, error) {
	bankID, err := parseBankID(row[accColBankID])
	if err != nil {
		return account.Account{}, fmt.Errorf("bank id: %w", err)
	}
	return account.Account{
		BankName:      row[accColBankName],
		BankID:        bankID,
		AccountNumber: row[accColAccountNumber],
		EntityID:      row[accColEntityID],
		EntityName:    row[accColEntityName],
	}, nil
}

// parseDate accepts the dataset's "YYYY/MM/DD HH:MM" format and returns
// YYYYMMDD as uint32. Time-of-day is dropped because all queries operate at
// daily granularity.
func parseDate(timestamp string) (uint32, error) {
	if len(timestamp) < dateStrLength {
		return 0, fmt.Errorf("timestamp too short: %q", timestamp)
	}
	dateStr := timestamp[:dateStrLength]
	parts := strings.SplitN(dateStr, "/", 3)
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid date: %q", dateStr)
	}
	year, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, err
	}
	month, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, err
	}
	day, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(year*10000 + month*100 + day), nil
}

func parseAmount(amount string) (float64, error) {
	if amount == "" {
		return 0, errors.New("empty amount")
	}
	return strconv.ParseFloat(strings.TrimSpace(amount), 64)
}

func parseBankID(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}
