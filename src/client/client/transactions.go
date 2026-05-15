package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
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
	dateStrLength = 10
	centsDecimalPlaces = 2
	centsPerUnit       = 100
)

func (client *Client) sendTransactions() error {
	file, err := os.Open(client.config.InputTransactions)
	if err != nil {
		return fmt.Errorf("opening transactions file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = txColumnCount

	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("reading transactions header: %w", err)
	}

	batch := make([]transaction.Transaction, 0, client.config.BatchSize)
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading transaction row: %w", err)
		}

		tx, err := parseTransactionRow(row)
		if err != nil {
			return fmt.Errorf("parsing transaction row: %w", err)
		}
		batch = append(batch, tx)

		if len(batch) >= client.config.BatchSize {
			if err := client.flushTransactionBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := client.flushTransactionBatch(batch); err != nil {
			return err
		}
	}

	if err := external.WriteEndOfTransactions(client.conn); err != nil {
		return err
	}
	return client.expectMsgType(external.Ack)
}

func (client *Client) flushTransactionBatch(batch []transaction.Transaction) error {
	if err := external.WriteTransactionBatch(client.conn, batch); err != nil {
		return err
	}
	return client.expectMsgType(external.Ack)
}

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
	amountCents, err := parseAmountCents(row[txColAmountPaid])
	if err != nil {
		return transaction.Transaction{}, fmt.Errorf("amount paid: %w", err)
	}

	return transaction.Transaction{
		Date:            date,
		FromBank:        fromBank,
		FromAccount:     row[txColFromAccount],
		ToBank:          toBank,
		ToAccount:       row[txColToAccount],
		AmountPaidCents: amountCents,
		PaymentCurrency: row[txColPaymentCurrency],
		PaymentFormat:   row[txColPaymentFormat],
	}, nil
}

// parseDate accepts the dataset's "YYYY/MM/DD HH:MM" format and returns YYYYMMDD as uint32. 
// Time-of-day is dropped because all queries operate at daily granularity.
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

// parseAmountCents converts a decimal string like "6794.63" into 679463 cents without going through float64.
func parseAmountCents(amount string) (uint64, error) {
	if amount == "" {
		return 0, errors.New("empty amount")
	}
	parts := strings.SplitN(amount, ".", 2)
	whole, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	var frac uint64
	if len(parts) == 2 {
		fracStr := parts[1]
		if len(fracStr) >= centsDecimalPlaces {
			fracStr = fracStr[:centsDecimalPlaces]
		} else {
			fracStr = fracStr + strings.Repeat("0", centsDecimalPlaces-len(fracStr))
		}
		frac, err = strconv.ParseUint(fracStr, 10, 64)
		if err != nil {
			return 0, err
		}
	}
	return whole*centsPerUnit + frac, nil
}

func parseBankID(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}
