package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

const (
	transactionBatchHeaderBytes = 1 + 4 // MsgType + uint32 batch size
	txBatchArrayInitialCapacity = 16
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

	batch := make([]transaction.Transaction, 0, txBatchArrayInitialCapacity)
	batchBytes := transactionBatchHeaderBytes
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

		txBytes := transactionSize(tx)
		if txBytes+transactionBatchHeaderBytes > client.config.BatchMaxBytes {
			return fmt.Errorf(
				"transaction record too large for BATCH_MAX_BYTES: record=%d header=%d max=%d",
				txBytes,
				transactionBatchHeaderBytes,
				client.config.BatchMaxBytes,
			)
		}

		if len(batch) > 0 && batchBytes+txBytes > client.config.BatchMaxBytes {
			if err := client.flushTransactionBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
			batchBytes = transactionBatchHeaderBytes
		}

		batch = append(batch, tx)
		batchBytes += txBytes
	}

	if len(batch) > 0 {
		if err := client.flushTransactionBatch(batch); err != nil {
			return err
		}
	}

	return client.sendIngestMessage(func(conn net.Conn) error {
		return external.WriteEndOfTransactions(conn)
	})
}

func (client *Client) flushTransactionBatch(batch []transaction.Transaction) error {
	return client.sendIngestMessage(func(conn net.Conn) error {
		return external.WriteTransactionBatch(conn, batch)
	})
}

func transactionSize(tx transaction.Transaction) int {
	return 4 + // Date
		4 + // FromBank
		1 + len(tx.FromAccount) +
		4 + // ToBank
		1 + len(tx.ToAccount) +
		8 + // AmountPaid (float64)
		1 + len(tx.PaymentCurrency) +
		1 + len(tx.PaymentFormat)
}
