package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
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
