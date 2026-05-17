package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

const (
	accountBatchHeaderBytes = 1 + 4 // MsgType + uint32 batch size
)

func (client *Client) sendAccounts() error {
	file, err := os.Open(client.config.InputAccounts)
	if err != nil {
		return fmt.Errorf("opening accounts file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = accColumnCount

	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("reading accounts header: %w", err)
	}

	batch := make([]account.Account, 0, 16)
	batchBytes := accountBatchHeaderBytes
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading account row: %w", err)
		}

		acc, err := parseAccountRow(row)
		if err != nil {
			return fmt.Errorf("parsing account row: %w", err)
		}

		accBytes := accountSize(acc)
		if accBytes+accountBatchHeaderBytes > client.config.BatchMaxBytes {
			return fmt.Errorf(
				"account record too large for BATCH_MAX_BYTES: record=%d header=%d max=%d",
				accBytes,
				accountBatchHeaderBytes,
				client.config.BatchMaxBytes,
			)
		}

		if len(batch) > 0 && batchBytes+accBytes > client.config.BatchMaxBytes {
			if err := client.flushAccountBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
			batchBytes = accountBatchHeaderBytes
		}

		batch = append(batch, acc)
		batchBytes += accBytes
	}

	if len(batch) > 0 {
		if err := client.flushAccountBatch(batch); err != nil {
			return err
		}
	}

	if err := external.WriteEndOfAccounts(client.conn); err != nil {
		return err
	}
	return client.expectMsgType(external.Ack)
}

func (client *Client) flushAccountBatch(batch []account.Account) error {
	if err := external.WriteAccountBatch(client.conn, batch); err != nil {
		return err
	}
	return client.expectMsgType(external.Ack)
}

func accountSize(acc account.Account) int {
	return 1 + len(acc.BankName) +
		4 + // BankID
		1 + len(acc.AccountNumber) +
		1 + len(acc.EntityID) +
		1 + len(acc.EntityName)
}
