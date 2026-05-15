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
	accColBankName = iota
	accColBankID
	accColAccountNumber
	accColEntityID
	accColEntityName
	accColumnCount
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

	batch := make([]account.Account, 0, client.config.BatchSize)
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
		batch = append(batch, acc)

		if len(batch) >= client.config.BatchSize {
			if err := client.flushAccountBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
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
