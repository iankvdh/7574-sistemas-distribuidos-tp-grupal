package client

import (
	"net"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

func (client *Client) sendAccounts() error {
	if err := readAndBatchCSV(
		client.config.InputAccounts,
		accColumnCount,
		client.config.MaxExternalBatchBytes,
		parseAccountRow,
		accountSize,
		client.flushAccountBatch,
	); err != nil {
		return err
	}
	return client.sendIngestMessage(func(conn net.Conn) error {
		return external.WriteEndOfAccounts(conn)
	})
}

func (client *Client) flushAccountBatch(batch []account.Account) error {
	return client.sendIngestMessage(func(conn net.Conn) error {
		return external.WriteAccountBatch(conn, batch)
	})
}

func accountSize(acc account.Account) int {
	return 1 + len(acc.BankName) +
		4 + // BankID
		1 + len(acc.AccountNumber) +
		1 + len(acc.EntityID) +
		1 + len(acc.EntityName)
}
