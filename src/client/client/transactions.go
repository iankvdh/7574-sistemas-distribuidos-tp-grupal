package client

import (
	"net"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func (client *Client) sendTransactions() error {
	if err := readAndBatchCSV(
		client.config.InputTransactions,
		txColumnCount,
		client.config.BatchMaxBytes,
		parseTransactionRow,
		transactionSize,
		client.flushTransactionBatch,
	); err != nil {
		return err
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
