package client

import (
	"fmt"
	"log/slog"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

const (
	queryEOFStatus     = "EOF"
	requiredQueryEOFs  = 5
	minExpectedQueryID = 1
	maxExpectedQueryID = 5
)

func (client *Client) consumeResultBatch(items []external.ResultBatchItem) error {
	for _, item := range items {
		if err := client.consumeQueryResult(item.QueryID, item.Status); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) consumeQueryResult(queryID uint8, status string) error {
	if queryID < minExpectedQueryID || queryID > maxExpectedQueryID {
		return fmt.Errorf("unexpected query id: %d", queryID)
	}

	if err := client.initResultsCollector(); err != nil {
		return err
	}

	if status == queryEOFStatus {
		if err := client.results.writer.MarkQueryEOF(queryID); err != nil {
			return err
		}

		if _, exists := client.results.eofByQuery[queryID]; !exists {
			client.results.eofByQuery[queryID] = struct{}{}
			slog.Info("Received query EOF", "client_id", client.clientID, "query", queryID, "received", len(client.results.eofByQuery))
		}
		if client.hasAllQueryEOFs() {
			slog.Info("Received all query EOF markers", "client_id", client.clientID)
			client.signalAllQueryEOFs()
		}
		return nil
	}

	if err := client.results.writer.WriteRow(queryID, status); err != nil {
		return err
	}
	slog.Info("Received query row", "client_id", client.clientID, "query", queryID, "row", status)
	return nil
}
