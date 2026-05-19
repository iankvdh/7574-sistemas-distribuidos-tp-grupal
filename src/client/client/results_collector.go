package client

import (
	"fmt"
	"log/slog"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

type resultsCollector struct {
	writer     *queryResultsWriter
	eofByQuery map[uint8]struct{}
}

func (client *Client) initResultsCollector() error {
	if client.results != nil {
		return nil
	}

	writer, err := newQueryResultsWriter(client.config.ResultsDir, client.clientID)
	if err != nil {
		return err
	}

	client.results = &resultsCollector{
		writer:     writer,
		eofByQuery: make(map[uint8]struct{}, requiredQueryEOFs),
	}
	return nil
}

func (client *Client) closeResultsCollector() error {
	if client.results == nil {
		return nil
	}
	err := client.results.writer.Close()
	client.results = nil
	return err
}

func (client *Client) hasAllQueryEOFs() bool {
	if client.results == nil {
		return false
	}
	return len(client.results.eofByQuery) == requiredQueryEOFs
}

func (client *Client) consumeQueryResultFromConn() error {
	queryID, status, err := external.ReadQueryResult(client.conn)
	if err != nil {
		return err
	}

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
		}
		return nil
	}

	if err := client.results.writer.WriteRow(queryID, status); err != nil {
		return err
	}
	slog.Info("Received query row", "client_id", client.clientID, "query", queryID, "row", status)
	return nil
}
