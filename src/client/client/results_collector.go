package client

import "log/slog"

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
		eofByQuery: make(map[uint8]struct{}, client.config.ExpectedQueryEOFs),
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

func (client *Client) cleanupPartialResults() {
	if client.clientID == "" {
		return
	}
	if err := removeResultFiles(client.config.ResultsDir, client.clientID); err != nil {
		slog.Warn("While removing partial result files", "client_id", client.clientID, "err", err)
	}
}

func (client *Client) hasAllQueryEOFs() bool {
	if client.results == nil || client.config.ExpectedQueryEOFs <= 0 {
		return false
	}
	return len(client.results.eofByQuery) >= client.config.ExpectedQueryEOFs
}
