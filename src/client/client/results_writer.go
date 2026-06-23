package client

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const outputDirPermissions = 0o755
const outputFilePermissions = 0o644
const outputFileFlags = os.O_CREATE | os.O_TRUNC | os.O_WRONLY

const writerBufferSize = 16384

var queryHeaders = map[uint8]string{
	1: "From Bank,Account,To Bank,Account.1,Amount Paid",
	2: "Bank ID,Account,Bank Name,Amount Paid",
	3: "From Bank,Account,Payment Format,Amount Paid",
	4: "From Bank,Account,To Bank,Account.1",
	5: "count",
}

type queryResultFile struct {
	file   *os.File
	writer *bufio.Writer
}

func (queryFile *queryResultFile) close() error {
	flushErr := queryFile.writer.Flush()
	closeErr := queryFile.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

type queryResultsWriter struct {
	outputDir string
	clientID  string
	files     map[uint8]*queryResultFile
}

func newQueryResultsWriter(outputDir, clientID string) (*queryResultsWriter, error) {
	if outputDir == "" {
		return nil, errors.New("results output dir is required")
	}
	if err := os.MkdirAll(outputDir, outputDirPermissions); err != nil {
		return nil, err
	}

	return &queryResultsWriter{
		outputDir: outputDir,
		clientID:  clientID,
		files:     make(map[uint8]*queryResultFile),
	}, nil
}

func (writer *queryResultsWriter) WriteRow(queryID uint8, row string) error {
	queryFile, err := writer.ensureFile(queryID)
	if err != nil {
		return err
	}
	if _, err := queryFile.writer.WriteString(row); err != nil {
		return err
	}
	return queryFile.writer.WriteByte('\n')
}

func (writer *queryResultsWriter) MarkQueryEOF(queryID uint8) error {
	queryFile, err := writer.ensureFile(queryID)
	if err != nil {
		return err
	}

	delete(writer.files, queryID)
	return queryFile.close()
}

func (writer *queryResultsWriter) Close() error {
	var firstErr error
	for queryID, queryFile := range writer.files {
		if err := queryFile.close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("while closing results file for query %d: %w", queryID, err)
		}
		delete(writer.files, queryID)
	}
	return firstErr
}

func (writer *queryResultsWriter) ensureFile(queryID uint8) (*queryResultFile, error) {
	header, ok := queryHeaders[queryID]
	if !ok {
		return nil, fmt.Errorf("unsupported query id: %d", queryID)
	}

	if queryFile, exists := writer.files[queryID]; exists {
		return queryFile, nil
	}

	return writer.createQueryResultFile(queryID, header)
}

func (writer *queryResultsWriter) createQueryResultFile(queryID uint8, header string) (*queryResultFile, error) {
	filePath := filepath.Join(writer.outputDir, resultFileName(writer.clientID, queryID))
	file, err := os.OpenFile(filePath, outputFileFlags, outputFilePermissions)
	if err != nil {
		return nil, err
	}
	bufferedWriter := bufio.NewWriterSize(file, writerBufferSize)
	if _, err := bufferedWriter.WriteString(header + "\n"); err != nil {
		_ = file.Close()
		return nil, err
	}

	queryFile := &queryResultFile{file: file, writer: bufferedWriter}
	writer.files[queryID] = queryFile
	return queryFile, nil
}

func resultFileName(clientID string, queryID uint8) string {
	return fmt.Sprintf("%s_q%d.csv", clientID, queryID)
}

func removeResultFiles(dir, clientID string) error {
	var firstErr error
	for queryID := range queryHeaders {
		path := filepath.Join(dir, resultFileName(clientID, queryID))
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
