package client

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrUnsupportedQueryID = errors.New("unsupported query id")

var queryHeaders = map[uint8]string{
	1: "From Bank,Account,To Bank,Account.1,Amount Paid",
	2: "From Bank,Account,Bank Name,Amount Paid",
	3: "From Bank,Account,Payment Format,Amount Paid",
	4: "Bank,Account",
	5: "count",
}

type queryResultsWriter struct {
	outputDir string
	clientID  string
	files     map[uint8]*os.File
}

func newQueryResultsWriter(outputDir, clientID string) (*queryResultsWriter, error) {
	if outputDir == "" {
		return nil, errors.New("results output dir is required")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, err
	}

	return &queryResultsWriter{
		outputDir: outputDir,
		clientID:  clientID,
		files:     make(map[uint8]*os.File, requiredQueryEOFs),
	}, nil
}

func (writer *queryResultsWriter) WriteRow(queryID uint8, row string) error {
	file, err := writer.ensureFile(queryID)
	if err != nil {
		return err
	}
	_, err = file.WriteString(row + "\n")
	return err
}

func (writer *queryResultsWriter) MarkQueryEOF(queryID uint8) error {
	file, err := writer.ensureFile(queryID)
	if err != nil {
		return err
	}

	delete(writer.files, queryID)
	return file.Close()
}

func (writer *queryResultsWriter) Close() error {
	var firstErr error
	for queryID, file := range writer.files {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("while closing results file for query %d: %w", queryID, err)
		}
		delete(writer.files, queryID)
	}
	return firstErr
}

func (writer *queryResultsWriter) ensureFile(queryID uint8) (*os.File, error) {
	header, ok := queryHeaders[queryID]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedQueryID, queryID)
	}

	if file, exists := writer.files[queryID]; exists {
		return file, nil
	}

	filePath := filepath.Join(writer.outputDir, resultFileName(writer.clientID, queryID))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := file.WriteString(header + "\n"); err != nil {
		_ = file.Close()
		return nil, err
	}

	writer.files[queryID] = file
	return file, nil
}

func resultFileName(clientID string, queryID uint8) string {
	return fmt.Sprintf("%s_q%d.csv", sanitizeForFileName(clientID), queryID)
}

func sanitizeForFileName(input string) string {
	var builder strings.Builder
	builder.Grow(len(input))
	for _, current := range input {
		if (current >= 'a' && current <= 'z') ||
			(current >= 'A' && current <= 'Z') ||
			(current >= '0' && current <= '9') ||
			current == '-' || current == '_' || current == '.' {
			builder.WriteRune(current)
		} else {
			builder.WriteByte('_')
		}
	}

	if builder.Len() == 0 {
		return "client"
	}
	return builder.String()
}
