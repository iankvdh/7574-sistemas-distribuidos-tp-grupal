package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
)

const batchHeaderBytes = 1 + 4 // MsgType + uint32 batch size

func readAndBatchCSV[T any](
	filePath string,
	columnCount int,
	maxBytes int,
	parse func([]string) (T, error),
	size func(T) int,
	flush func([]T) error,
) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = columnCount

	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("reading header from %s: %w", filePath, err)
	}

	batch := make([]T, 0, 16)
	batchBytes := batchHeaderBytes
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row from %s: %w", filePath, err)
		}

		item, err := parse(row)
		if err != nil {
			return fmt.Errorf("parsing row from %s: %w", filePath, err)
		}

		itemBytes := size(item)
		if itemBytes+batchHeaderBytes > maxBytes {
			return fmt.Errorf(
				"record too large for BATCH_MAX_BYTES in %s: record=%d header=%d max=%d",
				filePath, itemBytes, batchHeaderBytes, maxBytes,
			)
		}

		if len(batch) > 0 && batchBytes+itemBytes > maxBytes {
			if err := flush(batch); err != nil {
				return err
			}
			batch = batch[:0]
			batchBytes = batchHeaderBytes
		}

		batch = append(batch, item)
		batchBytes += itemBytes
	}

	if len(batch) > 0 {
		return flush(batch)
	}
	return nil
}
