package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

const (
	defaultAllTransactionsQueue = "all_transactions"
	defaultAllAccountsQueue     = "all_accounts"
	defaultFinalQueue           = "final"
	defaultMomPort              = 5672
	defaultBatchMaxBytes        = 8192
)

type SerialAnalyzerConfig struct {
	AllTransactionsQueue string
	AllAccountsQueue     string
	FinalQueue           string
	MomHost              string
	MomPort              int
	BatchMaxBytes        int
}

func Load() (SerialAnalyzerConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return SerialAnalyzerConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort := defaultMomPort
	if raw := os.Getenv("MOM_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return SerialAnalyzerConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
		}
		momPort = parsed
	}

	batchMaxBytes := defaultBatchMaxBytes
	if raw := os.Getenv("BATCH_MAX_BYTES"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return SerialAnalyzerConfig{}, fmt.Errorf("invalid BATCH_MAX_BYTES: %q", raw)
		}
		batchMaxBytes = parsed
	}

	return SerialAnalyzerConfig{
		AllTransactionsQueue: envOrDefault("ALL_TRANSACTIONS_QUEUE", defaultAllTransactionsQueue),
		AllAccountsQueue:     envOrDefault("ALL_ACCOUNTS_QUEUE", defaultAllAccountsQueue),
		FinalQueue:           envOrDefault("FINAL_QUEUE", defaultFinalQueue),
		MomHost:              momHost,
		MomPort:              momPort,
		BatchMaxBytes:        batchMaxBytes,
	}, nil
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
