package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

const (
	defaultAllTransactionsQueue  = "all_transactions"
	defaultAllAccountsQueue      = "all_accounts"
	defaultFinalQueue            = "final"
	defaultMomPort               = 5672
	defaultMaxInternalBatchBytes = 65536
)

type SerialAnalyzerConfig struct {
	AllTransactionsQueue  string
	AllAccountsQueue      string
	FinalQueue            string
	MomHost               string
	MomPort               int
	MaxInternalBatchBytes int
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

	maxInternalBatchBytes := defaultMaxInternalBatchBytes
	if raw := os.Getenv("MAX_INTERNAL_BATCH_BYTES"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return SerialAnalyzerConfig{}, fmt.Errorf("invalid MAX_INTERNAL_BATCH_BYTES: %q", raw)
		}
		maxInternalBatchBytes = parsed
	}

	return SerialAnalyzerConfig{
		AllTransactionsQueue:  envOrDefault("ALL_TRANSACTIONS_QUEUE", defaultAllTransactionsQueue),
		AllAccountsQueue:      envOrDefault("ALL_ACCOUNTS_QUEUE", defaultAllAccountsQueue),
		FinalQueue:            envOrDefault("FINAL_QUEUE", defaultFinalQueue),
		MomHost:               momHost,
		MomPort:               momPort,
		MaxInternalBatchBytes: maxInternalBatchBytes,
	}, nil
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
