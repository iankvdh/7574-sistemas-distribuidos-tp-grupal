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
)

type GatewayConfig struct {
	AllTransactionsQueue string
	AllAccountsQueue     string
	FinalQueue           string
	GatewayID            int
	ServerHost           string
	ServerPort           string
	MomHost              string
	MomPort              int
}

func Load() (GatewayConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return GatewayConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return GatewayConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	rawGatewayID := os.Getenv("GATEWAY_ID")
	if rawGatewayID == "" {
		return GatewayConfig{}, errors.New("GATEWAY_ID environment variable is required")
	}
	gatewayID, err := strconv.Atoi(rawGatewayID)
	if err != nil || gatewayID <= 0 {
		return GatewayConfig{}, errors.New("GATEWAY_ID must be a positive integer")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return GatewayConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort := defaultMomPort
	if raw := os.Getenv("MOM_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return GatewayConfig{}, fmt.Errorf("invalid MOM_PORT: %w", err)
		}
		momPort = parsed
	}

	allTransactionsQueue := envOrDefault("ALL_TRANSACTIONS_QUEUE", defaultAllTransactionsQueue)
	allAccountsQueue := envOrDefault("ALL_ACCOUNTS_QUEUE", defaultAllAccountsQueue)
	finalQueue := envOrDefault("FINAL_QUEUE", defaultFinalQueue)

	return GatewayConfig{
		AllTransactionsQueue: allTransactionsQueue,
		AllAccountsQueue:     allAccountsQueue,
		FinalQueue:           finalQueue,
		GatewayID:            gatewayID,
		ServerHost:           serverHost,
		ServerPort:           serverPort,
		MomHost:              momHost,
		MomPort:              momPort,
	}, nil
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
