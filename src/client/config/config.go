package config

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/client/client"
)

const (
	defaultConnectMaxAttempts = 6
	defaultBackoffBaseMs      = 200
	defaultBackoffMaxMs       = 3000
	defaultConnectTimeoutMs   = 800
	defaultQueryTimeoutMs     = 30000
	defaultBatchMaxBytes      = 8192
)

func Load() (client.ClientConfig, error) {
	gatewayPrefix := os.Getenv("GATEWAY_PREFIX")
	if gatewayPrefix == "" {
		return client.ClientConfig{}, errors.New("GATEWAY_PREFIX environment variable is required")
	}

	rawGatewayAmount := os.Getenv("GATEWAY_AMOUNT")
	if rawGatewayAmount == "" {
		return client.ClientConfig{}, errors.New("GATEWAY_AMOUNT environment variable is required")
	}
	gatewayAmount, err := strconv.Atoi(rawGatewayAmount)
	if err != nil || gatewayAmount <= 0 {
		return client.ClientConfig{}, errors.New("GATEWAY_AMOUNT must be a positive integer")
	}

	gatewayPort := os.Getenv("GATEWAY_PORT")
	if gatewayPort == "" {
		return client.ClientConfig{}, errors.New("GATEWAY_PORT environment variable is required")
	}

	inputTransactions := os.Getenv("INPUT_TRANSACTIONS")
	if inputTransactions == "" {
		return client.ClientConfig{}, errors.New("INPUT_TRANSACTIONS environment variable is required")
	}

	inputAccounts := os.Getenv("INPUT_ACCOUNTS")
	if inputAccounts == "" {
		return client.ClientConfig{}, errors.New("INPUT_ACCOUNTS environment variable is required")
	}

	batchMaxBytes, err := parsePositiveIntWithDefault("BATCH_MAX_BYTES", defaultBatchMaxBytes)
	if err != nil {
		return client.ClientConfig{}, err
	}

	connectMaxAttempts, err := parsePositiveIntWithDefault("CONNECT_MAX_ATTEMPTS", defaultConnectMaxAttempts)
	if err != nil {
		return client.ClientConfig{}, err
	}
	backoffBaseMs, err := parsePositiveIntWithDefault("BACKOFF_BASE_MS", defaultBackoffBaseMs)
	if err != nil {
		return client.ClientConfig{}, err
	}
	backoffMaxMs, err := parsePositiveIntWithDefault("BACKOFF_MAX_MS", defaultBackoffMaxMs)
	if err != nil {
		return client.ClientConfig{}, err
	}
	connectTimeoutMs, err := parsePositiveIntWithDefault("CONNECT_TIMEOUT_MS", defaultConnectTimeoutMs)
	if err != nil {
		return client.ClientConfig{}, err
	}
	queryTimeoutMs, err := parsePositiveIntWithDefault("QUERY_WAIT_TIMEOUT_MS", defaultQueryTimeoutMs)
	if err != nil {
		return client.ClientConfig{}, err
	}

	return client.ClientConfig{
		GatewayPrefix:      gatewayPrefix,
		GatewayAmount:      gatewayAmount,
		GatewayPort:        gatewayPort,
		InputTransactions:  inputTransactions,
		InputAccounts:      inputAccounts,
		BatchMaxBytes:      batchMaxBytes,
		ConnectMaxAttempts: connectMaxAttempts,
		BackoffBase:        time.Duration(backoffBaseMs) * time.Millisecond,
		BackoffMax:         time.Duration(backoffMaxMs) * time.Millisecond,
		ConnectTimeout:     time.Duration(connectTimeoutMs) * time.Millisecond,
		QueryWaitTimeout:   time.Duration(queryTimeoutMs) * time.Millisecond,
	}, nil
}

func parsePositiveIntWithDefault(envName string, defaultValue int) (int, error) {
	raw := os.Getenv(envName)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, errors.New(envName + " must be a positive integer")
	}
	return value, nil
}
