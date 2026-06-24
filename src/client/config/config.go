package config

import (
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/client/client"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
)

const (
	defaultConnectMaxAttempts         = 6
	defaultBackoffBaseMs              = 200
	defaultBackoffMaxMs               = 3000
	defaultConnectTimeoutMs           = 800
	defaultMaxExternalBatchBytes      = 8192
	defaultResultsDir                 = "results"
	defaultExpectedQueryEOFs          = 5
	defaultReconnectMaxElapsedSeconds = 180
)

func Load() (client.ClientConfig, error) {
	gatewayPrefix, err := env.RequiredString("GATEWAY_PREFIX")
	if err != nil {
		return client.ClientConfig{}, err
	}

	gatewayAmount, err := env.RequiredInt("GATEWAY_AMOUNT", true)
	if err != nil {
		return client.ClientConfig{}, err
	}

	gatewayPort, err := env.RequiredString("GATEWAY_PORT")
	if err != nil {
		return client.ClientConfig{}, err
	}

	inputTransactions, err := env.RequiredString("INPUT_TRANSACTIONS")
	if err != nil {
		return client.ClientConfig{}, err
	}

	inputAccounts, err := env.RequiredString("INPUT_ACCOUNTS")
	if err != nil {
		return client.ClientConfig{}, err
	}

	maxExternalBatchBytes, err := env.IntWithDefault("MAX_EXTERNAL_BATCH_BYTES", defaultMaxExternalBatchBytes, true)
	if err != nil {
		return client.ClientConfig{}, err
	}

	connectMaxAttempts, err := env.IntWithDefault("CONNECT_MAX_ATTEMPTS", defaultConnectMaxAttempts, true)
	if err != nil {
		return client.ClientConfig{}, err
	}
	backoffBaseMs, err := env.IntWithDefault("BACKOFF_BASE_MS", defaultBackoffBaseMs, true)
	if err != nil {
		return client.ClientConfig{}, err
	}
	backoffMaxMs, err := env.IntWithDefault("BACKOFF_MAX_MS", defaultBackoffMaxMs, true)
	if err != nil {
		return client.ClientConfig{}, err
	}
	connectTimeoutMs, err := env.IntWithDefault("CONNECT_TIMEOUT_MS", defaultConnectTimeoutMs, true)
	if err != nil {
		return client.ClientConfig{}, err
	}
	reconnectMaxElapsedSecs, err := env.IntWithDefault("RECONNECT_MAX_ELAPSED_SECONDS", defaultReconnectMaxElapsedSeconds, true)
	if err != nil {
		return client.ClientConfig{}, err
	}

	expectedQueryEOFs, err := env.IntWithDefault("EXPECTED_QUERY_EOFS", defaultExpectedQueryEOFs, true)
	if err != nil {
		return client.ClientConfig{}, err
	}

	return client.ClientConfig{
		GatewayPrefix:         gatewayPrefix,
		GatewayAmount:         gatewayAmount,
		GatewayPort:           gatewayPort,
		InputTransactions:     inputTransactions,
		InputAccounts:         inputAccounts,
		ResultsDir:            env.StringWithDefault("RESULTS_DIR", defaultResultsDir),
		MaxExternalBatchBytes: maxExternalBatchBytes,
		ExpectedQueryEOFs:     expectedQueryEOFs,
		ConnectMaxAttempts:    connectMaxAttempts,
		BackoffBase:           time.Duration(backoffBaseMs) * time.Millisecond,
		BackoffMax:            time.Duration(backoffMaxMs) * time.Millisecond,
		ConnectTimeout:        time.Duration(connectTimeoutMs) * time.Millisecond,
		ReconnectMaxElapsed:   time.Duration(reconnectMaxElapsedSecs) * time.Second,
	}, nil
}
