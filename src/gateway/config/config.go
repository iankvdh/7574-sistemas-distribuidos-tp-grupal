package config

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
)

const (
	defaultAllTransactionsQueue  = "all_transactions"
	defaultAllAccountsQueue      = "all_accounts"
	defaultFinalQueue            = "final"
	defaultMomPort               = 5672
	defaultMaxExternalBatchBytes = 8192
	defaultExpectedQueryEOFs     = 5
)

type GatewayConfig struct {
	AllTransactionsQueue  string
	AllAccountsQueue      string
	FinalQueue            string
	MaxExternalBatchBytes int
	ExpectedQueryEOFs     int
	GatewayID             int
	ServerHost            string
	ServerPort            string
	MomHost               string
	MomPort               int
}

func Load() (GatewayConfig, error) {
	serverHost, err := env.RequiredString("SERVER_HOST")
	if err != nil {
		return GatewayConfig{}, err
	}

	serverPort, err := env.RequiredString("SERVER_PORT")
	if err != nil {
		return GatewayConfig{}, err
	}

	gatewayID, err := env.RequiredInt("GATEWAY_ID", true)
	if err != nil {
		return GatewayConfig{}, err
	}

	momHost, err := env.RequiredString("MOM_HOST")
	if err != nil {
		return GatewayConfig{}, err
	}

	momPort, err := env.IntWithDefault("MOM_PORT", defaultMomPort, true)
	if err != nil {
		return GatewayConfig{}, err
	}

	maxExternalBatchBytes, err := env.IntWithDefault("MAX_EXTERNAL_BATCH_BYTES", defaultMaxExternalBatchBytes, true)
	if err != nil {
		return GatewayConfig{}, err
	}

	expectedQueryEOFs, err := env.IntWithDefault("EXPECTED_QUERY_EOFS", defaultExpectedQueryEOFs, true)
	if err != nil {
		return GatewayConfig{}, err
	}

	return GatewayConfig{
		AllTransactionsQueue:  env.StringWithDefault("ALL_TRANSACTIONS_QUEUE", defaultAllTransactionsQueue),
		AllAccountsQueue:      env.StringWithDefault("ALL_ACCOUNTS_QUEUE", defaultAllAccountsQueue),
		FinalQueue:            env.StringWithDefault("FINAL_QUEUE", defaultFinalQueue),
		MaxExternalBatchBytes: maxExternalBatchBytes,
		ExpectedQueryEOFs:     expectedQueryEOFs,
		GatewayID:             gatewayID,
		ServerHost:            serverHost,
		ServerPort:            serverPort,
		MomHost:               momHost,
		MomPort:               momPort,
	}, nil
}
