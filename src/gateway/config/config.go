package config

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
)

const (
	defaultAllTransactionsQueue = "all_transactions"
	defaultAllAccountsQueue     = "all_accounts"
	defaultFinalQueue           = "final"
	defaultMomPort              = 5672
	defaultResultBatchMaxBytes  = 8192
)

type GatewayConfig struct {
	AllTransactionsQueue string
	AllAccountsQueue     string
	FinalQueue           string
	ResultBatchMaxBytes  int
	GatewayID            int
	ServerHost           string
	ServerPort           string
	MomHost              string
	MomPort              int
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

	resultBatchMaxBytes, err := env.IntWithDefault("RESULT_BATCH_MAX_BYTES", defaultResultBatchMaxBytes, true)
	if err != nil {
		return GatewayConfig{}, err
	}

	return GatewayConfig{
		AllTransactionsQueue: env.StringWithDefault("ALL_TRANSACTIONS_QUEUE", defaultAllTransactionsQueue),
		AllAccountsQueue:     env.StringWithDefault("ALL_ACCOUNTS_QUEUE", defaultAllAccountsQueue),
		FinalQueue:           env.StringWithDefault("FINAL_QUEUE", defaultFinalQueue),
		ResultBatchMaxBytes:  resultBatchMaxBytes,
		GatewayID:            gatewayID,
		ServerHost:           serverHost,
		ServerPort:           serverPort,
		MomHost:              momHost,
		MomPort:              momPort,
	}, nil
}
