package config

import (
	"strings"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
)

const (
	defaultAllTransactionsExchange = "all_transactions"
	defaultAllAccountsExchange     = "all_accounts"
	defaultFinalQueue              = "final"
	defaultMomPort                 = 5672
	defaultMaxExternalBatchBytes   = 8192
	defaultExpectedQueryEOFs       = 5
	defaultShardCount              = 1
	defaultPublisherPoolSize       = 8
	defaultDataDir                 = "/data"
	defaultClientAbortsExchange    = "client_aborts"
)

type GatewayConfig struct {
	AllTransactionsExchange string
	AllAccountsExchange     string
	NTxShards               int
	NAccountShards          int
	FinalQueue              string
	MaxExternalBatchBytes   int
	ExpectedQueryEOFs       int
	GatewayID               int
	ServerHost              string
	ServerPort              string
	MomHost                 string
	MomPort                 int
	PublisherPoolSize       int
	DataDir                 string
	ClientAbortsExchange    string

	HeartbeatEnabled  bool
	ContainerName     string
	SentinelUDPAddrs  []string
	HeartbeatInterval time.Duration
	HeartbeatJitter   time.Duration
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

	nTxShards, err := env.IntWithDefault("N_TX_SHARDS", defaultShardCount, true)
	if err != nil {
		return GatewayConfig{}, err
	}

	nAccountShards, err := env.IntWithDefault("N_ACCOUNT_SHARDS", defaultShardCount, true)
	if err != nil {
		return GatewayConfig{}, err
	}
	publisherPoolSize, err := env.IntWithDefault("GATEWAY_PUBLISHER_POOL_SIZE", defaultPublisherPoolSize, true)
	if err != nil {
		return GatewayConfig{}, err
	}

	heartbeatEnabled := strings.EqualFold(env.StringWithDefault("HEARTBEAT_ENABLED", "true"), "true")
	containerName := env.StringWithDefault("CONTAINER_NAME", "")
	sentinelUDP := env.SplitNonEmpty(env.StringWithDefault("SENTINEL_UDP", ""))
	hbIntervalSec, err := env.IntWithDefault("HEARTBEAT_INTERVAL_SECONDS", 5, true)
	if err != nil {
		return GatewayConfig{}, err
	}
	hbJitterMs, err := env.IntWithDefault("HEARTBEAT_JITTER_MS", 1000, false)
	if err != nil {
		return GatewayConfig{}, err
	}

	return GatewayConfig{
		AllTransactionsExchange: env.StringWithDefault("ALL_TRANSACTIONS_EXCHANGE", defaultAllTransactionsExchange),
		AllAccountsExchange:     env.StringWithDefault("ALL_ACCOUNTS_EXCHANGE", defaultAllAccountsExchange),
		NTxShards:               nTxShards,
		NAccountShards:          nAccountShards,
		FinalQueue:              env.StringWithDefault("FINAL_QUEUE", defaultFinalQueue),
		MaxExternalBatchBytes:   maxExternalBatchBytes,
		ExpectedQueryEOFs:       expectedQueryEOFs,
		GatewayID:               gatewayID,
		ServerHost:              serverHost,
		ServerPort:              serverPort,
		MomHost:                 momHost,
		MomPort:                 momPort,
		PublisherPoolSize:       publisherPoolSize,
		DataDir:                 env.StringWithDefault("DATA_DIR", defaultDataDir),
		ClientAbortsExchange:    env.StringWithDefault("CLIENT_ABORTS_EXCHANGE", defaultClientAbortsExchange),

		HeartbeatEnabled:  heartbeatEnabled,
		ContainerName:     containerName,
		SentinelUDPAddrs:  sentinelUDP,
		HeartbeatInterval: time.Duration(hbIntervalSec) * time.Second,
		HeartbeatJitter:   time.Duration(hbJitterMs) * time.Millisecond,
	}, nil
}
