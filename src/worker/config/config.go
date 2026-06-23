package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
)

const (
	defaultMomPort               = 5672
	defaultMaxInternalBatchBytes = 65536
	defaultDataDir               = "/data"
	defaultCheckpointInterval    = 64
	defaultTombstoneTTLSeconds   = 3600
	defaultClientAbortsExchange  = "client_aborts"
	defaultAMQPCloseTimeoutSecs  = 5
	defaultMaintenanceIntervalS  = 1
	defaultMaxPendingAckAgeS     = 10
	defaultWALCompactionFactor   = 2
)

type WorkerConfig struct {
	StrategyName string
	ReplicaID    int
	NReplicas    int
	InputConfigs []InputConfig
	Outputs      []OutputConfig

	// number of outputs that should receive matching transactions; the rest receive non-matching ones
	OutputsMatchCount     int
	RingQueueIn           string // optional: queue this replica consumes ring tokens from
	RingQueueOut          string // optional: queue this replica publishes ring tokens to (next replica)
	MomHost               string
	MomPort               int
	MaxInternalBatchBytes int
	StageType             uint8
	DataDir               string
	CheckpointDir         string
	ClientAbortsExchange  string
	CheckpointInterval    int
	WALCompactionFactor   int
	WALEnabled            bool
	TombstoneTTL          time.Duration
	AMQPCloseTimeout      time.Duration
	MaintenanceInterval   time.Duration
	MaxPendingAckAge      time.Duration

	// Heartbeat (Sentinel liveness).
	HeartbeatEnabled  bool
	ContainerName     string
	SentinelUDPAddrs  []string
	HeartbeatInterval time.Duration
	HeartbeatJitter   time.Duration
}

func Load() (WorkerConfig, error) {
	strategyName, err := env.RequiredString("STRATEGY")
	if err != nil {
		return WorkerConfig{}, err
	}

	replicaID, err := env.IntWithDefault("REPLICA_ID", 0, false)
	if err != nil {
		return WorkerConfig{}, err
	}

	nReplicas, err := env.IntWithDefault("N_REPLICAS", 1, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	if replicaID < 0 || replicaID >= nReplicas {
		return WorkerConfig{}, fmt.Errorf("REPLICA_ID=%d out of range for N_REPLICAS=%d", replicaID, nReplicas)
	}

	stageTypeName, err := env.RequiredString("STAGE_TYPE")
	if err != nil {
		return WorkerConfig{}, err
	}
	stageType, err := inner.StageTypeFromName(stageTypeName)
	if err != nil {
		return WorkerConfig{}, err
	}

	dataDir := env.StringWithDefault("DATA_DIR", defaultDataDir)
	checkpointDir := filepath.Join(dataDir, fmt.Sprintf("%d_%d", stageType, replicaID))

	checkpointInterval, err := env.IntWithDefault("CHECKPOINT_INTERVAL", defaultCheckpointInterval, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	if checkpointInterval > middleware.PrefetchCount {
		return WorkerConfig{}, fmt.Errorf(
			"CHECKPOINT_INTERVAL (%d) must be <= PREFETCH_COUNT (%d)",
			checkpointInterval, middleware.PrefetchCount)
	}

	walCompactionFactor, err := env.IntWithDefault("WAL_COMPACTION_FACTOR", defaultWALCompactionFactor, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	if walCompactionFactor < 1 {
		walCompactionFactor = defaultWALCompactionFactor
	}
	walEnabled := env.StringWithDefault("WAL_ENABLED", "true") != "false"

	tombstoneTTLSecs, err := env.IntWithDefault("TOMBSTONE_TTL_SECONDS", defaultTombstoneTTLSeconds, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	amqpCloseTimeoutSecs, err := env.IntWithDefault("AMQP_CLOSE_TIMEOUT_SECONDS", defaultAMQPCloseTimeoutSecs, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	maintenanceIntervalSecs, err := env.IntWithDefault("MAINTENANCE_INTERVAL_SECONDS", defaultMaintenanceIntervalS, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	maxPendingAckAgeSecs, err := env.IntWithDefault("MAX_PENDING_ACK_AGE_SECONDS", defaultMaxPendingAckAgeS, true)
	if err != nil {
		return WorkerConfig{}, err
	}

	inputConfigs, err := ParseInputs()
	if err != nil {
		return WorkerConfig{}, err
	}

	outputs, err := ParseOutputs()
	if err != nil {
		return WorkerConfig{}, err
	}

	momHost, err := env.RequiredString("MOM_HOST")
	if err != nil {
		return WorkerConfig{}, err
	}

	momPort, err := env.IntWithDefault("MOM_PORT", defaultMomPort, true)
	if err != nil {
		return WorkerConfig{}, err
	}

	outputsMatchCount, err := env.IntWithDefault("OUTPUTS_MATCH_COUNT", 0, false)
	if err != nil {
		return WorkerConfig{}, err
	}
	if outputsMatchCount < 0 {
		return WorkerConfig{}, errors.New("OUTPUTS_MATCH_COUNT must be non-negative")
	}
	if outputsMatchCount > len(outputs) {
		return WorkerConfig{}, errors.New("OUTPUTS_MATCH_COUNT exceeds number of OUTPUT_* outputConfigs")
	}

	maxInternalBatchBytes, err := env.IntWithDefault("MAX_INTERNAL_BATCH_BYTES", defaultMaxInternalBatchBytes, true)
	if err != nil {
		return WorkerConfig{}, err
	}

	ringQueueIn := env.StringWithDefault("RING_QUEUE_IN", "")
	ringQueueOut := env.StringWithDefault("RING_QUEUE_OUT", "")
	// Both or neither; specifying one without the other is a misconfiguration.
	if (ringQueueIn == "") != (ringQueueOut == "") {
		return WorkerConfig{}, errors.New("RING_QUEUE_IN and RING_QUEUE_OUT must be set together")
	}

	heartbeatEnabled := strings.EqualFold(env.StringWithDefault("HEARTBEAT_ENABLED", "true"), "true")
	containerName := env.StringWithDefault("CONTAINER_NAME", "")
	sentinelUDP := env.SplitNonEmpty(env.StringWithDefault("SENTINEL_UDP", ""))
	hbIntervalSec, err := env.IntWithDefault("HEARTBEAT_INTERVAL_SECONDS", 5, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	hbJitterMs, err := env.IntWithDefault("HEARTBEAT_JITTER_MS", 1000, false)
	if err != nil {
		return WorkerConfig{}, err
	}

	return WorkerConfig{
		StrategyName:          strategyName,
		ReplicaID:             replicaID,
		NReplicas:             nReplicas,
		InputConfigs:          inputConfigs,
		Outputs:               outputs,
		OutputsMatchCount:     outputsMatchCount,
		RingQueueIn:           ringQueueIn,
		RingQueueOut:          ringQueueOut,
		MomHost:               momHost,
		MomPort:               momPort,
		MaxInternalBatchBytes: maxInternalBatchBytes,

		StageType:            stageType,
		DataDir:              dataDir,
		CheckpointDir:        checkpointDir,
		ClientAbortsExchange: env.StringWithDefault("CLIENT_ABORTS_EXCHANGE", defaultClientAbortsExchange),
		CheckpointInterval:   checkpointInterval,
		WALCompactionFactor:  walCompactionFactor,
		WALEnabled:           walEnabled,
		TombstoneTTL:         time.Duration(tombstoneTTLSecs) * time.Second,
		AMQPCloseTimeout:    time.Duration(amqpCloseTimeoutSecs) * time.Second,
		MaintenanceInterval: time.Duration(maintenanceIntervalSecs) * time.Second,
		MaxPendingAckAge:    time.Duration(maxPendingAckAgeSecs) * time.Second,

		HeartbeatEnabled:  heartbeatEnabled,
		ContainerName:     containerName,
		SentinelUDPAddrs:  sentinelUDP,
		HeartbeatInterval: time.Duration(hbIntervalSec) * time.Second,
		HeartbeatJitter:   time.Duration(hbJitterMs) * time.Millisecond,
	}, nil
}
