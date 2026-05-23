package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultMomPort       = 5672
	defaultBatchMaxBytes = 64 * 1024
)

type WorkerConfig struct {
	StrategyName      string
	ReplicaID         int
	NReplicas         int
	Input             string // raw INPUT env var: queue:NAME or direct_exchange:NAME[:KEY]
	OutputsMatchCount int
	RingQueueIn       string // optional: queue this replica consumes ring tokens from
	RingQueueOut      string // optional: queue this replica publishes ring tokens to (next replica)
	MomHost           string
	MomPort           int
	BatchMaxBytes     int
}

func Load() (WorkerConfig, error) {
	strategyName := strings.TrimSpace(os.Getenv("STRATEGY"))
	if strategyName == "" {
		return WorkerConfig{}, errors.New("STRATEGY environment variable is required")
	}

	replicaID, err := getIntFromEnv("REPLICA_ID", 0, false)
	if err != nil {
		return WorkerConfig{}, err
	}

	nReplicas, err := getIntFromEnv("N_REPLICAS", 1, true)
	if err != nil {
		return WorkerConfig{}, err
	}
	if replicaID < 0 || replicaID >= nReplicas {
		return WorkerConfig{}, fmt.Errorf("REPLICA_ID=%d out of range for N_REPLICAS=%d", replicaID, nReplicas)
	}

	input := strings.TrimSpace(os.Getenv("INPUT"))
	if input == "" {
		return WorkerConfig{}, errors.New("INPUT environment variable is required")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return WorkerConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort := defaultMomPort
	if raw := os.Getenv("MOM_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return WorkerConfig{}, fmt.Errorf("invalid MOM_PORT: %s", raw)
		}
		momPort = parsed
	}

	outputsMatchCount, err := getIntFromEnv("OUTPUTS_MATCH_COUNT", 0, false)
	if err != nil {
		return WorkerConfig{}, err
	}
	if outputsMatchCount < 0 {
		return WorkerConfig{}, errors.New("OUTPUTS_MATCH_COUNT must be non-negative")
	}

	batchMaxBytes, err := getIntFromEnv("BATCH_MAX_BYTES", defaultBatchMaxBytes, true)
	if err != nil {
		return WorkerConfig{}, err
	}

	ringQueueIn := strings.TrimSpace(os.Getenv("RING_QUEUE_IN"))
	ringQueueOut := strings.TrimSpace(os.Getenv("RING_QUEUE_OUT"))
	// Both or neither; specifying one without the other is a misconfiguration.
	if (ringQueueIn == "") != (ringQueueOut == "") {
		return WorkerConfig{}, errors.New("RING_QUEUE_IN and RING_QUEUE_OUT must be set together")
	}
	if nReplicas > 1 && ringQueueIn == "" {
		return WorkerConfig{}, errors.New("RING_QUEUE_IN/RING_QUEUE_OUT are required when N_REPLICAS>1")
	}

	return WorkerConfig{
		StrategyName:      strategyName,
		ReplicaID:         replicaID,
		NReplicas:         nReplicas,
		Input:             input,
		OutputsMatchCount: outputsMatchCount,
		RingQueueIn:       ringQueueIn,
		RingQueueOut:      ringQueueOut,
		MomHost:           momHost,
		MomPort:           momPort,
		BatchMaxBytes:     batchMaxBytes,
	}, nil
}

func getIntFromEnv(name string, defaultValue int, mustBePositive bool) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %s", name, raw)
	}
	if mustBePositive && parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}
