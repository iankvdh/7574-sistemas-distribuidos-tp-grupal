package config

import (
	"errors"
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
)

const (
	defaultMomPort               = 5672
	defaultMaxInternalBatchBytes = 65536
)

type WorkerConfig struct {
	StrategyName          string
	ReplicaID             int
	NReplicas             int
	Input                 string // raw INPUT env var
	InputConfig           InputConfig
	Outputs               []OutputConfig
	OutputsMatchCount     int
	RingQueueIn           string // optional: queue this replica consumes ring tokens from
	RingQueueOut          string // optional: queue this replica publishes ring tokens to (next replica)
	MomHost               string
	MomPort               int
	MaxInternalBatchBytes int
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

	input, err := env.RequiredString("INPUT")
	if err != nil {
		return WorkerConfig{}, err
	}

	inputConfig, err := ParseInput(input)
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
	if nReplicas > 1 && ringQueueIn == "" {
		return WorkerConfig{}, errors.New("RING_QUEUE_IN/RING_QUEUE_OUT are required when N_REPLICAS>1")
	}

	return WorkerConfig{
		StrategyName:          strategyName,
		ReplicaID:             replicaID,
		NReplicas:             nReplicas,
		Input:                 input,
		InputConfig:           inputConfig,
		Outputs:               outputs,
		OutputsMatchCount:     outputsMatchCount,
		RingQueueIn:           ringQueueIn,
		RingQueueOut:          ringQueueOut,
		MomHost:               momHost,
		MomPort:               momPort,
		MaxInternalBatchBytes: maxInternalBatchBytes,
	}, nil
}
