package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type InputConfig struct {
	Name       string
	Kind       OutputKind
	RoutingKey string
	Exchange   string
}

func ParseInput(raw string) (InputConfig, error) {
	if raw == "" {
		return InputConfig{}, fmt.Errorf("INPUT environment variable is required")
	}
	outputConfig, err := parseOutputConfig(raw, -1)
	if err != nil {
		return InputConfig{}, err
	}
	if outputConfig.Kind == KindShardedQueues {
		return InputConfig{}, fmt.Errorf("INPUT does not accept sharded_queues; consume a specific queue:PREFIX_i")
	}
	return InputConfig{
		Name:       outputConfig.Name,
		Kind:       outputConfig.Kind,
		RoutingKey: outputConfig.RoutingKey,
		Exchange:   outputConfig.Exchange,
	}, nil
}

func ParseInputs() ([]InputConfig, error) {
	indices := collectInputIndices()
	legacy := os.Getenv("INPUT")

	if len(indices) > 0 && legacy != "" {
		return nil, fmt.Errorf("mixing INPUT and INPUT_N is not allowed; use INPUT_N exclusively")
	}

	if len(indices) == 0 {
		cfg, err := ParseInput(legacy)
		if err != nil {
			return nil, err
		}
		return []InputConfig{cfg}, nil
	}

	configs := make([]InputConfig, 0, len(indices))
	for _, i := range indices {
		raw := os.Getenv(fmt.Sprintf("INPUT_%d", i))
		cfg, err := ParseInput(raw)
		if err != nil {
			return nil, fmt.Errorf("INPUT_%d: %w", i, err)
		}
		configs = append(configs, cfg)
	}
	return configs, nil
}

func collectInputIndices() []int {
	const prefix = "INPUT_"
	indices := make([]int, 0, 4)
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, prefix) {
			continue
		}
		eq := strings.IndexByte(e, '=')
		if eq == -1 {
			continue
		}
		name := e[:eq]
		suffix := strings.TrimPrefix(name, prefix)
		if suffix == "" {
			continue
		}
		idx, err := strconv.Atoi(suffix)
		if err != nil || idx < 0 {
			continue
		}
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	return indices
}
