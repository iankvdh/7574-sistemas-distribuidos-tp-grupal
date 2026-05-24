package config

import "fmt"

type InputConfig struct {
	Name       string
	Kind       OutputKind
	RoutingKey string
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
	}, nil
}
