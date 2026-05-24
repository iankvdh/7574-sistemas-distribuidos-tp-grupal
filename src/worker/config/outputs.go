package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type OutputKind int

const (
	KindQueue OutputKind = iota + 1
	KindDirectExchange
	// KindShardedQueues fans messages out to K named queues (PREFIX_0..PREFIX_{K-1}),
	// choosing the destination by hashing client_id so each client sticks to one shard.
	KindShardedQueues
)

type OutputBinding struct {
	Name       string
	Kind       OutputKind
	Target     string
	Key        string
	ShardCount int
}

func ParseOutputs() ([]OutputBinding, error) {
	indices := collectOutputIndices()
	if len(indices) == 0 {
		return nil, nil
	}
	bindings := make([]OutputBinding, 0, len(indices))
	for _, i := range indices {
		raw := os.Getenv(fmt.Sprintf("OUTPUT_%d", i))
		binding, err := parseBinding(raw, i)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func parseBinding(raw string, idx int) (OutputBinding, error) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return OutputBinding{}, fmt.Errorf("OUTPUT_%d invalid: %q (expected <kind>:<target>[:<extra>])", idx, raw)
	}
	kind, rest := parts[0], parts[1]
	switch kind {
	case "queue":
		return OutputBinding{Name: rest, Kind: KindQueue, Target: rest}, nil
	case "direct_exchange":
		exchangeName, key := rest, ""
		if subParts := strings.SplitN(rest, ":", 2); len(subParts) == 2 {
			exchangeName = subParts[0]
			key = subParts[1]
		}
		if exchangeName == "" {
			return OutputBinding{}, fmt.Errorf("OUTPUT_%d invalid direct_exchange target", idx)
		}
		return OutputBinding{Name: exchangeName, Kind: KindDirectExchange, Target: exchangeName, Key: key}, nil
	case "sharded_queues":
		subParts := strings.SplitN(rest, ":", 2)
		if len(subParts) != 2 || subParts[0] == "" || subParts[1] == "" {
			return OutputBinding{}, fmt.Errorf("OUTPUT_%d invalid sharded_queues binding: %q (expected sharded_queues:PREFIX:K)", idx, raw)
		}
		prefix, kRaw := subParts[0], subParts[1]
		K, err := strconv.Atoi(kRaw)
		if err != nil || K <= 0 {
			return OutputBinding{}, fmt.Errorf("OUTPUT_%d invalid shard count in %q", idx, raw)
		}
		return OutputBinding{Name: prefix, Kind: KindShardedQueues, Target: prefix, ShardCount: K}, nil
	default:
		return OutputBinding{}, fmt.Errorf("OUTPUT_%d unsupported kind %q", idx, kind)
	}
}

func collectOutputIndices() []int {
	const prefix = "OUTPUT_"
	indices := make([]int, 0, 4)
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, prefix) {
			continue
		}
		eq := strings.IndexByte(env, '=')
		if eq == -1 {
			continue
		}
		name := env[:eq]
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

func ParseInput(raw string) (OutputBinding, error) {
	if raw == "" {
		return OutputBinding{}, fmt.Errorf("INPUT environment variable is required")
	}
	b, err := parseBinding(raw, -1)
	if err != nil {
		return OutputBinding{}, err
	}
	if b.Kind == KindShardedQueues {
		return OutputBinding{}, fmt.Errorf("INPUT does not accept sharded_queues; consume a specific queue:PREFIX_i")
	}
	return b, nil
}
