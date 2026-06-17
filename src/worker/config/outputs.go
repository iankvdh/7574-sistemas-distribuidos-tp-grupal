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
	// KindRoundRobinQueues fans messages out to K named queues (PREFIX_0..PREFIX_{K-1}),
	// choosing the destination by a per-client counter that increments on each data batch.
	// The counter is persisted in the checkpoint so routing is deterministic across crashes.
	// Used for stateless downstream stages where uniform distribution matters more than affinity.
	KindRoundRobinQueues
	// KindFinalQueue is a regular AMQP queue, but the runtime serializes payloads
	// addressed at it as FinalQueryResultBatch envelopes (instead of InnerBatch),
	// so the gateway's final-queue consumer can pick them up unchanged. Used by
	// the final_joiner strategy to publish per-client query results to
	// `final_<gatewayID>`.
	KindFinalQueue
	// KindBoundQueue is a named, non-exclusive, non-auto-delete queue bound to a
	// direct exchange with a fixed routing key. Unlike KindDirectExchange (which
	// consumes via an anonymous auto-delete queue), it survives a consumer crash
	// so un-acked messages are redelivered on recovery. Input-only.
	KindBoundQueue
)

type OutputConfig struct {
	Name       string
	Kind       OutputKind
	RoutingKey string
	ShardCount int
	Exchange   string // KindBoundQueue: exchange the queue is bound to
}

func ParseOutputs() ([]OutputConfig, error) {
	indices := collectOutputIndices()
	if len(indices) == 0 {
		return nil, nil
	}
	outputConfigs := make([]OutputConfig, 0, len(indices))
	for _, i := range indices {
		raw := os.Getenv(fmt.Sprintf("OUTPUT_%d", i))
		outputConfig, err := parseOutputConfig(raw, i)
		if err != nil {
			return nil, err
		}
		outputConfigs = append(outputConfigs, outputConfig)
	}
	return outputConfigs, nil
}

func parseOutputConfig(raw string, idx int) (OutputConfig, error) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid: %q (expected <kind>:<target>[:<extra>])", idx, raw)
	}
	kind, rest := parts[0], parts[1]
	switch kind {
	case "queue":
		return OutputConfig{Name: rest, Kind: KindQueue}, nil
	case "direct_exchange":
		exchangeName, routingKey := rest, ""
		if subParts := strings.SplitN(rest, ":", 2); len(subParts) == 2 {
			exchangeName = subParts[0]
			routingKey = subParts[1]
		}
		if exchangeName == "" {
			return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid direct_exchange target", idx)
		}
		return OutputConfig{Name: exchangeName, Kind: KindDirectExchange, RoutingKey: routingKey}, nil
	case "sharded_queues":
		subParts := strings.SplitN(rest, ":", 2)
		if len(subParts) != 2 || subParts[0] == "" || subParts[1] == "" {
			return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid sharded_queues config: %q (expected sharded_queues:PREFIX:K)", idx, raw)
		}
		prefix, kRaw := subParts[0], subParts[1]
		K, err := strconv.Atoi(kRaw)
		if err != nil || K <= 0 {
			return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid shard count in %q", idx, raw)
		}
		return OutputConfig{Name: prefix, Kind: KindShardedQueues, ShardCount: K}, nil
	case "round_robin_queues":
		subParts := strings.SplitN(rest, ":", 2)
		if len(subParts) != 2 || subParts[0] == "" || subParts[1] == "" {
			return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid round_robin_queues config: %q (expected round_robin_queues:PREFIX:K)", idx, raw)
		}
		prefix, kRaw := subParts[0], subParts[1]
		K, err := strconv.Atoi(kRaw)
		if err != nil || K <= 0 {
			return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid shard count in %q", idx, raw)
		}
		return OutputConfig{Name: prefix, Kind: KindRoundRobinQueues, ShardCount: K}, nil
	case "final_queue":
		if rest == "" {
			return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid final_queue target", idx)
		}
		return OutputConfig{Name: rest, Kind: KindFinalQueue}, nil
	case "bound_queue":
		subParts := strings.SplitN(rest, ":", 3)
		if len(subParts) != 3 || subParts[0] == "" || subParts[1] == "" {
			return OutputConfig{}, fmt.Errorf("OUTPUT_%d invalid bound_queue config: %q (expected bound_queue:QUEUE:EXCHANGE:ROUTING_KEY)", idx, raw)
		}
		return OutputConfig{Name: subParts[0], Kind: KindBoundQueue, Exchange: subParts[1], RoutingKey: subParts[2]}, nil
	default:
		return OutputConfig{}, fmt.Errorf("OUTPUT_%d unsupported kind %q", idx, kind)
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
