package topology

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
)

// OutputKind enumerates the supported destination types for a strategy output.
type OutputKind int

const (
	KindQueue OutputKind = iota + 1
	// KindDirectExchange: a RabbitMQ direct exchange. Producers publish with the
	// configured routing key (empty by default). Multiple consumers can each bind
	// their own private queue to the same exchange and receive a copy.
	KindDirectExchange
	// KindShardedQueues: K named queues (Target_0 ... Target_{K-1}). The producer
	// publishes each message to the queue chosen by hashing the client_id, so that
	// the consumer indexed by that shard receives all data and EOFs for that
	// client. Consumers consume directly from queue:Target_i (resolved by the
	// generator from REPLICA_ID).
	KindShardedQueues
)

// OutputBinding describes one publishing target declared by the worker.
//
//	queue:NAME                       → KindQueue
//	direct_exchange:NAME             → KindDirectExchange, routing key ""
//	direct_exchange:NAME:KEY         → KindDirectExchange, routing key KEY
//	sharded_queues:PREFIX:K          → KindShardedQueues, K named queues PREFIX_0..PREFIX_{K-1}
type OutputBinding struct {
	Name       string
	Kind       OutputKind
	Target     string
	Key        string
	ShardCount int
}

// OutputSink groups the binding with the live middlewares that implement it.
// For KindQueue and KindDirectExchange Middlewares has exactly 1 entry; for
// KindShardedQueues it has ShardCount entries (Middlewares[i] corresponds to
// queue PREFIX_i).
type OutputSink struct {
	Name        string
	Kind        OutputKind
	ShardCount  int
	Middlewares []middleware.Middleware
}

// ParseOutputs reads OUTPUT_0, OUTPUT_1, ... env vars in order and returns the parsed bindings.
// It stops at the first index that is absent.
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
		// sharded_queues:PREFIX:K
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

// ParseInput parses the INPUT env var into an OutputBinding-shaped descriptor.
// Inputs are always single queues or direct exchanges; the generator resolves a
// joiner replica's shard by setting INPUT=queue:PREFIX_{REPLICA_ID} so the
// sharded-queues kind does not appear here.
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

// BuildInputMiddleware opens the middleware the worker will consume from.
func BuildInputMiddleware(binding OutputBinding, conn middleware.ConnSettings) (middleware.Middleware, error) {
	switch binding.Kind {
	case KindQueue:
		return middleware.CreateQueueMiddleware(binding.Target, conn)
	case KindDirectExchange:
		return middleware.CreateExchangeMiddleware(binding.Target, []string{binding.Key}, conn)
	default:
		return nil, fmt.Errorf("unsupported input kind for %q", binding.Name)
	}
}

// BuildOutputSinks opens the middleware(s) backing each binding.
func BuildOutputSinks(bindings []OutputBinding, conn middleware.ConnSettings) ([]OutputSink, error) {
	sinks := make([]OutputSink, 0, len(bindings))
	closeAll := func() {
		for _, s := range sinks {
			for _, mw := range s.Middlewares {
				_ = mw.Close()
			}
		}
	}
	for _, b := range bindings {
		switch b.Kind {
		case KindQueue:
			mw, err := middleware.CreateQueueMiddleware(b.Target, conn)
			if err != nil {
				closeAll()
				return nil, fmt.Errorf("open output %q: %w", b.Name, err)
			}
			sinks = append(sinks, OutputSink{Name: b.Name, Kind: b.Kind, Middlewares: []middleware.Middleware{mw}})
		case KindDirectExchange:
			mw, err := middleware.CreateExchangeMiddleware(b.Target, []string{b.Key}, conn)
			if err != nil {
				closeAll()
				return nil, fmt.Errorf("open output %q: %w", b.Name, err)
			}
			sinks = append(sinks, OutputSink{Name: b.Name, Kind: b.Kind, Middlewares: []middleware.Middleware{mw}})
		case KindShardedQueues:
			if b.ShardCount <= 0 {
				closeAll()
				return nil, fmt.Errorf("output %q: sharded_queues requires ShardCount>0", b.Name)
			}
			shards := make([]middleware.Middleware, 0, b.ShardCount)
			for i := 0; i < b.ShardCount; i++ {
				qname := fmt.Sprintf("%s_%d", b.Target, i)
				mw, err := middleware.CreateQueueMiddleware(qname, conn)
				if err != nil {
					for _, m := range shards {
						_ = m.Close()
					}
					closeAll()
					return nil, fmt.Errorf("open output %q shard %d: %w", b.Name, i, err)
				}
				shards = append(shards, mw)
			}
			sinks = append(sinks, OutputSink{Name: b.Name, Kind: b.Kind, ShardCount: b.ShardCount, Middlewares: shards})
		default:
			closeAll()
			return nil, fmt.Errorf("unsupported output kind for %q", b.Name)
		}
	}
	return sinks, nil
}
