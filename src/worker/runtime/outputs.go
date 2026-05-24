package runtime

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
)

type OutputTarget struct {
	Name        string
	Kind        config.OutputKind
	ShardCount  int
	Middlewares []middleware.Middleware
}

func BuildInputMiddleware(binding config.OutputBinding, conn middleware.ConnSettings) (middleware.Middleware, error) {
	switch binding.Kind {
	case config.KindQueue:
		return middleware.CreateQueueMiddleware(binding.Target, conn)
	case config.KindDirectExchange:
		return middleware.CreateExchangeMiddleware(binding.Target, []string{binding.Key}, conn)
	default:
		return nil, fmt.Errorf("unsupported input kind for %q", binding.Name)
	}
}

func BuildOutputTargets(bindings []config.OutputBinding, conn middleware.ConnSettings) ([]OutputTarget, error) {
	targets := make([]OutputTarget, 0, len(bindings))
	closeAll := func() {
		for _, s := range targets {
			for _, mw := range s.Middlewares {
				_ = mw.Close()
			}
		}
	}
	for _, b := range bindings {
		switch b.Kind {
		case config.KindQueue:
			mw, err := middleware.CreateQueueMiddleware(b.Target, conn)
			if err != nil {
				closeAll()
				return nil, fmt.Errorf("open output %q: %w", b.Name, err)
			}
			targets = append(targets, OutputTarget{Name: b.Name, Kind: b.Kind, Middlewares: []middleware.Middleware{mw}})
		case config.KindDirectExchange:
			mw, err := middleware.CreateExchangeMiddleware(b.Target, []string{b.Key}, conn)
			if err != nil {
				closeAll()
				return nil, fmt.Errorf("open output %q: %w", b.Name, err)
			}
			targets = append(targets, OutputTarget{Name: b.Name, Kind: b.Kind, Middlewares: []middleware.Middleware{mw}})
		case config.KindShardedQueues:
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
			targets = append(targets, OutputTarget{Name: b.Name, Kind: b.Kind, ShardCount: b.ShardCount, Middlewares: shards})
		default:
			closeAll()
			return nil, fmt.Errorf("unsupported output kind for %q", b.Name)
		}
	}
	return targets, nil
}
