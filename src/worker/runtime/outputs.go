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

func BuildOutputTargets(outputConfigs []config.OutputConfig, conn middleware.ConnSettings) ([]OutputTarget, error) {
	targets := make([]OutputTarget, 0, len(outputConfigs))

	for _, outputConfig := range outputConfigs {
		switch outputConfig.Kind {
		case config.KindQueue:
			mw, err := middleware.CreateQueueMiddleware(outputConfig.Name, conn)
			if err != nil {
				closeOutputTargets(targets)
				return nil, fmt.Errorf("open output %q: %w", outputConfig.Name, err)
			}
			targets = append(targets, OutputTarget{Name: outputConfig.Name, Kind: outputConfig.Kind, Middlewares: []middleware.Middleware{mw}})
		case config.KindDirectExchange:
			mw, err := middleware.CreateExchangeMiddleware(outputConfig.Name, []string{outputConfig.RoutingKey}, conn)
			if err != nil {
				closeOutputTargets(targets)
				return nil, fmt.Errorf("open output %q: %w", outputConfig.Name, err)
			}
			targets = append(targets, OutputTarget{Name: outputConfig.Name, Kind: outputConfig.Kind, Middlewares: []middleware.Middleware{mw}})
		case config.KindShardedQueues:
			if outputConfig.ShardCount <= 0 {
				closeOutputTargets(targets)
				return nil, fmt.Errorf("output %q: sharded_queues requires ShardCount>0", outputConfig.Name)
			}
			shards := make([]middleware.Middleware, 0, outputConfig.ShardCount)
			for i := 0; i < outputConfig.ShardCount; i++ {
				qname := fmt.Sprintf("%s_%d", outputConfig.Name, i)
				mw, err := middleware.CreateQueueMiddleware(qname, conn)
				if err != nil {
					for _, m := range shards {
						_ = m.Close()
					}
					closeOutputTargets(targets)
					return nil, fmt.Errorf("open output %q shard %d: %w", outputConfig.Name, i, err)
				}
				shards = append(shards, mw)
			}
			targets = append(targets, OutputTarget{Name: outputConfig.Name, Kind: outputConfig.Kind, ShardCount: outputConfig.ShardCount, Middlewares: shards})
		default:
			closeOutputTargets(targets)
			return nil, fmt.Errorf("unsupported output kind for %q", outputConfig.Name)
		}
	}
	return targets, nil
}

func closeOutputTargets(targets []OutputTarget) {
	for _, t := range targets {
		for _, m := range t.Middlewares {
			_ = m.Close()
		}
	}
}
