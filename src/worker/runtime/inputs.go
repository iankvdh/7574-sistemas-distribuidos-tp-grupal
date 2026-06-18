package runtime

import (
	"fmt"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
)

func BuildInputMiddleware(inputConfig config.InputConfig, conn middleware.ConnSettings) (middleware.Middleware, error) {
	switch inputConfig.Kind {
	case config.KindQueue:
		return middleware.CreateQueueMiddleware(inputConfig.Name, conn)
	case config.KindDirectExchange:
		queueName := directExchangeInputQueueName(inputConfig.Name, inputConfig.RoutingKey)
		return middleware.CreateBoundQueueMiddleware(queueName, inputConfig.Name, inputConfig.RoutingKey, conn)
	case config.KindBoundQueue:
		return middleware.CreateBoundQueueMiddleware(inputConfig.Name, inputConfig.Exchange, inputConfig.RoutingKey, conn)
	default:
		return nil, fmt.Errorf("unsupported input kind for %q", inputConfig.Name)
	}
}

func directExchangeInputQueueName(exchange, routingKey string) string {
	name := "direct_" + exchange + "_" + routingKey
	name = strings.NewReplacer(":", "_", "/", "_", "\\", "_", " ", "_").Replace(name)
	return strings.TrimRight(name, "_")
}
