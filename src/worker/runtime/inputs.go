package runtime

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
)

func BuildInputMiddleware(inputConfig config.InputConfig, conn middleware.ConnSettings) (middleware.Middleware, error) {
	switch inputConfig.Kind {
	case config.KindQueue:
		return middleware.CreateQueueMiddleware(inputConfig.Name, conn)
	case config.KindDirectExchange:
		return middleware.CreateExchangeMiddleware(inputConfig.Name, []string{inputConfig.RoutingKey}, conn)
	default:
		return nil, fmt.Errorf("unsupported input kind for %q", inputConfig.Name)
	}
}
