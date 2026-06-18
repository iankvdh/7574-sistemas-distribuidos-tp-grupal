package gateway

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
)

type exchangeMiddlewareFactory func(exchange string, keys []string, settings middleware.ConnSettings) (middleware.Middleware, error)

type gatewayExchangePool struct {
	middlewares []middleware.Middleware
}

func newGatewayExchangePool(exchange string, keys []string, settings middleware.ConnSettings, size int) (*gatewayExchangePool, error) {
	return newGatewayExchangePoolWithFactory(exchange, keys, settings, size, middleware.CreateExchangeMiddleware)
}

func newGatewayExchangePoolWithFactory(exchange string, keys []string, settings middleware.ConnSettings, size int, factory exchangeMiddlewareFactory) (*gatewayExchangePool, error) {
	if size < 1 {
		return nil, fmt.Errorf("gateway exchange pool size must be positive, got %d", size)
	}
	pool := &gatewayExchangePool{middlewares: make([]middleware.Middleware, 0, size)}
	for i := 0; i < size; i++ {
		mw, err := factory(exchange, keys, settings)
		if err != nil {
			_ = pool.Close()
			return nil, fmt.Errorf("create publisher %d/%d for exchange %q: %w", i+1, size, exchange, err)
		}
		pool.middlewares = append(pool.middlewares, mw)
	}
	return pool, nil
}

func (pool *gatewayExchangePool) ForClient(clientID inner.ClientID) middleware.Middleware {
	return pool.middlewares[hashing.Shard(string(clientID), len(pool.middlewares))]
}

func (pool *gatewayExchangePool) Close() error {
	var firstErr error
	for _, mw := range pool.middlewares {
		if mw == nil {
			continue
		}
		if err := mw.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
