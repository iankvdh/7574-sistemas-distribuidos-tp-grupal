package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	return newQueueMiddleware(queueName, connectionSettings)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	return newExchangeMiddleware(exchange, keys, connectionSettings)
}

func CreateBestEffortExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	mw, err := newExchangeMiddleware(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}
	mw.publisher.mandatory = false
	return mw, nil
}

func CreateBoundQueueMiddleware(queueName, exchange, routingKey string, connectionSettings ConnSettings) (Middleware, error) {
	return newBoundQueueMiddleware(queueName, exchange, routingKey, connectionSettings)
}
