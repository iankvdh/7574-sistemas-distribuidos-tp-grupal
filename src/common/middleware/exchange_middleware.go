package middleware

type exchangeMiddleware struct {
	*publisher
	exchange    string
	routingKeys []string
}

func newExchangeMiddleware(exchange string, keys []string, settings ConnSettings) (*exchangeMiddleware, error) {
	p, err := newPublisher(settings)
	if err != nil {
		return nil, err
	}

	if err := p.channel.ExchangeDeclare(
		exchange, // nombre del exchange
		"direct", // direct porque queremos rutear por routing key
		false,    // durable
		false,    // auto-delete
		false,    // internal
		false,    // no-wait
		nil,
	); err != nil {
		return nil, closeChannelAndConnection(p.channel, p.conn, err)
	}

	return &exchangeMiddleware{publisher: p, exchange: exchange, routingKeys: keys}, nil
}

func (e *exchangeMiddleware) Send(msg Message) error {
	for _, key := range e.routingKeys {
		if err := e.Publish(e.exchange, key, []byte(msg.Body)); err != nil {
			return err
		}
	}
	return nil
}

func (e *exchangeMiddleware) SendWithKey(msg Message, routingKey string) error {
	return e.Publish(e.exchange, routingKey, []byte(msg.Body))
}

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	q, err := e.channel.QueueDeclare(
		"",    // nombre random
		false, // durable
		true,  // auto-delete
		true,  // exclusive
		false, // no-wait
		nil)
	if err != nil {
		return middlewareError(err)
	}

	// bindeamos la queue al exchange con cada una de las routing keys indicadas.
	for _, key := range e.routingKeys {
		if err := e.channel.QueueBind(q.Name, key, e.exchange, false, nil); err != nil {
			return middlewareError(err)
		}
	}

	deliveries, err := e.channel.Consume(q.Name, "", false, true, false, false, nil)
	if err != nil {
		return middlewareError(err)
	}

	for delivery := range deliveries {
		d := delivery
		callbackFunc(
			Message{Body: string(d.Body)},
			func() { d.Ack(false) },
			func() { d.Nack(false, true) },
		)
	}

	return ErrMessageMiddlewareDisconnected
}

func (e *exchangeMiddleware) StopConsuming() error {
	return e.channel.Close()
}
