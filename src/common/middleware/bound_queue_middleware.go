package middleware

type boundQueueMiddleware struct {
	*publisher
	exchange   string
	queueName  string
	routingKey string
}

func newBoundQueueMiddleware(queueName, exchange, routingKey string, settings ConnSettings) (*boundQueueMiddleware, error) {
	p, err := newPublisher(settings)
	if err != nil {
		return nil, err
	}
	ch := p.channel

	if err := ch.ExchangeDeclare(
		exchange,
		"direct",
		false, // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return nil, closeChannelAndConnection(ch, p.conn, err)
	}

	if _, err := ch.QueueDeclare(
		queueName,
		false, // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return nil, closeChannelAndConnection(ch, p.conn, err)
	}

	if err := ch.QueueBind(queueName, routingKey, exchange, false, nil); err != nil {
		return nil, closeChannelAndConnection(ch, p.conn, err)
	}

	return &boundQueueMiddleware{
		publisher:  p,
		exchange:   exchange,
		queueName:  queueName,
		routingKey: routingKey,
	}, nil
}

func (b *boundQueueMiddleware) Send(msg Message) error {
	return b.Publish(b.exchange, b.routingKey, []byte(msg.Body))
}

func (b *boundQueueMiddleware) SendWithKey(msg Message, routingKey string) error {
	return b.Publish(b.exchange, routingKey, []byte(msg.Body))
}

func (b *boundQueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	deliveries, err := b.channel.Consume(
		b.queueName, // queue
		"",          // random consumer tag
		false,       // no-auto-ack
		false,       // no-exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
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

func (b *boundQueueMiddleware) StopConsuming() error {
	return b.channel.Close()
}
