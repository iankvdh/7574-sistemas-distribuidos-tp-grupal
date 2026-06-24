package middleware

type queueMiddleware struct {
	*publisher
	queueName string
}

func newQueueMiddleware(queueName string, settings ConnSettings) (*queueMiddleware, error) {
	p, err := newPublisher(settings)
	if err != nil {
		return nil, err
	}

	if _, err := p.channel.QueueDeclare(
		queueName,
		false, // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return nil, closeChannelAndConnection(p.channel, p.conn, err)
	}

	return &queueMiddleware{publisher: p, queueName: queueName}, nil
}

func (q *queueMiddleware) Send(msg Message) error {
	return q.Publish("", q.queueName, []byte(msg.Body))
}

func (q *queueMiddleware) SendWithKey(msg Message, _ string) error {
	return q.Send(msg)
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	deliveries, err := q.channel.Consume(
		q.queueName, // queue
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

	// Iteramos sobre el channel hasta que se cierre.
	// Con autoAck=false, el broker retiene el mensaje hasta recibir el Ack.
	// Si el proceso muere sin hacer Ack, el mensaje se reencola automáticamente.
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

func (q *queueMiddleware) StopConsuming() error {
	return q.channel.Close()
}
