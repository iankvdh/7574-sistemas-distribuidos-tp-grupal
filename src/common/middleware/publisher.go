package middleware

import (
	"fmt"
	"log"
	"log/slog"
	"math"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultMaxOutstandingPublishes = 4096
	DefaultFlushHighWaterRatio     = 0.80
	DefaultHeartbeatSeconds        = 10
	connectBackoffInitial          = 500 * time.Millisecond
	connectBackoffMax              = 30 * time.Second
)

type publisher struct {
	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel

	confirms        chan amqp.Confirmation
	pendingConfirms int
	flushHighWater  int
}

func newPublisher(settings ConnSettings) (*publisher, error) {
	maxOutstanding := settings.MaxOutstandingPublishes
	if maxOutstanding <= 0 {
		maxOutstanding = DefaultMaxOutstandingPublishes
	}
	if maxOutstanding < PrefetchCount {
		return nil, fmt.Errorf(
			"middleware: MAX_OUTSTANDING_PUBLISHES (%d) must be >= PREFETCH_COUNT (%d)",
			maxOutstanding, PrefetchCount)
	}
	ratio := settings.FlushHighWaterRatio
	if ratio <= 0 {
		ratio = DefaultFlushHighWaterRatio
	}
	heartbeatSeconds := settings.HeartbeatSeconds
	if heartbeatSeconds <= 0 {
		heartbeatSeconds = DefaultHeartbeatSeconds
	}

	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	conn := connectWithBackoff(url, heartbeatSeconds)

	ch, err := conn.Channel()
	if err != nil {
		return nil, closeConnection(conn, err)
	}

	if err := ch.Confirm(false); err != nil {
		return nil, closeChannelAndConnection(ch, conn, err)
	}
	if err := ch.Qos(PrefetchCount, 0, false); err != nil {
		return nil, closeChannelAndConnection(ch, conn, err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, maxOutstanding))

	returns := ch.NotifyReturn(make(chan amqp.Return, 16))
	go func() {
		for ret := range returns {
			log.Fatalf("middleware: message returned by broker (no binding): exchange=%q routing_key=%q",
				ret.Exchange, ret.RoutingKey)
		}
	}()

	return &publisher{
		conn:           conn,
		channel:        ch,
		confirms:       confirms,
		flushHighWater: flushHighWaterFor(maxOutstanding, ratio),
	}, nil
}

func flushHighWaterFor(maxOutstanding int, ratio float64) int {
	return int(math.Max(1, float64(maxOutstanding)*ratio))
}

func (p *publisher) Publish(exchange, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.publishLocked(exchange, routingKey, body)
}

func (p *publisher) publishLocked(exchange, routingKey string, body []byte) error {
	if p.pendingConfirms >= p.flushHighWater {
		if err := p.flushPublisherLocked(); err != nil {
			return err
		}
	}
	if err := p.channel.Publish(
		exchange,
		routingKey,
		true,  // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	); err != nil {
		return middlewareError(err)
	}
	p.pendingConfirms++
	return nil
}

func (p *publisher) FlushPublisher() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.flushPublisherLocked()
}

func (p *publisher) flushPublisherLocked() error {
	for i := 0; i < p.pendingConfirms; i++ {
		conf, ok := <-p.confirms
		if !ok {
			return ErrMessageMiddlewareDisconnected
		}
		if !conf.Ack {
			return ErrMessageMiddlewareMessage
		}
	}
	p.pendingConfirms = 0
	return nil
}

func (p *publisher) Close() error {
	if err := p.channel.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	if err := p.conn.Close(); err != nil && err != amqp.ErrClosed {
		return ErrMessageMiddlewareClose
	}
	return nil
}

func connectWithBackoff(amqpURL string, heartbeatSeconds int) *amqp.Connection {
	backoff := connectBackoffInitial
	for {
		conn, err := amqp.DialConfig(amqpURL, amqp.Config{
			Heartbeat: time.Duration(heartbeatSeconds) * time.Second,
		})
		if err == nil {
			return conn
		}
		slog.Warn("RabbitMQ connection failed, retrying", "backoff", backoff.String(), "err", err)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > connectBackoffMax {
			backoff = connectBackoffMax
		}
	}
}
