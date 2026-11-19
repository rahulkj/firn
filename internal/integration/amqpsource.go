package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPSourceConnector consumes from one RabbitMQ queue with manual
// acknowledgements; records are acked only after Firn accepts them
// (multiple=true per batch on one long-lived channel). At-least-once;
// duplicates possible. Position is diagnostic-only (the broker owns
// delivery state).
type AMQPSourceConnector struct {
	name     string
	urlEnv   string
	queue    string
	prefetch int
	conn     *amqp.Connection
	ch       *amqp.Channel
	delivery <-chan amqp.Delivery
	mu       sync.Mutex
	lastTag  uint64
}

// NewAMQPSourceConnector creates the connector. urlEnv holds the AMQP URL
// (e.g. amqp://guest:guest@host:5672/%2f). prefetch is the QoS prefetch (default 64).
func NewAMQPSourceConnector(name, urlEnv, queue string, prefetch int) (*AMQPSourceConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if urlEnv == "" {
		return nil, errors.New("integration: amqp_url_env required")
	}
	if queue == "" {
		return nil, errors.New("integration: queue required")
	}
	if prefetch <= 0 {
		prefetch = 64
	}
	return &AMQPSourceConnector{name: name, urlEnv: urlEnv, queue: queue, prefetch: prefetch}, nil
}

func (a *AMQPSourceConnector) Name() string { return a.name }

func (a *AMQPSourceConnector) Close() error {
	if a.ch != nil {
		_ = a.ch.Close()
		a.ch = nil
	}
	if a.conn != nil {
		_ = a.conn.Close()
		a.conn = nil
	}
	return nil
}

func (a *AMQPSourceConnector) ensureConnected(ctx context.Context) error {
	if a.ch != nil {
		return nil
	}
	url := os.Getenv(a.urlEnv)
	if url == "" {
		return fmt.Errorf("integration: env %s is empty (AMQP_URL)", a.urlEnv)
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("integration: amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("integration: amqp channel: %w", err)
	}
	if err := ch.Qos(a.prefetch, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("integration: amqp qos: %w", err)
	}
	if _, err := ch.QueueDeclarePassive(a.queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("integration: queue %q does not exist: %w", a.queue, err)
	}
	sub, err := ch.Consume(a.queue, "firn-ingest-"+a.name, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("integration: amqp consume: %w", err)
	}
	a.conn = conn
	a.ch = ch
	a.delivery = sub
	return nil
}

// FetchBatch drains up to max messages from the queue within a short window.
func (a *AMQPSourceConnector) FetchBatch(ctx context.Context, _ string, max int) ([]Record, error) {
	if max <= 0 {
		max = 100
	}
	if err := a.ensureConnected(ctx); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	var recs []Record
	var lastTag uint64
loop:
	for len(recs) < max {
		select {
		case delivery, ok := <-a.delivery:
			if !ok {
				return recs, nil
			}
			r := Record{Value: append([]byte(nil), delivery.Body...)}
			h := map[string]string{"amqp_queue": a.queue, "amqp_tag": fmt.Sprint(delivery.DeliveryTag)}
			for k, v := range delivery.Headers {
				if s, ok := v.(string); ok {
					h[k] = s
				}
			}
			if k, ok := delivery.Headers["key"]; ok {
				if s, ok := k.(string); ok {
					r.Key = []byte(s)
				}
			}
			r.Headers = h
			lastTag = delivery.DeliveryTag
			r.Position = fmt.Sprintf("%d", delivery.DeliveryTag)
			recs = append(recs, r)
		case <-ctx.Done():
			return recs, nil
		default:
			if time.Now().After(deadline) {
				break loop
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	if lastTag != 0 {
		a.mu.Lock()
		a.lastTag = lastTag
		a.mu.Unlock()
	}
	return recs, nil
}

// Ack acknowledges all deliveries up to and including the last tag read on
// this channel (multiple=true). The Runner calls it after Firn accepts the batch.
func (a *AMQPSourceConnector) Ack(_ context.Context, _ []Record) error {
	a.mu.Lock()
	tag := a.lastTag
	a.mu.Unlock()
	if tag == 0 || a.ch == nil {
		return nil
	}
	return a.ch.Ack(tag, true)
}
