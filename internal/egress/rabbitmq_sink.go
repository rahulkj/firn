package egress

import (
	"context"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQSink publishes to one queue with persistent delivery mode and
// broker publish confirms (at-least-once). The record value is the message
// body; the record key (if set) becomes the AMQP routing key.
type RabbitMQSink struct {
	urlEnv   string
	queue    string
	conn     *amqp.Connection
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
}

func newRabbitMQSink(r Route) (Sink, error) {
	return &RabbitMQSink{urlEnv: r.Sink.AMQPURLEnv, queue: r.Sink.AMQPQueue}, nil
}

func (s *RabbitMQSink) ensureConn(ctx context.Context) error {
	if s.ch != nil {
		return nil
	}
	url := os.Getenv(s.urlEnv)
	if url == "" {
		return fmt.Errorf("egress: env %s is empty (AMQP_URL)", s.urlEnv)
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("egress: amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("egress: amqp channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("egress: amqp confirm mode: %w", err)
	}
	if _, err := ch.QueueDeclarePassive(s.queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("egress: rabbitmq queue %q: %w", s.queue, err)
	}
	s.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 100))
	s.conn = conn
	s.ch = ch
	return nil
}

func (s *RabbitMQSink) Write(ctx context.Context, route string, batch []Delivery) error {
	if err := s.ensureConn(ctx); err != nil {
		return err
	}
	// Publish the whole batch, then drain confirmations (confirms arrive in
	// publish order on this channel).
	for _, d := range batch {
		// Route via the default (direct) exchange: routing key must equal
		// the destination queue name. Record key rides as a header.
		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/octet-stream",
			Body:         d.Value,
		}
		if d.Key != nil {
			msg.Headers = amqp.Table{"key": string(d.Key)}
			msg.CorrelationId = string(d.Key)
		}
		if err := s.ch.PublishWithContext(ctx, "", s.queue, true, false, msg); err != nil {
			return fmt.Errorf("egress: amqp publish: %w (retryable)", err)
		}
	}
	deadline := time.After(10 * time.Second)
	for i := 0; i < len(batch); i++ {
		select {
		case confirm := <-s.confirms:
			if !confirm.Ack {
				return fmt.Errorf("egress: amqp broker nacked publish (retryable)")
			}
		case <-deadline:
			return fmt.Errorf("egress: timed out waiting for amqp confirms (retryable)")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *RabbitMQSink) IsTerminal(err error) bool { return false }

func (s *RabbitMQSink) Close() error {
	if s.ch != nil {
		_ = s.ch.Close()
		s.ch = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	return nil
}
