package egress

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

// NATSSink publishes record values onto a subject. With nats_stream set it
// uses JetStream publish (synchronous, at-least-once). Without a stream it
// uses plain core NATS publish (no broker ack — delivery attempt only;
// documented as at-most-once in that configuration).
type NATSSink struct {
	urlEnv  string
	subject string
	stream  string
	conn    *nats.Conn
	js      nats.JetStreamContext
}

func newNATSSink(r Route) (Sink, error) {
	return &NATSSink{urlEnv: r.Sink.NatsURLEnv, subject: r.Sink.NatsSubject, stream: r.Sink.NatsStream}, nil
}

func (s *NATSSink) ensureConn(ctx context.Context) error {
	if s.conn != nil {
		return nil
	}
	url := os.Getenv(s.urlEnv)
	if url == "" {
		return fmt.Errorf("egress: env %s is empty (NATS_URL)", s.urlEnv)
	}
	nc, err := nats.Connect(url, nats.Name("firn-egress"))
	if err != nil {
		return fmt.Errorf("egress: nats connect: %w", err)
	}
	if s.stream != "" {
		js, err := nc.JetStream()
		if err != nil {
			nc.Close()
			return fmt.Errorf("egress: nats jetstream: %w", err)
		}
		// Verify the stream exists.
		if _, err := js.StreamInfo(s.stream); err != nil {
			nc.Close()
			return fmt.Errorf("egress: nats stream %q: %w", s.stream, err)
		}
		s.js = js
	}
	s.conn = nc
	return nil
}

func (s *NATSSink) Write(ctx context.Context, route string, batch []Delivery) error {
	if err := s.ensureConn(ctx); err != nil {
		return err
	}
	for _, d := range batch {
		msg := &nats.Msg{Subject: s.subject, Data: d.Value, Header: nats.Header{}}
		if d.Key != nil {
			msg.Header.Set("key", string(d.Key))
		}
		for k, v := range d.Headers {
			msg.Header.Set(k, v)
		}
		if s.js != nil {
			if _, err := s.js.PublishMsg(msg); err != nil {
				// JetStream publish failure: retryable.
				return fmt.Errorf("egress: nats js publish: %w (retryable)", err)
			}
			continue
		}
		// Core NATS: no broker ack; callers that need durability must set nats_stream.
		if err := s.conn.PublishMsg(msg); err != nil {
			return fmt.Errorf("egress: nats publish: %w (retryable)", err)
		}
	}
	return nil
}

func (s *NATSSink) IsTerminal(err error) bool { return false }

func (s *NATSSink) Close() error {
	if s.conn != nil {
		_ = s.conn.Drain() // Flush pending writes and close
		_ = time.Now()
		s.conn = nil
	}
	return nil
}
