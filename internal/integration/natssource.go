package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

// NATSJetStreamSourceConnector consumes from a JetStream stream via a
// durable pull consumer. Records are acked only after Firn accepts the
// batch. Position encodes the last delivered stream sequence as a decimal
// string (informational; the consumer tracks position server-side).
type NATSJetStreamSourceConnector struct {
	name        string
	urlEnv      string
	stream      string
	subject     string
	consumer    string
	batch       int
	conn        *nats.Conn
	js          nats.JetStreamContext
	pending     []*nats.Msg
}

// NewNATSJetStreamSourceConnector creates the connector. urlEnv holds the
// NATS URL (e.g. nats://localhost:4222). consumer defaults to "firn-<name>".
// subject filters messages; empty = all messages in the stream.
func NewNATSJetStreamSourceConnector(name, urlEnv, stream, subject, consumer string, batch int) (*NATSJetStreamSourceConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if urlEnv == "" {
		return nil, errors.New("integration: nats_url_env required")
	}
	if stream == "" {
		return nil, errors.New("integration: nats_stream required (JetStream)")
	}
	if consumer == "" {
		consumer = "firn-" + name
	}
	if batch <= 0 {
		batch = 100
	}
	return &NATSJetStreamSourceConnector{name: name, urlEnv: urlEnv, stream: stream, subject: subject, consumer: consumer, batch: batch}, nil
}

func (n *NATSJetStreamSourceConnector) Name() string { return n.name }

func (n *NATSJetStreamSourceConnector) Close() error {
	if n.conn != nil {
		_ = n.conn.Drain()
		n.conn = nil
	}
	return nil
}

func (n *NATSJetStreamSourceConnector) ensureConnected(ctx context.Context) error {
	if n.js != nil {
		return nil
	}
	url := os.Getenv(n.urlEnv)
	if url == "" {
		return fmt.Errorf("integration: env %s is empty (NATS_URL)", n.urlEnv)
	}
	nc, err := nats.Connect(url, nats.Name("firn-ingest-"+n.name))
	if err != nil {
		return fmt.Errorf("integration: nats connect: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return fmt.Errorf("integration: nats jetstream: %w", err)
	}
	n.conn = nc
	n.js = js
	return nil
}

// FetchBatch pulls up to max messages from the durable consumer. Messages
// are retained for Ack() (called by the runner after Firn accepts).
func (n *NATSJetStreamSourceConnector) FetchBatch(ctx context.Context, position string, max int) ([]Record, error) {
	if max <= 0 {
		max = n.batch
	}
	if err := n.ensureConnected(ctx); err != nil {
		return nil, err
	}
	subject := n.subject
	if subject == "" {
		subject = ">"
	}
	sub, err := n.js.PullSubscribe(subject, n.consumer, nats.BindStream(n.stream))
	if err != nil {
		return nil, fmt.Errorf("integration: nats pull subscribe %q/%q: %w", n.stream, n.consumer, err)
	}
	fetchCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	msgs, err := sub.Fetch(max, nats.ContextOpt{Context: fetchCtx})
	if err != nil {
		// Idle poll: timeout with no messages. Any other error is loud.
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, fmt.Errorf("integration: nats fetch: %w", err)
	}
	var recs []Record
	n.pending = n.pending[:0]
	for _, msg := range msgs {
		m, err := msg.Metadata()
		if err != nil {
			n.pending = append(n.pending, msg)
			continue
		}
		r := Record{Value: append([]byte(nil), msg.Data...), Position: fmt.Sprintf("%d", m.Sequence.Stream)}
		r.Headers = map[string]string{"nats_stream": n.stream, "nats_subject": msg.Subject, "nats_seq": r.Position}
		for k, v := range msg.Header {
			if len(v) > 0 {
				r.Headers[k] = v[0]
			}
		}
		if k := msg.Header.Get("key"); k != "" {
			r.Key = []byte(k)
		}
		recs = append(recs, r)
		n.pending = append(n.pending, msg)
		if len(recs) >= max {
			break
		}
	}
	return recs, nil
}

// Ack acknowledges every message in the last FetchBatch (AckExplicit).
func (n *NATSJetStreamSourceConnector) Ack(_ context.Context, records []Record) error {
	if len(n.pending) == 0 {
		return nil
	}
	if len(records) != len(n.pending) {
		// Records and pending must be the same multiset; ack them pairwise by
		// count (order is preserved by Fetch).
		m := len(records)
		if m > len(n.pending) {
			m = len(n.pending)
		}
		for i := 0; i < m; i++ {
			if err := n.pending[i].Ack(); err != nil {
				return fmt.Errorf("integration: nats ack: %w", err)
			}
		}
		n.pending = n.pending[m:]
		return nil
	}
	for _, msg := range n.pending {
		if err := msg.Ack(); err != nil {
			return fmt.Errorf("integration: nats ack: %w", err)
		}
	}
	n.pending = nil
	return nil
}
