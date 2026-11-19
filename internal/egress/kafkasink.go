package egress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// KafkaSink produces route batches into an external Kafka topic with
// acks=all (all in-sync replicas must acknowledge). Keys pass through
// unchanged (null keys use Kafka's default partitioner). All produce errors
// are treated as retryable; records failing past the route retry budget
// dead-letter (or, without a DLQ, park and stop the route) per policy.
type kafkaSink struct {
	topic string
	name  string
	cl    *kgo.Client
}

func newKafkaSink(r Route) (Sink, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(r.Sink.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1 << 20),
	}
	if r.Sink.CAEnv != "" {
		caFile := os.Getenv(r.Sink.CAEnv)
		pemBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("egress: route %q: kafka ca %q: %w", r.Name, caFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("egress: route %q: kafka ca %q: no certificates in PEM", r.Name, caFile)
		}
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	}
	if r.Sink.SASL != nil {
		m, err := kafkaSASLMechanism(r)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(m))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka sink: %w", err)
	}
	return &kafkaSink{topic: r.Sink.Topic, name: r.Name, cl: cl}, nil
}

func kafkaSASLMechanism(r Route) (sasl.Mechanism, error) {
	m := r.Sink.SASL
	user, pass := m.Username, os.Getenv(m.PasswordEnv)
	if user == "" || pass == "" {
		return nil, fmt.Errorf("egress: route %q: kafka SASL requires username and env %s", r.Name, m.PasswordEnv)
	}
	switch m.Mechanism {
	case "plain":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("egress: route %q: sasl mechanism must be plain|scram-sha-256|scram-sha-512 (got %q)", r.Name, m.Mechanism)
	}
}

func (s *kafkaSink) Write(ctx context.Context, route string, batch []Delivery) error {
	recs := make([]*kgo.Record, 0, len(batch))
	for _, d := range batch {
		rec := &kgo.Record{Topic: s.topic, Key: d.Key, Value: d.Value}
		for k, v := range d.Headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs = append(recs, rec)
	}
	results := s.cl.ProduceSync(ctx, recs...)
	for _, res := range results {
		if res.Err != nil {
			return fmt.Errorf("kafka sink produce %s: %w", s.topic, res.Err) // retryable
		}
	}
	return nil
}

func (s *kafkaSink) IsTerminal(err error) bool { return false }

func (s *kafkaSink) Close() error {
	s.cl.Close()
	return nil
}
