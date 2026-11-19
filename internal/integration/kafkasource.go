package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSourceConnector is a pull-based Kafka source (plan 007 slice 4).
// Position is an opaque comma-separated list of partition:offset pairs
// ("0:120,1:88"); an empty position starts from the configured start offset
// (earliest by default). The connector tracks positions itself via the
// checkpoint runtime — it does not use Kafka consumer groups, so multiple
// checkpoint files map to independent consumers of the same topic.
type KafkaSourceConnector struct {
	name     string
	brokers  []string
	topic    string
	start    string // earliest | latest
	client   *kgo.Client
}

// NewKafkaSourceConnector creates a connector; start is "earliest" (default) or "latest".
func NewKafkaSourceConnector(name string, brokers []string, topic, start string) (*KafkaSourceConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if len(brokers) == 0 || topic == "" {
		return nil, errors.New("integration: brokers and topic required")
	}
	if start == "" {
		start = "earliest"
	}
	if start != "earliest" && start != "latest" {
		return nil, fmt.Errorf("integration: start must be earliest|latest (got %q)", start)
	}
	return &KafkaSourceConnector{name: name, brokers: brokers, topic: topic, start: start}, nil
}

func (k *KafkaSourceConnector) Name() string { return k.name }

func (k *KafkaSourceConnector) Close() error {
	if k.client != nil {
		k.client.Close()
		k.client = nil
	}
	return nil
}

// kafkaPosition encodes/decodes the checkpoint position.
type kafkaPosition map[int32]int64

func encodeKafkaPosition(p kafkaPosition) string {
	parts := make([]string, 0, len(p))
	for part, off := range p {
		parts = append(parts, fmt.Sprintf("%d:%d", part, off))
	}
	// Deterministic order for stable checkpoints.
	for i := 1; i < len(parts); i++ {
		for j := i; j > 0 && parts[j] < parts[j-1]; j-- {
			parts[j], parts[j-1] = parts[j-1], parts[j]
		}
	}
	return strings.Join(parts, ",")
}

func decodeKafkaPosition(s string) (kafkaPosition, error) {
	out := kafkaPosition{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, tok := range strings.Split(s, ",") {
		var p int32
		var o int64
		if _, err := fmt.Sscanf(tok, "%d:%d", &p, &o); err != nil {
			return nil, fmt.Errorf("integration: bad kafka position token %q: %w", tok, err)
		}
		out[p] = o
	}
	return out, nil
}

// FetchBatch consumes up to max records after the position encoded in
// position. A 500ms poll bounds the wait; fewer records than max may return.
func (k *KafkaSourceConnector) FetchBatch(ctx context.Context, position string, max int) ([]Record, error) {
	if max <= 0 {
		max = 500
	}
	pos, err := decodeKafkaPosition(position)
	if err != nil {
		return nil, err
	}
	parts := map[int32]kgo.Offset{}
	if len(pos) == 0 {
		// No position yet: discover partitions via the admin client.
		adminClient, err := kgo.NewClient(kgo.SeedBrokers(k.brokers...))
		if err != nil {
			return nil, fmt.Errorf("integration: kafka admin client: %w", err)
		}
		meta, err := kadm.NewClient(adminClient).Metadata(ctx, k.topic)
		adminClient.Close()
		if err != nil {
			return nil, fmt.Errorf("integration: kafka metadata: %w", err)
		}
		detail, ok := meta.Topics[k.topic]
		if !ok || detail.Err != nil {
			return nil, fmt.Errorf("integration: topic %q: %v", k.topic, detail.Err)
		}
		if len(detail.Partitions) == 0 {
			return nil, fmt.Errorf("integration: topic %q has no partitions", k.topic)
		}
		for p := range detail.Partitions {
			if k.start == "latest" {
				parts[p] = kgo.NewOffset().AtEnd()
			} else {
				parts[p] = kgo.NewOffset().AtStart()
			}
		}
	} else {
		for p, o := range pos {
			parts[p] = kgo.NewOffset().At(o)
		}
	}
	// Fresh client per batch is wasteful; keep one around.
	if k.client == nil {
		opts := []kgo.Opt{
			kgo.SeedBrokers(k.brokers...),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{k.topic: parts}),
			// No consumer group: positions are checkpointed via the
			// integration checkpoint store, not Kafka commits.
			kgo.FetchMaxBytes(1 << 20),
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, fmt.Errorf("integration: kafka consumer: %w", err)
		}
		k.client = cl
	}
	pollCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	fetches := k.client.PollFetches(pollCtx)
	fetchErr := error(nil)
	fetches.EachError(func(topic string, partition int32, err error) {
		// A poll timeout with no records injects a synthetic partition=-1
		// fetch carrying context.DeadlineExceeded. That is the normal idle
		// path, not a failure.
		if partition == -1 && errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if fetchErr == nil {
			fetchErr = fmt.Errorf("%s/%d: %w", topic, partition, err)
		}
	})
	if fetchErr != nil {
		return nil, fetchErr
	}
	newPos := kafkaPosition{}
	for p, o := range pos {
		newPos[p] = o
	}
	var recs []Record
	fetches.EachRecord(func(rec *kgo.Record) {
		if len(recs) >= max {
			return
		}
		r := Record{Key: append([]byte(nil), rec.Key...), Value: append([]byte(nil), rec.Value...)}
		if len(rec.Headers) > 0 {
			r.Headers = map[string]string{}
			for _, h := range rec.Headers {
				r.Headers[h.Key] = string(h.Value)
			}
		}
		// Partition is embedded in the record via FetchTopicPartition on the
		// enclosing FetchTopicPartition; each record carries it through the
		// fetches iteration — franz-go sets rec.Partition on fetch records.
		newPos[rec.Partition] = rec.Offset + 1
		r.Position = encodeKafkaPosition(newPosCopy(newPos))
		recs = append(recs, r)
	})
	return recs, nil
}

func newPosCopy(p kafkaPosition) kafkaPosition {
	out := make(kafkaPosition, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}
