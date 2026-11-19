package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStreamsSourceConnector consumes one Redis Stream with a consumer
// group (at-least-once). Position is the last stream entry ID returned;
// resume uses the group's pending/unseen semantics (entries beyond ">"
// are new; pending entries require XAUTOCLAIM, which this connector does
// NOT do — see docs: pending messages from crashed consumers are reclaimed
// by re-creating the group or a separate angel job).
type RedisStreamsSourceConnector struct {
	name     string
	urlEnv   string
	stream   string
	group    string
	consumer string
	batch    int
	client   *redis.Client
	pending  []string // entry IDs from last FetchBatch
}

// NewRedisStreamsSourceConnector creates the connector. urlEnv names the
// env var holding the Redis connection URL. group defaults to "firn-<name>",
// consumer defaults to "firn-<name>-1".
func NewRedisStreamsSourceConnector(name, urlEnv, stream, group, consumer string, batch int) (*RedisStreamsSourceConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if urlEnv == "" {
		return nil, errors.New("integration: redis_url_env required")
	}
	if stream == "" {
		return nil, errors.New("integration: redis_stream required")
	}
	if group == "" {
		group = "firn-" + name
	}
	if consumer == "" {
		consumer = "firn-" + name + "-1"
	}
	if batch <= 0 {
		batch = 100
	}
	return &RedisStreamsSourceConnector{name: name, urlEnv: urlEnv, stream: stream, group: group, consumer: consumer, batch: batch}, nil
}

func (r *RedisStreamsSourceConnector) Name() string { return r.name }

func (r *RedisStreamsSourceConnector) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

func (r *RedisStreamsSourceConnector) ensureClient(ctx context.Context) error {
	if r.client != nil {
		return nil
	}
	url := os.Getenv(r.urlEnv)
	if url == "" {
		return fmt.Errorf("integration: env %s is empty (REDIS_URL)", r.urlEnv)
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return fmt.Errorf("integration: redis parse url: %w", err)
	}
	c := redis.NewClient(opts)
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return fmt.Errorf("integration: redis ping: %w", err)
	}
	// Create the group if missing; start from the beginning of the stream
	// for migration-grade completeness. BUSYGROUP is fine (idempotent).
	if err := c.XGroupCreateMkStream(ctx, r.stream, r.group, "0").Err(); err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			_ = c.Close()
			return fmt.Errorf("integration: redis xgroup create: %w", err)
		}
	}
	r.client = c
	return nil
}

// FetchBatch pulls up to max entries with XREADGROUP (">" = new messages).
func (r *RedisStreamsSourceConnector) FetchBatch(ctx context.Context, _ string, max int) ([]Record, error) {
	if max <= 0 {
		max = r.batch
	}
	if err := r.ensureClient(ctx); err != nil {
		return nil, err
	}
	readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	streams, err := r.client.XReadGroup(readCtx, &redis.XReadGroupArgs{
		Group:    r.group,
		Consumer: r.consumer,
		Count:    int64(max),
		Block:    300 * time.Millisecond,
		Streams:  []string{r.stream, ">"},
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || readCtx.Err() != nil {
			return nil, nil // idle poll
		}
		return nil, fmt.Errorf("integration: redis xreadgroup: %w", err)
	}
	var recs []Record
	r.pending = r.pending[:0]
	for _, msg := range streams {
		for _, m := range msg.Messages {
			rec := Record{Position: m.ID, Headers: map[string]string{"redis_stream": r.stream, "redis_id": m.ID}}
			switch v := m.Values["data"].(type) {
			case string:
				rec.Value = []byte(v)
			case []byte:
				rec.Value = v
			default:
				rec.Value = []byte(fmt.Sprint(v))
			}
			switch k := m.Values["key"].(type) {
			case string:
				rec.Key = []byte(k)
			case []byte:
				rec.Key = k
			}
			recs = append(recs, rec)
			r.pending = append(r.pending, m.ID)
			if len(recs) >= max {
				break
			}
		}
	}
	return recs, nil
}

// Ack acknowledges all entries from the last FetchBatch (calls XACK).
func (r *RedisStreamsSourceConnector) Ack(ctx context.Context, records []Record) error {
	if len(r.pending) == 0 {
		return nil
	}
	if err := r.client.XAck(ctx, r.stream, r.group, r.pending...).Err(); err != nil {
		return fmt.Errorf("integration: redis xack: %w", err)
	}
	r.pending = nil
	return nil
}
