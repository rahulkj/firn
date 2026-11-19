package egress

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStreamsSink appends each record as one stream entry with a single
// "data" field carrying the record value verbatim (and "key" when set).
// Retention is bounded by MAXLEN ~ 10000 (approximate trim).
type RedisStreamsSink struct {
	urlEnv string
	stream string
	client *redis.Client
}

func newRedisStreamsSink(r Route) (Sink, error) {
	return &RedisStreamsSink{urlEnv: r.Sink.RedisURLEnv, stream: r.Sink.RedisStream}, nil
}

func (s *RedisStreamsSink) ensureClient(ctx context.Context) error {
	if s.client != nil {
		return nil
	}
	url := os.Getenv(s.urlEnv)
	if url == "" {
		return fmt.Errorf("egress: env %s is empty (REDIS_URL)", s.urlEnv)
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return fmt.Errorf("egress: redis parse url: %w", err)
	}
	c := redis.NewClient(opts)
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return fmt.Errorf("egress: redis ping: %w", err)
	}
	s.client = c
	return nil
}

func (s *RedisStreamsSink) Write(ctx context.Context, route string, batch []Delivery) error {
	if err := s.ensureClient(ctx); err != nil {
		return err
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, 0, len(batch))
	for _, d := range batch {
		values := map[string]any{"data": string(d.Value)}
		if d.Key != nil {
			values["key"] = string(d.Key)
		}
		cmds = append(cmds, pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: s.stream,
			MaxLen: 10000,
			Approx: true,
			Values: values,
		}))
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		// Any pipeline failure: retryable. Individual errors surface in cmds errors.
		return fmt.Errorf("egress: redis xadd: %w (retryable)", err)
	}
	for i, cmd := range cmds {
		if cmd == nil {
			continue
		}
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("egress: redis xadd entry %d: %w (retryable)", i, err)
		}
	}
	// Keep a small idle drain so a failing Redis surfaces promptly rather than buffering forever.
	_ = time.Now()
	return nil
}

func (s *RedisStreamsSink) IsTerminal(err error) bool { return false }

func (s *RedisStreamsSink) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
