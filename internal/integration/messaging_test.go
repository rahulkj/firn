package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// Live AMQP source test. Requires AMQP_TEST_URL, e.g.
//   AMQP_TEST_URL=amqp://USER:PASS@127.0.0.1:5672/%2f go test ./internal/integration/ -run TestAMQP -v
// The test creates a queue firn.test, publishes messages to it, then runs the connector.
func TestAMQPSourceLive(t *testing.T) {
	url := envOrSkip(t, "AMQP_TEST_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := amqp091.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	_, err = ch.QueueDeclare("firn.test", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueuePurge("firn.test", false); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := ch.PublishWithContext(ctx, "", "firn.test", false, false, amqp091.Publishing{Body: []byte(fmt.Sprintf("msg-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	_ = ch.Close()
	_ = conn.Close()

	c, err := NewAMQPSourceConnector("amqp-test", "AMQP_TEST_URL", "firn.test", 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	seen := map[string]bool{}
	position := ""
	for range 40 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
			position = r.Position
		}
		if len(seen) >= 4 {
			break
		}
		if len(recs) == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if len(seen) != 4 {
		t.Fatalf("seen %d messages (%v), want 4", len(seen), seen)
	}
	// Ack must not error and must clear broker state.
	recs, err := c.FetchBatch(ctx, position, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(ctx, recs); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// Live NATS JetStream test. Requires NATS_TEST_URL and NATS_TEST_STREAM=EVTS
// with subjects evt.>, e.g.
//   NATS_TEST_URL='nats://127.0.0.1:4222' NATS_TEST_STREAM=EVTS go test ./internal/integration/ -run TestNATS -v
// Creates the stream if missing.
func TestNATSJetStreamSourceLive(t *testing.T) {
	url := envOrSkip(t, "NATS_TEST_URL")
	stream := envOrSkip(t, "NATS_TEST_STREAM")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{"evt.>"},
		Storage:  nats.FileStorage,
	}); err != nil {
		if !isStreamExists(err) {
			t.Fatal(err)
		}
	}
	// Purge the stream for a deterministic test.
	if err := js.PurgeStream(stream); err != nil {
		t.Fatal(err)
	}

	c, err := NewNATSJetStreamSourceConnector("nats-test", "NATS_TEST_URL", stream, "evt.>", "", 64)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	for i := 0; i < 3; i++ {
		if _, err := js.Publish("evt.test", []byte(fmt.Sprintf("nats-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	position := ""
	for range 40 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
			position = r.Position
		}
		if len(seen) >= 3 {
			break
		}
		if len(recs) == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("seen %d messages, want 3", len(seen))
	}
}

// Live Redis Streams test. Requires REDIS_TEST_URL, e.g.
//   REDIS_TEST_URL='redis://127.0.0.1:6379/0' go test ./internal/integration/ -run TestRedis -v
func TestRedisStreamsLive(t *testing.T) {
	url := envOrSkip(t, "REDIS_TEST_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := NewRedisStreamsSourceConnector("redis-test", "REDIS_TEST_URL", "firn.test", "", "", 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	rc := redis.NewClient(mustParseRedisURL(t, url))
	defer func() { _ = rc.Close() }()
	// Deterministic stream: drop group and stream if they exist, then re-create.
	_ = rc.XGroupDestroy(ctx, "firn.test", "firn-redis-test").Err()
	_ = rc.Del(ctx, "firn.test").Err()
	for i := 0; i < 3; i++ {
		_, err := rc.XAdd(ctx, &redis.XAddArgs{
			Stream: "firn.test",
			Values: map[string]any{"data": fmt.Sprintf("redis-%d", i), "key": fmt.Sprintf("k%d", i)},
		}).Result()
		if err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	position := ""
	for range 40 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
			position = r.Position
		}
		if len(seen) >= 3 {
			break
		}
		if len(recs) == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("seen %d entries, want 3", len(seen))
	}
}

func isStreamExists(err error) bool {
	// Stream exists error from AddStream when name already exists
	return err != nil && (err.Error() == "nats: stream name already in use" || contains(err.Error(), "already"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func mustParseRedisURL(t *testing.T, url string) *redis.Options {
	t.Helper()
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	return opts
}
