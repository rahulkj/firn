package egress

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func TestRabbitMQSinkLive(t *testing.T) {
	url := envOrSkip(t, "AMQP_TEST_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Seed: declare a test queue and purge it.
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueueDeclare("firn.egress.test", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueuePurge("firn.egress.test", false); err != nil {
		t.Fatal(err)
	}
	_ = ch.Close()
	_ = conn.Close()

	r := Route{Name: "rmq-out"}
	r.Sink = SinkConfig{Type: "rabbitmq", AMQPURLEnv: "AMQP_TEST_URL", AMQPQueue: "firn.egress.test"}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()

	batch := []Delivery{
		{Value: []byte("alpha"), Offset: 0, Partition: 0, Key: []byte("k1")},
		{Value: []byte("beta"), Offset: 1, Partition: 0},
	}
	if err := sink.Write(ctx, "rmq-out", batch); err != nil {
		t.Fatal(err)
	}

	// Read back with a separate connection.
	conn2, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn2.Close() }()
	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for i := 0; i < 2; i++ {
		msg, ok, err := ch2.Get("firn.egress.test", true)
		if err != nil || !ok {
			t.Fatalf("get %d: %v", i, err)
		}
		seen[fmt.Sprint(msg.DeliveryTag)] = string(msg.Body)
	}
	_ = ch2.Close()
	if seen["1"] != "alpha" || seen["2"] != "beta" {
		t.Fatalf("messages wrong: %v", seen)
	}
}

func TestNATSSinkLiveJetStream(t *testing.T) {
	url := envOrSkip(t, "NATS_TEST_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Ensure the stream exists (create it idempotently).
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "EGT",
		Subjects: []string{"egress.test.>"},
		Storage:  nats.FileStorage,
	})
	if err != nil && !isStreamAlreadyExists(err) {
		nc.Close()
		t.Fatal(err)
	}
	if err := js.PurgeStream("EGT"); err != nil {
		nc.Close()
		t.Fatal(err)
	}
	nc.Close()

	r := Route{Name: "nats-out"}
	r.Sink = SinkConfig{Type: "nats", NatsURLEnv: "NATS_TEST_URL", NatsSubject: "egress.test.data", NatsStream: "EGT"}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()

	batch := []Delivery{
		{Value: []byte("one"), Offset: 0, Partition: 0, Key: []byte("k1")},
		{Value: []byte("two"), Offset: 1, Partition: 0},
	}
	if err := sink.Write(ctx, "nats-out", batch); err != nil {
		t.Fatal(err)
	}

	// Verify by fetching from the consumer.
	nc2, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { nc2.Close() }()
	js2, err := nc2.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := js2.PullSubscribe("egress.test.data", "firn-egress-test-verify", nats.BindStream("EGT"))
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := sub.Fetch(10, nats.ContextOpt{Context: ctx})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, m := range msgs {
		seen[m.Header.Get("key")] = string(m.Data)
		_ = m.Ack()
	}
	if seen["k1"] != "one" {
		t.Fatalf("msg 1 missing or wrong: %v", seen)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 messages, saw %v", seen)
	}
}

func isStreamAlreadyExists(err error) bool {
	s := err.Error()
	return s == "nats: stream name already in use" || (len(s) > 0 && contains(s, "already"))
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestRedisStreamsSinkLive(t *testing.T) {
	url := envOrSkip(t, "REDIS_TEST_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Deterministic stream.
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rc := redis.NewClient(opts)
	defer func() { _ = rc.Close() }()
	_ = rc.Del(ctx, "firn.sink.test").Err()

	r := Route{Name: "redis-out"}
	r.Sink = SinkConfig{Type: "redis-streams", RedisURLEnv: "REDIS_TEST_URL", RedisStream: "firn.sink.test"}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()

	batch := []Delivery{
		{Value: []byte("left"), Offset: 0, Partition: 0, Key: []byte("kk")},
		{Value: []byte("right"), Offset: 1, Partition: 0},
	}
	if err := sink.Write(ctx, "redis-out", batch); err != nil {
		t.Fatal(err)
	}

	// Read entries back.
	msgs, err := rc.XRead(ctx, &redis.XReadArgs{
		Streams: []string{"firn.sink.test", "0-0"},
		Count:   10,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	var entries []string
	for _, msg := range msgs {
		for _, m := range msg.Messages {
			entries = append(entries, fmt.Sprint(m.Values["data"]))
		}
	}
	if fmt.Sprint(entries) != "[left right]" {
		t.Fatalf("entries = %v, want [left right]", entries)
	}
}

// envOrSkip mirrors the integration package's helper for consistency.
func envOrSkip(t *testing.T, key string) string {
	t.Helper()
	v := getenv(key)
	if v == "" {
		t.Skipf("%s not set; skipping live test", key)
	}
	return v
}

func getenv(key string) string { return os.Getenv(key) }
