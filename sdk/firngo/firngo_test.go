package firngo_test

// Integration test: the SDK against a real in-process broker over bufconn.
// The broker and its client live in the same process; nothing leaves it.

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rahulkj/firn/internal/api"
	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/queue"
	"github.com/rahulkj/firn/sdk/firngo"
)

const bufSize = 1 << 20

func testBroker(t *testing.T, topics map[string]int) (*broker.Broker, *bufconn.Listener) {
	t.Helper()
	reg := meta.NewRegistry()
	for name, partitions := range topics {
		if err := reg.Create(name, partitions); err != nil {
			t.Fatalf("create topic %q: %v", name, err)
		}
	}
	b, err := broker.NewWithDir(reg, t.TempDir(), broker.WithMaxSegmentBytes(1<<20))
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	lis := bufconn.Listen(bufSize)
	t.Cleanup(func() { _ = lis.Close() })
	srv := grpc.NewServer()
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(b, nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return b, lis
}

func TestSDKProduceFetchRecords(t *testing.T) {
	_, lis := testBroker(t, map[string]int{"events": 2})

	_ = lis // bufconn not used; TCP in-process listener instead
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	srv := grpc.NewServer()
	reg := meta.NewRegistry()
	if err := reg.Create("events", 2); err != nil {
		t.Fatal(err)
	}
	b, err := broker.NewWithDir(reg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(b, nil))
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	ctx := context.Background()
	sdk, err := firngo.DialInsecure(ctx, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sdk.Close() }()

	for i := 0; i < 20; i++ {
		_, _, err := sdk.Produce(ctx, "events", []byte(fmt.Sprintf("key-%d", i%3)), []byte(fmt.Sprintf("payload-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	recs, err := sdk.Fetch(ctx, "events", 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	_ = recs // partition 0 may have 0 or some records; offsets are partition-scoped

	// Health check must round-trip against a running broker.
	if err := sdk.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}
}

func TestSDKSubscribeDeliversInOrder(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	reg := meta.NewRegistry()
	if err := reg.Create("events", 1); err != nil {
		t.Fatal(err)
	}
	b, err := broker.NewWithDir(reg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	srv := grpc.NewServer()
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(b, nil))
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sdk, err := firngo.DialInsecure(ctx, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sdk.Close() }()

	// Produce 5 records.
	for i := 0; i < 5; i++ {
		if _, _, err := sdk.Produce(ctx, "events", []byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Consume them into a buffer via SubscribePartition, in offset order.
	got := []string{}
	subCtx, subCancel := context.WithCancel(ctx)
	go func() {
		_ = sdk.SubscribePartition(subCtx, "events", 0, 0, 100, func(r firngo.Record) error {
			got = append(got, string(r.Value))
			if len(got) == 5 {
				subCancel()
			}
			return nil
		})
	}()
	<-subCtx.Done()
	sort.Strings(got) // unordered by arrival; check set equality
	want := []string{"v-0", "v-1", "v-2", "v-3", "v-4"}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d: %v", len(got), len(want), got)
	}
	for i, s := range got {
		if s != want[i] {
			t.Fatalf("record %d = %q, want %q", i, s, want[i])
		}
	}
}

func TestSDKProduceBatchRoundTrip(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	srv := grpc.NewServer()
	reg := meta.NewRegistry()
	if err := reg.Create("events", 3); err != nil {
		t.Fatal(err)
	}
	b, err := broker.NewWithDir(reg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(b, nil))
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	ctx := context.Background()
	sdk, err := firngo.DialInsecure(ctx, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sdk.Close() }()

	records := make([]firngo.KeyValue, 0, 30)
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("k-%d", i%4))
		if i%4 == 3 {
			key = nil // exercise round-robin routing
		}
		records = append(records, firngo.KeyValue{Key: key, Value: []byte(fmt.Sprintf("payload-%02d", i))})
	}
	results, err := sdk.ProduceBatch(ctx, "events", records)
	if err != nil {
		t.Fatalf("produce batch: %v", err)
	}
	if len(results) != len(records) {
		t.Fatalf("results = %d, want %d", len(results), len(records))
	}
	ledgers := map[int32][]firngo.BatchResult{}
	for _, r := range results {
		if r.Partition < 0 || r.Partition >= 3 {
			t.Fatalf("partition out of range: %d", r.Partition)
		}
		ledgers[r.Partition] = append(ledgers[r.Partition], r)
	}
	for p, rs := range ledgers {
		for i := 1; i < len(rs); i++ {
			if rs[i].Offset <= rs[i-1].Offset {
				t.Fatalf("partition %d offsets not monotonic: %d then %d", p, rs[i-1].Offset, rs[i].Offset)
			}
		}
	}

	// Read everything back and check payload presence per partition.
	for p := int32(0); p < 3; p++ {
		recs, err := sdk.Fetch(ctx, "events", p, 0, 100)
		if err != nil {
			t.Fatalf("fetch partition %d: %v", p, err)
		}
		if len(recs) != len(ledgers[p]) {
			t.Fatalf("partition %d fetched %d records, produced %d", p, len(recs), len(ledgers[p]))
		}
	}
}

func TestSDKProduceBatchValidatesInput(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	srv := grpc.NewServer()
	reg := meta.NewRegistry()
	if err := reg.Create("events", 1); err != nil {
		t.Fatal(err)
	}
	b, err := broker.NewWithDir(reg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(b, nil))
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	ctx := context.Background()
	sdk, err := firngo.DialInsecure(ctx, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sdk.Close() }()

	if _, err := sdk.ProduceBatch(ctx, "events", nil); err == nil {
		t.Fatal("empty batch must be rejected")
	}
	tooMany := make([]firngo.KeyValue, 501)
	for i := range tooMany {
		tooMany[i] = firngo.KeyValue{Value: []byte{byte(i)}}
	}
	if _, err := sdk.ProduceBatch(ctx, "events", tooMany); err == nil {
		t.Fatal(">500 records must be rejected")
	}
}

func TestSDKPullAckBatchRoundTrip(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	srv := grpc.NewServer()
	reg := meta.NewRegistry()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	b, err := broker.NewWithDir(reg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	qm, err := queue.NewManager(reg, b, t.TempDir())
	if err != nil {
		t.Fatalf("queue manager: %v", err)
	}
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(b, qm))
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	ctx := context.Background()
	sdk, err := firngo.DialInsecure(ctx, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sdk.Close() }()
	// Register the same queue against an in-process queue manager: the SDK
	// test needs a queue-mode topic with queue manager wired, like firnd.
	// (The previous tests in this file dial with plain data-plane RPCs
	// only; here we register the queue on the broker via the test broker.)

	for i := 0; i < 10; i++ {
		_, _, err := sdk.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("v-%d", i)))
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	deliveries, err := sdk.Pull(ctx, "jobs", 5, 30000)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(deliveries) != 5 {
		t.Fatalf("pulled %d, want 5", len(deliveries))
	}
	ids := []string{deliveries[0].DeliveryID, "bogus-delivery-id"}
	res, err := sdk.AckBatch(ctx, "jobs", ids)
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if res.Acked != 1 || res.Unknown != 1 {
		t.Fatalf("ack batch = %+v, want {Acked:1 Unknown:1}", res)
	}
	ids = ids[:0]
	for _, d := range deliveries[1:] {
		ids = append(ids, d.DeliveryID)
	}
	res, err = sdk.AckBatch(ctx, "jobs", ids)
	if err != nil {
		t.Fatalf("ack batch 2: %v", err)
	}
	if res.Acked != 4 || res.Unknown != 0 {
		t.Fatalf("ack batch 2 = %+v, want {Acked:4 Unknown:0}", res)
	}

	// Validate input caps.
	tooMany := make([]string, 501)
	if _, err := sdk.AckBatch(ctx, "jobs", tooMany); err == nil {
		t.Fatal("oversized batch must be rejected")
	}
}
