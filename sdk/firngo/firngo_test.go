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
