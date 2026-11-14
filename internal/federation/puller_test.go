package federation_test

// Integration tests for the passive one-way puller (ADR-010, plan 004 slice 2):
// byte-identical import from a source cluster into this sink, incremental
// resumption, sink write fencing, queue delivery idling, and the auth
// isolation of federation keys. The source is a second broker in a temp dir;
// FederationService is reached over bufconn.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rahulkj/firn/internal/api"
	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/federation"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/queue"
	"github.com/rahulkj/firn/internal/ratelimit"
)

func newFileBroker(t *testing.T, dir string, reg *meta.Registry) *broker.Broker {
	t.Helper()
	b, err := broker.NewWithDir(reg, dir, broker.WithMaxSegmentBytes(1<<20))
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func newRegistry(t *testing.T, topics ...string) *meta.Registry {
	t.Helper()
	reg := meta.NewRegistry()
	for _, name := range topics {
		if err := reg.Create(name, 1); err != nil {
			t.Fatalf("create topic %q: %v", name, err)
		}
	}
	return reg
}

func TestPassivePullByteIdenticalAndResumes(t *testing.T) {
	srcReg := newRegistry(t, "orders")
	dstReg := newRegistry(t, "orders")
	src := newFileBroker(t, t.TempDir(), srcReg)
	dst := newFileBroker(t, t.TempDir(), dstReg)
	ctx := context.Background()

	produce := func(from, to int) {
		for i := from; i < to; i++ {
			if _, _, err := src.Produce(ctx, "orders", []byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i))); err != nil {
				t.Fatalf("produce %d: %v", i, err)
			}
		}
	}
	produce(0, 20)

	// Source federation endpoint on bufconn.
	lis := bufconnListen(t)
	srv := grpc.NewServer()
	firnpb.RegisterFederationServiceServer(srv, api.NewFederationServer(src, federation.OpenInMemory()))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	dst.SetFederationFence(func(topic string, partition int32) bool {
		return topic == "orders" // as firnd wires it from the peer store
	})

	store := federation.OpenInMemory()
	if err := store.AddPeer(federation.Peer{
		Name:      "src",
		Addr:      "bufnet",
		Direction: federation.DirectionIn,
		Mode:      federation.ModePassive,
		Key:       "peer-bearer",
		Selectors: []federation.Selector{{Topics: []string{"orders"}}},
	}); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	puller := federation.NewPuller(federation.PullerConfig{
		Broker:   dst,
		Store:    store,
		Interval: time.Millisecond,
		Dial:     bufconnDialer(lis),
	})
	pullCtx, pullCancel := context.WithCancel(context.Background())
	defer pullCancel()
	go puller.Run(pullCtx)

	waitFor(t, 15*time.Second, func() bool {
		hw, err := dst.PartitionHighWatermark(ctx, "orders", 0)
		return err == nil && hw == 20
	})

	// Byte-identical on-disk frames (the passive-mode contract).
	srcRaw, err := src.ReadRange(ctx, "orders", 0, 0, 19)
	if err != nil {
		t.Fatalf("source range: %v", err)
	}
	dstRaw, err := dst.ReadRange(ctx, "orders", 0, 0, 19)
	if err != nil {
		t.Fatalf("sink range: %v", err)
	}
	if !bytes.Equal(srcRaw, dstRaw) {
		t.Fatalf("passive import is not byte-identical: source %d bytes, sink %d bytes", len(srcRaw), len(dstRaw))
	}

	srcRecs, err := src.Fetch(ctx, "orders", 0, 0, 100)
	if err != nil {
		t.Fatalf("source fetch: %v", err)
	}
	dstRecs, err := dst.Fetch(ctx, "orders", 0, 0, 100)
	if err != nil {
		t.Fatalf("sink fetch: %v", err)
	}
	if len(srcRecs) != len(dstRecs) {
		t.Fatalf("record count differs: %d vs %d", len(srcRecs), len(dstRecs))
	}
	// Timestamps are preserved by byte-identical replication; compare them
	// through the raw read path, which carries the frame timestamp.
	for i := range srcRecs {
		if srcRecs[i].Offset != dstRecs[i].Offset {
			t.Fatalf("record %d: offsets differ (%d vs %d)", i, srcRecs[i].Offset, dstRecs[i].Offset)
		}
		if !bytes.Equal(srcRecs[i].Key, dstRecs[i].Key) || !bytes.Equal(srcRecs[i].Value, dstRecs[i].Value) {
			t.Fatalf("record %d: key/value differ", i)
		}
	}

	// Incremental resumption: produce 5 more, only those are re-fetched
	// (offsets below HWM are skipped idempotently either way).
	produce(20, 25)
	pullCancel()
	puller.Stop()

	pullCtx2, pullCancel2 := context.WithCancel(context.Background())
	defer pullCancel2()
	puller2 := federation.NewPuller(federation.PullerConfig{
		Broker:   dst,
		Store:    store,
		Interval: time.Millisecond,
		Dial:     bufconnDialer(lis),
	})
	go puller2.Run(pullCtx2)
	waitFor(t, 15*time.Second, func() bool {
		hw, err := dst.PartitionHighWatermark(ctx, "orders", 0)
		return err == nil && hw == 25
	})
	pullCancel2()
	puller2.Stop()
	hw, _ := dst.PartitionHighWatermark(ctx, "orders", 0)
	if hw != 25 {
		t.Fatalf("sink high watermark = %d, want 25", hw)
	}

	// Sink stays fenced until cutover: local produce is refused.
	if _, _, err := dst.Produce(ctx, "orders", []byte("k"), []byte("local-write")); !errors.Is(err, broker.ErrFederatedPassive) {
		t.Fatalf("sink produce on federated topic: want ErrFederatedPassive, got %v", err)
	}
}

func TestFederationKeyIsolationOverGRPC(t *testing.T) {
	srcReg := newRegistry(t, "orders")
	src := newFileBroker(t, t.TempDir(), srcReg)

	keysDir := t.TempDir()
	store, err := apikeys.OpenDir(keysDir)
	if err != nil {
		t.Fatalf("apikeys store: %v", err)
	}
	fedKey, err := store.Create("fed-peer", "federation")
	if err != nil {
		t.Fatalf("create federation key: %v", err)
	}
	opKey, err := store.Create("ops", "operator")
	if err != nil {
		t.Fatalf("create operator key: %v", err)
	}

	authorizer := api.NewAuthorizer(store, ratelimit.New(1000, 1000), nil)
	lis := bufconnListen(t)
	srv := grpc.NewServer(grpc.UnaryInterceptor(authorizer.GRPCInterceptor()))
	firnpb.RegisterFederationServiceServer(srv, api.NewFederationServer(src, federation.OpenInMemory()))
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(src, nil))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	dial := func(t *testing.T) *grpc.ClientConn {
		t.Helper()
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	withBearer := func(ctx context.Context, key string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
	}

	// Federation key can call FederationFetch.
	fedConn := dial(t)
	fctx, cancel := context.WithTimeout(withBearer(context.Background(), fedKey), 5*time.Second)
	defer cancel()
	if _, err := firnpb.NewFederationServiceClient(fedConn).FederationFetch(fctx, &firnpb.FederationFetchRequest{Topic: "orders", Partition: 0, FirstOffset: 0, LastOffset: -1}); err != nil {
		t.Fatalf("federation key on FederationFetch: %v", err)
	}
	// Federation key cannot Produce.
	pctx, cancel := context.WithTimeout(withBearer(context.Background(), fedKey), 5*time.Second)
	defer cancel()
	_, err = firnpb.NewFirnServiceClient(fedConn).Produce(pctx, &firnpb.ProduceRequest{Topic: "orders", Key: []byte("k"), Value: []byte("v")})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("federation key on Produce: want PermissionDenied, got %v", err)
	}
	// Operator key cannot call FederationFetch.
	opConn := dial(t)
	octx, cancel := context.WithTimeout(withBearer(context.Background(), opKey), 5*time.Second)
	defer cancel()
	_, err = firnpb.NewFederationServiceClient(opConn).FederationFetch(octx, &firnpb.FederationFetchRequest{Topic: "orders", Partition: 0, FirstOffset: 0, LastOffset: -1})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("operator key on FederationFetch: want PermissionDenied, got %v", err)
	}
}

func TestPassivePullQueueDeliveryStaysIdle(t *testing.T) {
	reg := meta.NewRegistry()
	if err := reg.CreateWithMode("jobs", 1, "queue"); err != nil {
		t.Fatalf("create queue topic: %v", err)
	}
	dst := newFileBroker(t, t.TempDir(), reg)
	m, err := queue.NewManager(reg, dst, "")
	if err != nil {
		t.Fatalf("queue manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetFederationFence(func(q string) bool { return q == "jobs" })
	_, err = m.Pull(context.Background(), "jobs", 1, 30*time.Second)
	if !errors.Is(err, queue.ErrFederatedPassive) {
		t.Fatalf("pull on passively-federated queue: want ErrFederatedPassive, got %v", err)
	}
	if err := m.Ack(context.Background(), "jobs", "delivery-1"); !errors.Is(err, queue.ErrFederatedPassive) {
		t.Fatalf("ack on passively-federated queue: want ErrFederatedPassive, got %v", err)
	}
}

func bufconnListen(t *testing.T) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 16)
	t.Cleanup(func() { _ = lis.Close() })
	return lis
}

func bufconnDialer(lis *bufconn.Listener) func(context.Context, federation.Peer) (*grpc.ClientConn, error) {
	return func(ctx context.Context, _ federation.Peer) (*grpc.ClientConn, error) {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		return conn, err
	}
}

func waitFor(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestPullerLagGrowsWhenPeerUnreachable(t *testing.T) {
	srcReg := newRegistry(t, "orders")
	src := newFileBroker(t, t.TempDir(), srcReg)
	defer func() { _ = src.Close() }()
	dst := newFileBroker(t, t.TempDir(), newRegistry(t, "orders"))

	store := federation.OpenInMemory()
	if err := store.AddPeer(federation.Peer{
		Name: "src", Addr: "bufnet", Direction: federation.DirectionIn,
		Mode: federation.ModePassive, Key: "k",
		Selectors: []federation.Selector{{Topics: []string{"orders"}}},
	}); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	var mu sync.Mutex
	observed := map[string]float64{}
	puller := federation.NewPuller(federation.PullerConfig{
		Broker:   dst,
		Store:    store,
		Interval: time.Millisecond,
		Dial: func(ctx context.Context, peer federation.Peer) (*grpc.ClientConn, error) {
			return nil, fmt.Errorf("no route to peer") // unreachable
		},
		LagSeconds: func(peer, topic string, partition int32, lag float64) {
			mu.Lock()
			observed[fmt.Sprintf("%s/%s", peer, topic)] = lag
			mu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go puller.Run(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()
	puller.Stop()

	mu.Lock()
	defer mu.Unlock()
	lag, ok := observed["src/orders"]
	if !ok {
		t.Fatal("no lag observation recorded for unreachable peer")
	}
	if lag <= 0 {
		t.Fatalf("lag must grow while the peer is unreachable, got %v", lag)
	}
}
