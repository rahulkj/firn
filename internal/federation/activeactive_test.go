package federation_test

// Active-active (full mode) integration tests: convergence both ways, echo
// prevention by the foreign-range invariant, fresh-sink full import,
// partition-count mismatch rejection. Both clusters run in one process over
// bufconn; FederationService is served with the real server-side echo filter.

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rahulkj/firn/internal/api"
	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/federation"
	"github.com/rahulkj/firn/internal/meta"
)

type cluster struct {
	broker *broker.Broker
	reg    *meta.Registry
	store  *federation.Store
	lis    *bufconn.Listener
	dir    string
}

func newClusterAt(t *testing.T, selfName string, topic string, partitions int, peerName string, peerAddr string, peerTopics []string, broker *broker.Broker, reg *meta.Registry, store *federation.Store, lis *bufconn.Listener, dir string) *cluster {
	t.Helper()
	c := &cluster{broker: broker, reg: reg, store: store, lis: lis, dir: dir}

	srv := grpc.NewServer()
	firnpb.RegisterFederationServiceServer(srv, api.NewFederationServer(broker, store))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	t.Cleanup(func() { _ = lis.Close() })

	mkPeer := func(name, addr string) federation.Peer {
		return federation.Peer{
			Name:      name,
			Addr:      addr,
			Direction: federation.DirectionBoth,
			Mode:      federation.ModeActiveActive,
			Key:       "peer-bearer",
			Selectors: []federation.Selector{{Topics: append([]string{}, peerTopics...)}},
		}
	}
	if peerName != "" {
		if err := store.AddPeer(mkPeer(peerName, peerAddr)); err != nil {
			t.Fatalf("register peer %q: %v", peerName, err)
		}
	}
	_ = selfName
	return c
}

func newPair(t *testing.T, topic string, partitions int) (a, b *cluster, aPuller, bPuller *federation.Puller) {
	t.Helper()
	return newPairWithMode(t, topic, partitions, "stream")
}

func newPairWithMode(t *testing.T, topic string, partitions int, mode string) (a, b *cluster, aPuller, bPuller *federation.Puller) {
	t.Helper()
	mk := func(name string) (*meta.Registry, *broker.Broker, string) {
		reg := meta.NewRegistry()
		if err := reg.CreateWithMode(topic, partitions, mode); err != nil {
			t.Fatalf("create topic on %s: %v", name, err)
		}
		dir := t.TempDir()
		b, err := broker.NewWithDir(reg, dir, broker.WithMaxSegmentBytes(1<<20))
		if err != nil {
			t.Fatalf("broker %s: %v", name, err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return reg, b, dir
	}
	aReg, aBr, aDir := mk("A")
	bReg, bBr, bDir := mk("B")

	aStore := federation.OpenInMemory()
	bStore := federation.OpenInMemory()
	aLis := bufconn.Listen(1 << 16)
	bLis := bufconn.Listen(1 << 16)

	dial := func(lis *bufconn.Listener) func(context.Context, federation.Peer) (*grpc.ClientConn, error) {
		return func(ctx context.Context, _ federation.Peer) (*grpc.ClientConn, error) {
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			return conn, err
		}
	}

	a = newClusterAt(t, "alpha", topic, partitions, "beta", "bufnet-b", []string{topic}, aBr, aReg, aStore, aLis, aDir)
	b = newClusterAt(t, "beta", topic, partitions, "alpha", "bufnet-a", []string{topic}, bBr, bReg, bStore, bLis, bDir)
	_ = aDir
	_ = bDir

	aPuller = federation.NewPuller(federation.PullerConfig{Broker: aBr, Store: aStore, Interval: time.Millisecond, SelfName: "alpha", Dial: dial(bLis)})
	bPuller = federation.NewPuller(federation.PullerConfig{Broker: bBr, Store: bStore, Interval: time.Millisecond, SelfName: "beta", Dial: dial(aLis)})
	go aPuller.Run(context.Background())
	go bPuller.Run(context.Background())
	t.Cleanup(aPuller.Stop)
	t.Cleanup(bPuller.Stop)
	return a, b, aPuller, bPuller
}

func keysOf(t *testing.T, b *broker.Broker, topic string) []string {
	t.Helper()
	hw, err := b.PartitionHighWatermark(context.Background(), topic, 0)
	if err != nil {
		t.Fatalf("hwm: %v", err)
	}
	recs, err := b.Fetch(context.Background(), topic, 0, 0, int(hw)+1)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, string(r.Key))
	}
	sort.Strings(out)
	return out
}

func TestActiveActiveConvergenceBothWays(t *testing.T) {
	a, b := mustClusters(t, "shared", 1)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, _, err := a.broker.Produce(ctx, "shared", []byte(fmt.Sprintf("a%d", i)), []byte(fmt.Sprintf("va%d", i))); err != nil {
			t.Fatalf("produce on A: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, _, err := b.broker.Produce(ctx, "shared", []byte(fmt.Sprintf("b%d", i)), []byte(fmt.Sprintf("vb%d", i))); err != nil {
			t.Fatalf("produce on B: %v", err)
		}
	}
	// Convergence barrier under full-suite -race CPU starvation: not a
	// correctness bound — a starved pair of pullers can take arbitrary time
	// to converge; 30s is generous scheduling headroom for CI.
	waitFor(t, 30*time.Second, func() bool {
		aHWM, err := a.broker.PartitionHighWatermark(ctx, "shared", 0)
		bHWM, err2 := b.broker.PartitionHighWatermark(ctx, "shared", 0)
		return err == nil && err2 == nil && aHWM == 5 && bHWM == 5
	})

	want := []string{"a0", "a1", "a2", "b0", "b1"}
	if got := keysOf(t, a.broker, "shared"); !equalStrings(got, want) {
		t.Fatalf("A converged to %v, want %v", got, want)
	}
	if got := keysOf(t, b.broker, "shared"); !equalStrings(got, want) {
		t.Fatalf("B converged to %v, want %v", got, want)
	}
}

func TestActiveActiveEchoPrevention(t *testing.T) {
	a, b := mustClusters(t, "shared", 1)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, _, err := a.broker.Produce(ctx, "shared", []byte(fmt.Sprintf("a%d", i)), []byte(fmt.Sprintf("va%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 15*time.Second, func() bool {
		bHWM, err := b.broker.PartitionHighWatermark(ctx, "shared", 0)
		return err == nil && bHWM == 3
	})
	// Several poll cycles with no new input: counts must stay stable.
	before := []int{len(keysOf(t, a.broker, "shared")), len(keysOf(t, b.broker, "shared"))}
	time.Sleep(300 * time.Millisecond)
	after := []int{len(keysOf(t, a.broker, "shared")), len(keysOf(t, b.broker, "shared"))}
	if before[0] != after[0] {
		t.Fatalf("A echoed records without input: %d -> %d", before[0], after[0])
	}
	if before[1] != after[1] {
		t.Fatalf("B echoed records without input: %d -> %d", before[1], after[1])
	}
	for i := 3; i < 5; i++ {
		if _, _, err := a.broker.Produce(ctx, "shared", []byte(fmt.Sprintf("a%d", i)), []byte(fmt.Sprintf("va%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 15*time.Second, func() bool {
		bHWM, err := b.broker.PartitionHighWatermark(ctx, "shared", 0)
		return err == nil && bHWM == 5
	})
	if got := keysOf(t, a.broker, "shared"); !equalStrings(got, []string{"a0", "a1", "a2", "a3", "a4"}) {
		t.Fatalf("A has duplicates or missing records: %v", got)
	}
}

func TestActiveActiveFreshSinkFullImport(t *testing.T) {
	a, b := mustClusters(t, "shared", 1)
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		if _, _, err := a.broker.Produce(ctx, "shared", []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 15*time.Second, func() bool {
		bHWM, err := b.broker.PartitionHighWatermark(ctx, "shared", 0)
		return err == nil && bHWM == 25
	})
	recs, err := b.broker.Fetch(ctx, "shared", 0, 0, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 25 {
		t.Fatalf("B has %d records, want 25", len(recs))
	}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("record %d re-origined to offset %d", i, r.Offset)
		}
		if string(r.Key) != fmt.Sprintf("k%02d", i) {
			t.Fatalf("record %d key = %q", i, r.Key)
		}
	}
}

func TestActiveActivePartitionMismatchRejected(t *testing.T) {
	// Sink A has "mismatch" at 1 partition; source B has it at 2.
	bReg := meta.NewRegistry()
	if err := bReg.Create("mismatch", 2); err != nil {
		t.Fatal(err)
	}
	bBr, err := broker.NewWithDir(bReg, t.TempDir(), broker.WithMaxSegmentBytes(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bBr.Close() }()

	aReg := meta.NewRegistry()
	if err := aReg.Create("mismatch", 1); err != nil {
		t.Fatal(err)
	}
	aBr, err := broker.NewWithDir(aReg, t.TempDir(), broker.WithMaxSegmentBytes(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aBr.Close() }()

	ctx := context.Background()
	if _, _, err := bBr.Produce(ctx, "mismatch", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	bStore := federation.OpenInMemory()
	aStore := federation.OpenInMemory()
	bLis := bufconn.Listen(1 << 16)
	defer func() { _ = bLis.Close() }()
	bSrv := grpc.NewServer()
	firnpb.RegisterFederationServiceServer(bSrv, api.NewFederationServer(bBr, bStore))
	go func() { _ = bSrv.Serve(bLis) }()
	defer bSrv.Stop()

	if err := aStore.AddPeer(federation.Peer{
		Name: "beta", Addr: "bufnet-b", Direction: federation.DirectionIn,
		Mode: federation.ModeActiveActive, Key: "k",
		Selectors: []federation.Selector{{Topics: []string{"mismatch"}}},
	}); err != nil {
		t.Fatal(err)
	}
	puller := federation.NewPuller(federation.PullerConfig{
		Broker:   aBr,
		Store:    aStore,
		Interval: time.Millisecond,
		SelfName: "alpha",
		Dial: func(ctx context.Context, _ federation.Peer) (*grpc.ClientConn, error) {
			return grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return bLis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	})
	go puller.Run(ctx)
	defer puller.Stop()

	time.Sleep(300 * time.Millisecond)
	hw, err := aBr.PartitionHighWatermark(ctx, "mismatch", 0)
	if err != nil {
		t.Fatal(err)
	}
	if hw != 0 {
		t.Fatalf("partition mismatch imported %d frames; want 0", hw)
	}
	if got := aStore.ListCursors(); len(got) != 0 {
		t.Fatalf("partition mismatch advanced cursors: %v", got)
	}
}

// mustClusters is a convenience wrapper: one fresh pair per test.
func mustClusters(t *testing.T, topic string, partitions int) (a, b *cluster) {
	t.Helper()
	a, b, _, _ = newPair(t, topic, partitions)
	return a, b
}

// mustQueueClusters is mustClusters with a queue-mode topic.
func mustQueueClusters(t *testing.T, topic string, partitions int) (a, b *cluster) {
	t.Helper()
	a, b, _, _ = newPairWithMode(t, topic, partitions, "queue")
	return a, b
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
