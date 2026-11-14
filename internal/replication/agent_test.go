package replication

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rahulkj/firn/internal/api"
	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/quorum"
)

// clusterNode bundles one broker + quorum voter + bufconn replica endpoint.
type clusterNode struct {
	id      string
	reg     *meta.Registry
	q       *quorum.Node
	b       *broker.Broker
	l       *bufconn.Listener
	srv     *grpc.Server
	cancel  context.CancelFunc
	dataDir string
}

func newCluster(t *testing.T, n int, root string) []*clusterNode {
	t.Helper()
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = fmt.Sprintf("n%d", i+1)
	}
	reg := meta.NewRegistry()
	nodes := make([]*clusterNode, 0, n)
	listeners := map[string]*bufconn.Listener{}
	for _, id := range peers {
		l := bufconn.Listen(1 << 20)
		listeners[id] = l

		dir := fmt.Sprintf("%s/%s", root, id)
		b, err := broker.NewWithDir(reg, dir,
			broker.WithMaxSegmentBytes(256),
			broker.WithBlockSealing(30*time.Millisecond))
		if err != nil {
			t.Fatalf("broker: %v", err)
		}
		q, err := quorum.NewNode(id, peers, fmt.Sprintf("%s/q", dir), quorum.NewLocalTransport(id), &quorum.NodeOptions{
			ElectionTimeout:   150 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("quorum: %v", err)
		}
		quorum.RegisterLocalNode(q)
		q.Start()

		srv := grpc.NewServer(grpc.MaxRecvMsgSize(80 << 20))
		firnpb.RegisterReplicaServiceServer(srv, api.NewReplicaServer(b))
		go func() { _ = srv.Serve(l) }()
		t.Cleanup(srv.Stop)

		cn := &clusterNode{id: id, reg: reg, q: q, b: b, l: l, srv: srv, dataDir: dir}
		b.SetAdvisor(NewAdvisor(id, q, time.Second))
		nodes = append(nodes, cn)
	}

	// Wire agents with bufconn dialers.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	for _, cn := range nodes {
		peersExcl := []string{}
		dialMap := map[string]*bufconn.Listener{}
		for id, l := range listeners {
			if id != cn.id {
				peersExcl = append(peersExcl, id)
			}
			dialMap[id] = l
		}
		ctx, cancel := context.WithCancel(context.Background())
		cn.cancel = cancel
		go NewAgent(Config{
			NodeID:   cn.id,
			Peers:    peersExcl,
			All:      peers,
			Registry: reg,
			Broker:   cn.b,
			Quorum:   cn.q,
			DataDir:  cn.dataDir,
			Dial: func(c context.Context, addr string) (*grpc.ClientConn, error) {
				listener := dialMap[addr]
				return grpc.NewClient("passthrough://"+addr,
					grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
					grpc.WithTransportCredentials(insecure.NewCredentials()))
			},
			LeaseTTL: time.Second,
			Interval: 50 * time.Millisecond,
		}).Run(ctx)
	}

	t.Cleanup(func() {
		for _, cn := range nodes {
			cn.cancel()
			quorum.UnregisterLocalNode(cn.q)
			cn.q.Close()
			_ = cn.b.Close()
		}
	})
	return nodes
}

func TestReplicationAndFailover(t *testing.T) {
	root := t.TempDir()
	nodes := newCluster(t, 3, root)

	if err := nodes[0].reg.CreateWithMode("failover", 2, meta.ModeStream); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Wait for a writable node for both partitions (the awardee applies the
	// grant asynchronously, so poll via the same advisor the produce path uses).
	holderFor := func(p int32) *clusterNode {
		for _, cn := range nodes {
			if NewAdvisor(cn.id, cn.q, time.Second).WriteAllowed("failover", p) {
				return cn
			}
		}
		return nil
	}
	waitFor(t, "a writable node per partition", 30*time.Second, func() bool {
		for p := int32(0); p < 2; p++ {
			if holderFor(p) == nil {
				return false
			}
		}
		return true
	})

	// Produce through each partition's lease holder.
	ctx := context.Background()
	for partition := int32(0); partition < 2; partition++ {
		holder := holderFor(partition)
		if holder == nil {
			t.Fatalf("no holder for partition %d", partition)
		}
		fmt.Printf("partition %d lease holder: %s\n", partition, holder.id)
		for i := int32(0); i < 10; i++ {
			if _, _, err := holder.b.Produce(ctx, "failover", nil, []byte(fmt.Sprintf("p%d-v%d", partition, i))); err != nil {
				t.Fatalf("produce p%d: %v", partition, err)
			}
		}
	}

	// Force blocks to seal and replicate: wait for commits visible everywhere.
	waitFor(t, "blocks committed everywhere", 30*time.Second, func() bool {
		for _, cn := range nodes {
			blocks := cn.q.Blocks("failover", 0)
			if len(blocks) == 0 {
				return false
			}
			blocks = cn.q.Blocks("failover", 1)
			if len(blocks) == 0 {
				return false
			}
		}
		return true
	})

	// Byte-compare replica raw frames for partition 0 up to the smallest
	// high watermark (the unsealed tail is never replicated — by design).
	var minHW int64 = 1 << 62
	for _, cn := range nodes {
		hw, err := cn.b.PartitionHighWatermark(ctx, "failover", 0)
		if err != nil {
			t.Fatalf("hwm on %s: %v", cn.id, err)
		}
		if hw < minHW {
			minHW = hw
		}
	}
	if minHW < 1 {
		t.Fatal("no replicated records found on any node")
	}
	rawByNode := map[string][]byte{}
	for _, cn := range nodes {
		raw, err := cn.b.ReadRange(ctx, "failover", 0, 0, minHW-1)
		if err != nil {
			t.Fatalf("read range on %s: %v", cn.id, err)
		}
		rawByNode[cn.id] = raw
	}
	base := rawByNode["n1"]
	for id, raw := range rawByNode {
		if !bytes.Equal(raw, base) {
			t.Fatalf("replica %s diverges (len %d vs %d)", id, len(raw), len(base))
		}
	}
	t.Logf("replica frames identical across %d nodes over offsets [0,%d) (%d bytes)", len(rawByNode), minHW, len(base))
}

// TestFailoverToSurvivor: the lease holder dies; the quorum leader takes
// the lease over after expiry; producers against the dead holder are fenced;
// the new holder produces and replication converges.
func TestFailoverToSurvivor(t *testing.T) {
	root := t.TempDir()
	nodes := newCluster(t, 3, root)

	if err := nodes[0].reg.CreateWithMode("failover", 1, meta.ModeStream); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	waitFor(t, "a node permits writes", 5*time.Second, func() bool {
		for _, cn := range nodes {
			if NewAdvisor(cn.id, cn.q, time.Second).WriteAllowed("failover", 0) {
				return true
			}
		}
		return false
	})

	// Produce through whichever node currently permits writes (mirrors
	// client failover semantics: nodes that lack the lease reject).
	holder := func() *clusterNode {
		for _, cn := range nodes {
			if NewAdvisor(cn.id, cn.q, time.Second).WriteAllowed("failover", 0) {
				return cn
			}
		}
		return nil
	}()
	if holder == nil {
		t.Fatal("no node permits writes for partition 0")
	}
	holderID := holder.id
	t.Logf("lease holder (writable): %s", holderID)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, _, err := holder.b.Produce(ctx, "failover", nil, []byte(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Pre-failover data must be replicated+committed everywhere BEFORE the
	// holder dies — otherwise the tail is unrecoverable (by design).
	waitFor(t, "pre-failover data replicated", 30*time.Second, func() bool {
		for _, cn := range nodes {
			hw, err := cn.b.PartitionHighWatermark(ctx, "failover", 0)
			if err != nil || hw != 5 {
				return false
			}
		}
		return true
	})

	// Kill the holder completely: replicator, broker, and the quorum voter.
	// This is the honest crash model — the node is gone, so re-election
	// can happen and lease takeover is real.
	holder.cancel()
	quorum.UnregisterLocalNode(holder.q)
	holder.q.Close()
	if err := holder.b.Close(); err != nil {
		t.Fatalf("close holder: %v", err)
	}
	live := []*clusterNode{}
	for _, cn := range nodes {
		if cn != holder {
			live = append(live, cn)
		}
	}

	// The dead holder must be fenced once the lease expires.
	waitFor(t, "old holder fenced", 5*time.Second, func() bool {
		return !NewAdvisor(holder.id, holder.q, time.Second).WriteAllowed("failover", 0)
	})

	// Wait for a fresh lease to land on a survivor.
	waitFor(t, "lease transferred", 5*time.Second, func() bool {
		for _, cn := range live {
			if NewAdvisor(cn.id, cn.q, time.Second).WriteAllowed("failover", 0) {
				return true
			}
		}
		return false
	})

	// Find the new holder via its advisor (exactly one accepts writes).
	var newHolder *clusterNode
	for _, cn := range live {
		if NewAdvisor(cn.id, cn.q, time.Second).WriteAllowed("failover", 0) {
			newHolder = cn
			break
		}
	}
	if newHolder == nil {
		t.Fatal("no survivor took the lease")
	}
	t.Logf("new holder: %s localHWM(before pull)=%d", newHolder.id, func() int64 {
		if hw, err := newHolder.b.PartitionHighWatermark(ctx, "failover", 0); err == nil {
			return hw
		}
		return -1
	}())
	for i := 0; i < 5; i++ {
		if _, _, err := newHolder.b.Produce(ctx, "failover", nil, []byte(fmt.Sprintf("post-%d", i))); err != nil {
			t.Fatalf("produce on survivor %s: %v", newHolder.id, err)
		}
	}

	// The dead node's zombie writes must be rejected.
	if _, _, err := holder.b.Produce(ctx, "failover", nil, []byte("zombie")); err == nil {
		t.Fatal("zombie holder produced after lease transfer")
	}

	// All records are fetchable on the new holder.
	recs, err := newHolder.b.Fetch(ctx, "failover", 0, 0, 100)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 10 {
		t.Fatalf("records = %d, want 10", len(recs))
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
