package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rahulkj/slipway/internal/api/slipwaypb"
	"github.com/rahulkj/slipway/internal/broker"
	"github.com/rahulkj/slipway/internal/meta"
	"github.com/rahulkj/slipway/internal/queue"
)

// newTestCluster wires an in-memory broker behind a bufconn gRPC server and
// an httptest HTTP admin server — the same wiring slipwayd uses, minus the
// network listeners.
func newTestCluster(t *testing.T) (slipwaypb.SlipwayServiceClient, *httptest.Server, *meta.Registry) {
	t.Helper()

	reg := meta.NewRegistry()
	b := broker.New(reg)
	q, err := queue.NewManager(reg, b, "")
	if err != nil {
		t.Fatalf("queue manager: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	slipwaypb.RegisterSlipwayServiceServer(grpcServer, NewServer(b, q))
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	admin := httptest.NewServer(NewAdminHandler(reg, b.Groups(), q))
	t.Cleanup(admin.Close)

	return slipwaypb.NewSlipwayServiceClient(conn), admin, reg
}

func createTopic(t *testing.T, admin *httptest.Server, name string, partitions int) *http.Response {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": name, "partitions": partitions})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(admin.URL+"/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post /topics: %v", err)
	}
	return resp
}

// TestProduceFetchEndToEnd is the slice-1 acceptance test: produce and
// fetch messages through the real gRPC surface. Restart loses data by
// design (in-memory MemLog); see README.
func TestProduceFetchEndToEnd(t *testing.T) {
	client, admin, _ := newTestCluster(t)
	ctx := context.Background()

	resp := createTopic(t, admin, "events", 3)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic: status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	const total = 10_000
	produced := map[int32]int64{} // partition -> count
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i%97) // ~97 distinct keys across partitions
		value := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 8)
		p, err := client.Produce(ctx, &slipwaypb.ProduceRequest{
			Topic: "events",
			Key:   []byte(key),
			Value: value,
		})
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		if p.Offset != produced[p.Partition] {
			t.Fatalf("partition %d: offset = %d, want %d (monotonic from 0)", p.Partition, p.Offset, produced[p.Partition])
		}
		produced[p.Partition]++
	}

	var fetched int64
	for partition, count := range produced {
		for offset := int64(0); offset < count; {
			f, err := client.Fetch(ctx, &slipwaypb.FetchRequest{
				Topic:      "events",
				Partition:  partition,
				Offset:     offset,
				MaxRecords: 128,
			})
			if err != nil {
				t.Fatalf("fetch partition %d offset %d: %v", partition, offset, err)
			}
			if len(f.Records) == 0 {
				t.Fatalf("partition %d offset %d: got 0 records, want up to %d", partition, offset, count-offset)
			}
			for _, r := range f.Records {
				if r.Offset != offset {
					t.Fatalf("partition %d: record offset = %d, want %d", partition, r.Offset, offset)
				}
				offset++
			}
			fetched += int64(len(f.Records))
		}
	}
	if fetched != total {
		t.Fatalf("fetched %d records, want %d", fetched, total)
	}
}

func TestProduceKeyStickinessOverGRPC(t *testing.T) {
	client, admin, _ := newTestCluster(t)
	ctx := context.Background()

	resp := createTopic(t, admin, "sticky", 4)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic: status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	var first int32 = -1
	for i := 0; i < 20; i++ {
		p, err := client.Produce(ctx, &slipwaypb.ProduceRequest{
			Topic: "sticky",
			Key:   []byte("same-key"),
			Value: []byte(fmt.Sprintf("v%d", i)),
		})
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
		if first == -1 {
			first = p.Partition
			continue
		}
		if p.Partition != first {
			t.Fatalf("same key produced to partitions %d and %d", first, p.Partition)
		}
	}
}

func TestProduceUnknownTopicReturnsNotFound(t *testing.T) {
	client, _, _ := newTestCluster(t)
	_, err := client.Produce(context.Background(), &slipwaypb.ProduceRequest{
		Topic: "nope",
		Value: []byte("v"),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err = %v)", status.Code(err), err)
	}
}

func TestFetchUnknownTopicReturnsNotFound(t *testing.T) {
	client, _, _ := newTestCluster(t)
	_, err := client.Fetch(context.Background(), &slipwaypb.FetchRequest{
		Topic:      "nope",
		Partition:  0,
		Offset:     0,
		MaxRecords: 10,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err = %v)", status.Code(err), err)
	}
}

func TestFetchInvalidArgument(t *testing.T) {
	client, admin, _ := newTestCluster(t)
	resp := createTopic(t, admin, "events", 1)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic: status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	for _, tc := range []struct {
		name string
		req  *slipwaypb.FetchRequest
	}{
		{"zero max_records", &slipwaypb.FetchRequest{Topic: "events", Partition: 0, Offset: 0, MaxRecords: 0}},
		{"negative offset", &slipwaypb.FetchRequest{Topic: "events", Partition: 0, Offset: -1, MaxRecords: 1}},
		{"partition out of range", &slipwaypb.FetchRequest{Topic: "events", Partition: 1, Offset: 0, MaxRecords: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Fetch(context.Background(), tc.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument (err = %v)", status.Code(err), err)
			}
		})
	}
}

func TestPullAckOverGRPC(t *testing.T) {
	client, admin, _ := newTestCluster(t)
	ctx := context.Background()

	body, _ := json.Marshal(map[string]any{"name": "jobs", "partitions": 1, "mode": "queue"})
	resp, err := http.Post(admin.URL+"/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create queue: status %d", resp.StatusCode)
	}

	for i := 0; i < 3; i++ {
		if _, err := client.Produce(ctx, &slipwaypb.ProduceRequest{Topic: "jobs", Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	pull, err := client.Pull(ctx, &slipwaypb.PullRequest{Queue: "jobs", MaxMessages: 10, VisibilityTimeoutMs: 30000})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(pull.Deliveries) != 3 {
		t.Fatalf("pulled %d deliveries, want 3", len(pull.Deliveries))
	}
	for _, d := range pull.Deliveries {
		if _, err := client.Ack(ctx, &slipwaypb.AckRequest{Queue: "jobs", DeliveryId: d.DeliveryId}); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	// All messages acked: a new pull returns empty.
	pull, err = client.Pull(ctx, &slipwaypb.PullRequest{Queue: "jobs", MaxMessages: 10, VisibilityTimeoutMs: 30000})
	if err != nil {
		t.Fatalf("pull 2: %v", err)
	}
	if len(pull.Deliveries) != 0 {
		t.Fatalf("pull after ack returned %d deliveries, want 0", len(pull.Deliveries))
	}

	// Pull on a non-existent queue -> NotFound.
	if _, err := client.Pull(ctx, &slipwaypb.PullRequest{Queue: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("pull unknown queue: code = %v, want NotFound", status.Code(err))
	}
}

func TestGroupsEndToEndOverGRPC(t *testing.T) {
	client, admin, _ := newTestCluster(t)
	ctx := context.Background()

	resp := createTopic(t, admin, "orders", 3)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic: status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	m1, err := client.JoinGroup(ctx, &slipwaypb.JoinGroupRequest{GroupId: "g1", Topics: []string{"orders"}, Protocol: "range"})
	if err != nil {
		t.Fatalf("m1 join: %v", err)
	}
	m2, err := client.JoinGroup(ctx, &slipwaypb.JoinGroupRequest{GroupId: "g1", Topics: []string{"orders"}, Protocol: "range"})
	if err != nil {
		t.Fatalf("m2 join: %v", err)
	}
	// m2's join triggers a rebalance, so m1's join-time generation is
	// stale by design; SyncGroup below returns the current one.
	if m2.GenerationId <= m1.GenerationId {
		t.Fatalf("expected generation to advance on join: m1=%d m2=%d", m1.GenerationId, m2.GenerationId)
	}

	s1, err := client.SyncGroup(ctx, &slipwaypb.SyncGroupRequest{GroupId: "g1", MemberId: m1.MemberId})
	if err != nil {
		t.Fatalf("m1 sync: %v", err)
	}
	s2, err := client.SyncGroup(ctx, &slipwaypb.SyncGroupRequest{GroupId: "g1", MemberId: m2.MemberId})
	if err != nil {
		t.Fatalf("m2 sync: %v", err)
	}
	partitions := map[int32]bool{}
	for _, a := range s1.Assignment {
		for _, p := range a.Partitions {
			partitions[p] = true
		}
	}
	for _, a := range s2.Assignment {
		for _, p := range a.Partitions {
			partitions[p] = true
		}
	}
	if len(partitions) != 3 {
		t.Fatalf("assignments cover %d/3 partitions", len(partitions))
	}

	// Commit and read back through the API.
	if _, err := client.CommitOffset(ctx, &slipwaypb.CommitOffsetRequest{GroupId: "g1", Topic: "orders", Partition: 0, Offset: 42, GenerationId: m2.GenerationId}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	f, err := client.FetchOffset(ctx, &slipwaypb.FetchOffsetRequest{GroupId: "g1", Topic: "orders", Partition: 0})
	if err != nil {
		t.Fatalf("fetch offset: %v", err)
	}
	if !f.Found || f.Offset != 42 {
		t.Fatalf("fetched offset = %+v, want found=42", f)
	}

	// Stale generation must be rejected.
	if _, err := client.CommitOffset(ctx, &slipwaypb.CommitOffsetRequest{GroupId: "g1", Topic: "orders", Partition: 1, Offset: 7, GenerationId: 1}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale generation commit: code = %v, want FailedPrecondition", status.Code(err))
	}

	// HTTP admin: GET /groups lists g1; GET /groups/g1/offsets shows the commit.
	res, err := http.Get(admin.URL + "/groups")
	if err != nil {
		t.Fatalf("get /groups: %v", err)
	}
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if !bytes.Contains(body, []byte(`"g1"`)) {
		t.Fatalf("/groups does not list g1: %s", body)
	}
	res, err = http.Get(admin.URL + "/groups/g1/offsets")
	if err != nil {
		t.Fatalf("get offsets: %v", err)
	}
	body, _ = io.ReadAll(res.Body)
	_ = res.Body.Close()
	if !bytes.Contains(body, []byte(`"offset":42`)) {
		t.Fatalf("/groups/g1/offsets missing committed offset: %s", body)
	}

	// Heartbeat with stale generation must be rejected.
	if _, err := client.Heartbeat(ctx, &slipwaypb.HeartbeatRequest{GroupId: "g1", MemberId: m1.MemberId, GenerationId: 1}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale heartbeat: code = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestAdminCreateAndListTopics(t *testing.T) {
	_, admin, _ := newTestCluster(t)

	resp := createTopic(t, admin, "orders", 3)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Duplicate create -> 409.
	resp = createTopic(t, admin, "orders", 3)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate create: status %d, want 409", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Invalid partition count -> 400.
	resp = createTopic(t, admin, "bad", 0)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("partitions=0 create: status %d, want 400", resp.StatusCode)
	}
	_ = resp.Body.Close()

	res, err := http.Get(admin.URL + "/topics")
	if err != nil {
		t.Fatalf("get /topics: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	var list struct {
		Topics []struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitions"`
		} `json:"topics"`
	}
	if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
		t.Fatalf("decode /topics: %v", err)
	}
	if len(list.Topics) != 1 || list.Topics[0].Name != "orders" || list.Topics[0].Partitions != 3 {
		t.Fatalf("topics = %+v, want [orders/3]", list.Topics)
	}
}
