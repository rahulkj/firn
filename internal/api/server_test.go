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

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/queue"
)

// newTestCluster wires an in-memory broker behind a bufconn gRPC server and
// an httptest HTTP admin server — the same wiring firnd uses, minus the
// network listeners.
func newTestCluster(t *testing.T) (firnpb.FirnServiceClient, *httptest.Server, *meta.Registry) {
	t.Helper()

	reg := meta.NewRegistry()
	b := broker.New(reg)
	q, err := queue.NewManager(reg, b, "")
	if err != nil {
		t.Fatalf("queue manager: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	firnpb.RegisterFirnServiceServer(grpcServer, NewServer(b, q))
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

	return firnpb.NewFirnServiceClient(conn), admin, reg
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
		p, err := client.Produce(ctx, &firnpb.ProduceRequest{
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
			f, err := client.Fetch(ctx, &firnpb.FetchRequest{
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
		p, err := client.Produce(ctx, &firnpb.ProduceRequest{
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
	_, err := client.Produce(context.Background(), &firnpb.ProduceRequest{
		Topic: "nope",
		Value: []byte("v"),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err = %v)", status.Code(err), err)
	}
}

func TestFetchUnknownTopicReturnsNotFound(t *testing.T) {
	client, _, _ := newTestCluster(t)
	_, err := client.Fetch(context.Background(), &firnpb.FetchRequest{
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
		req  *firnpb.FetchRequest
	}{
		{"zero max_records", &firnpb.FetchRequest{Topic: "events", Partition: 0, Offset: 0, MaxRecords: 0}},
		{"negative offset", &firnpb.FetchRequest{Topic: "events", Partition: 0, Offset: -1, MaxRecords: 1}},
		{"partition out of range", &firnpb.FetchRequest{Topic: "events", Partition: 1, Offset: 0, MaxRecords: 1}},
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
		if _, err := client.Produce(ctx, &firnpb.ProduceRequest{Topic: "jobs", Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	pull, err := client.Pull(ctx, &firnpb.PullRequest{Queue: "jobs", MaxMessages: 10, VisibilityTimeoutMs: 30000})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(pull.Deliveries) != 3 {
		t.Fatalf("pulled %d deliveries, want 3", len(pull.Deliveries))
	}
	for _, d := range pull.Deliveries {
		if _, err := client.Ack(ctx, &firnpb.AckRequest{Queue: "jobs", DeliveryId: d.DeliveryId}); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	// All messages acked: a new pull returns empty.
	pull, err = client.Pull(ctx, &firnpb.PullRequest{Queue: "jobs", MaxMessages: 10, VisibilityTimeoutMs: 30000})
	if err != nil {
		t.Fatalf("pull 2: %v", err)
	}
	if len(pull.Deliveries) != 0 {
		t.Fatalf("pull after ack returned %d deliveries, want 0", len(pull.Deliveries))
	}

	// Pull on a non-existent queue -> NotFound.
	if _, err := client.Pull(ctx, &firnpb.PullRequest{Queue: "nope"}); status.Code(err) != codes.NotFound {
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

	m1, err := client.JoinGroup(ctx, &firnpb.JoinGroupRequest{GroupId: "g1", Topics: []string{"orders"}, Protocol: "range"})
	if err != nil {
		t.Fatalf("m1 join: %v", err)
	}
	m2, err := client.JoinGroup(ctx, &firnpb.JoinGroupRequest{GroupId: "g1", Topics: []string{"orders"}, Protocol: "range"})
	if err != nil {
		t.Fatalf("m2 join: %v", err)
	}
	// m2's join triggers a rebalance, so m1's join-time generation is
	// stale by design; SyncGroup below returns the current one.
	if m2.GenerationId <= m1.GenerationId {
		t.Fatalf("expected generation to advance on join: m1=%d m2=%d", m1.GenerationId, m2.GenerationId)
	}

	s1, err := client.SyncGroup(ctx, &firnpb.SyncGroupRequest{GroupId: "g1", MemberId: m1.MemberId})
	if err != nil {
		t.Fatalf("m1 sync: %v", err)
	}
	s2, err := client.SyncGroup(ctx, &firnpb.SyncGroupRequest{GroupId: "g1", MemberId: m2.MemberId})
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
	if _, err := client.CommitOffset(ctx, &firnpb.CommitOffsetRequest{GroupId: "g1", Topic: "orders", Partition: 0, Offset: 42, GenerationId: m2.GenerationId}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	f, err := client.FetchOffset(ctx, &firnpb.FetchOffsetRequest{GroupId: "g1", Topic: "orders", Partition: 0})
	if err != nil {
		t.Fatalf("fetch offset: %v", err)
	}
	if !f.Found || f.Offset != 42 {
		t.Fatalf("fetched offset = %+v, want found=42", f)
	}

	// Stale generation must be rejected.
	if _, err := client.CommitOffset(ctx, &firnpb.CommitOffsetRequest{GroupId: "g1", Topic: "orders", Partition: 1, Offset: 7, GenerationId: 1}); status.Code(err) != codes.FailedPrecondition {
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
	if _, err := client.Heartbeat(ctx, &firnpb.HeartbeatRequest{GroupId: "g1", MemberId: m1.MemberId, GenerationId: 1}); status.Code(err) != codes.FailedPrecondition {
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

func TestProduceBatchGRPC(t *testing.T) {
	cli, _, reg := newTestCluster(t)
	if err := reg.Create("events", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	batch := make([]*firnpb.ProduceBatchRecord, 0, 10)
	for i := 0; i < 10; i++ {
		batch = append(batch, &firnpb.ProduceBatchRecord{Key: []byte(fmt.Sprintf("k-%d", i%2)), Value: []byte(fmt.Sprintf("v-%02d", i))})
	}
	resp, err := cli.ProduceBatch(context.Background(), &firnpb.ProduceBatchRequest{Topic: "events", Records: batch})
	if err != nil {
		t.Fatalf("produce batch: %v", err)
	}
	if len(resp.Records) != 10 {
		t.Fatalf("results = %d, want 10", len(resp.Records))
	}
	byPartition := map[int32][]*firnpb.ProduceBatchRecordResult{}
	for _, r := range resp.Records {
		byPartition[r.Partition] = append(byPartition[r.Partition], r)
	}
	for p, rs := range byPartition {
		for i := 1; i < len(rs); i++ {
			if rs[i].Offset <= rs[i-1].Offset {
				t.Fatalf("partition %d offsets not monotonic: %v", p, rs)
			}
		}
	}

	// Verify by fetching back: total record count per partition must match.
	total := 0
	for p := int32(0); p < 3; p++ {
		fr, err := cli.Fetch(context.Background(), &firnpb.FetchRequest{Topic: "events", Partition: p, Offset: 0, MaxRecords: 100})
		if err != nil {
			t.Fatalf("fetch partition %d: %v", p, err)
		}
		total += len(fr.Records)
	}
	if total != 10 {
		t.Fatalf("fetched %d records, produced 10", total)
	}

	// Empty and oversized batches rejected with InvalidArgument.
	if _, err := cli.ProduceBatch(context.Background(), &firnpb.ProduceBatchRequest{Topic: "events"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty batch: want InvalidArgument, got %v", err)
	}
	tooMany := make([]*firnpb.ProduceBatchRecord, 501)
	for i := range tooMany {
		tooMany[i] = &firnpb.ProduceBatchRecord{Value: []byte{byte(i)}}
	}
	if _, err := cli.ProduceBatch(context.Background(), &firnpb.ProduceBatchRequest{Topic: "events", Records: tooMany}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized batch: want InvalidArgument, got %v", err)
	}

	// Unknown topic is not silently accepted.
	if _, err := cli.ProduceBatch(context.Background(), &firnpb.ProduceBatchRequest{Topic: "nosuchtopic", Records: []*firnpb.ProduceBatchRecord{{Value: []byte("x")}}}); err == nil {
		t.Fatal("batch to unknown topic must fail")
	}
}

func TestAckBatchGRPC(t *testing.T) {
	cli, _, reg := newTestCluster(t)
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := cli.Produce(context.Background(), &firnpb.ProduceRequest{Topic: "jobs", Value: []byte(fmt.Sprintf("v-%d", i))}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	pull, err := cli.Pull(context.Background(), &firnpb.PullRequest{Queue: "jobs", MaxMessages: 5})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(pull.Deliveries) != 5 {
		t.Fatalf("pulled %d, want 5", len(pull.Deliveries))
	}
	ids := make([]string, 0, 6)
	for _, d := range pull.Deliveries {
		ids = append(ids, d.DeliveryId)
	}
	ids = append(ids, "bogus")
	resp, err := cli.AckBatch(context.Background(), &firnpb.AckBatchRequest{Queue: "jobs", DeliveryIds: ids})
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if resp.Acked != 5 || resp.Unknown != 1 {
		t.Fatalf("ack batch = {%d, %d}, want {5, 1}", resp.Acked, resp.Unknown)
	}
	resp2, err := cli.Pull(context.Background(), &firnpb.PullRequest{Queue: "jobs", MaxMessages: 1})
	if err != nil {
		t.Fatalf("pull of empty queue must not error: %v", err)
	}
	if len(resp2.Deliveries) != 0 {
		t.Fatalf("empty queue returned %d deliveries, want 0", len(resp2.Deliveries))
	}

	// Unknown topic / invalid batch carry the right codes.
	if _, err := cli.AckBatch(context.Background(), &firnpb.AckBatchRequest{Queue: "jobs"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty ids: want InvalidArgument, got %v", err)
	}
	if _, err := cli.AckBatch(context.Background(), &firnpb.AckBatchRequest{Queue: "plain", DeliveryIds: []string{"x"}}); err == nil {
		t.Fatal("AckBatch on a missing topic must fail")
	}
}

func TestGetMetadataReturnsTopicShape(t *testing.T) {
	cli, _, reg := newTestCluster(t)
	if err := reg.Create("events", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	resp, err := cli.GetMetadata(context.Background(), &firnpb.GetMetadataRequest{})
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if len(resp.Brokers) == 0 {
		t.Fatal("expected at least one broker in metadata")
	}
	var got *firnpb.TopicMetadata
	for _, tp := range resp.Topics {
		if tp.Name == "events" {
			got = tp
		}
	}
	if got == nil {
		t.Fatal("metadata missing 'events' topic")
	}
	if got.Partitions != 3 {
		t.Fatalf("partition count = %d, want 3", got.Partitions)
	}
	if len(got.PartitionsMeta) != 3 {
		t.Fatalf("partition_meta = %d entries, want 3", len(got.PartitionsMeta))
	}
	for i, pm := range got.PartitionsMeta {
		if pm.PartitionId != int32(i) {
			t.Fatalf("partition %d: id = %d", i, pm.PartitionId)
		}
	}

	// Empty request returns every topic.
	if err := reg.Create("other", 1); err != nil {
		t.Fatalf("create: %v", err)
	}
	resp2, err := cli.GetMetadata(context.Background(), &firnpb.GetMetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tp := range resp2.Topics {
		names[tp.Name] = true
	}
	if !names["events"] || !names["other"] {
		t.Fatalf("expected both topics in metadata, got %v", names)
	}
}

func TestRoutedBrokerServiceRoundTrip(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
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
	firnpb.RegisterRoutedBrokerServiceServer(srv, NewRoutedBrokerServer(b))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	cli := firnpb.NewRoutedBrokerServiceClient(conn)
	resp, err := cli.RoutedProduce(context.Background(), &firnpb.RoutedProduceRequest{Topic: "events", Key: []byte("k"), Value: []byte("vrouted")})
	if err != nil {
		t.Fatalf("routed produce: %v", err)
	}
	if resp.Partition < 0 || resp.Partition >= 2 {
		t.Fatalf("partition out of range: %d", resp.Partition)
	}
	recs, err := b.Fetch(context.Background(), "events", resp.Partition, resp.Offset, 1)
	if err != nil {
		t.Fatalf("fetch after routed produce: %v", err)
	}
	if len(recs) != 1 || string(recs[0].Value) != "vrouted" {
		t.Fatalf("routed record not readable: %v", recs)
	}
}
