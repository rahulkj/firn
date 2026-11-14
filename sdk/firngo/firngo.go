// Package firngo is the Go client SDK for Firn (ADR-013 slice A).
//
// Wrap your program around this package instead of the raw protobuf stubs:
//
//	c, err := firngo.DialInsecure(ctx, "localhost:9461")            // dev only
//	c, err := firngo.DialProduction(ctx, "firn.example.com:9461", "ev-xxx") // TLS via system roots
//	_, _, err = c.Produce(ctx, "orders", key, value)
//	_, err = c.ProduceBatch(ctx, "orders", []firngo.KeyValue{{Key: key, Value: value}})
//	recs, err := c.Fetch(ctx, "orders", 0, 0, 100)
//	c.SubscribeInPartition(ctx, "orders", 0, 0, 100, func(r Record) error {
//	    fmt.Printf("%s %s\n", r.Key, r.Value)
//	    return nil
//	})
package firngo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/rahulkj/firn/internal/api/firnpb"
)

// Record is one message.
type Record struct {
	Offset int64
	Key    []byte
	Value  []byte
}

// Client is a connected SDK handle. Safe to use from multiple goroutines.
type Client struct {
	conn *grpc.ClientConn
	fc   firnpb.FirnServiceClient
	key  string // empty = broker -dev (no auth)
}

// DialInsecure dials plaintext (development brokers started with -dev, no keys).
// Never use this for real traffic.
func DialInsecure(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		addr = "localhost:9461"
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("firngo: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, fc: firnpb.NewFirnServiceClient(conn)}, nil
}

// DialProduction dials with TLS and pins the server certificate to one or more CAs.
// caPool must contain the CA that signed the broker's server cert.
func DialProduction(ctx context.Context, addr, apiKey string, caPool *x509.CertPool) (*Client, error) {
	if addr == "" || apiKey == "" {
		return nil, errors.New("firngo: addr and apiKey are required")
	}
	cfg := &tls.Config{RootCAs: caPool, MinVersion: tls.VersionTLS12}
	creds := credentials.NewTLS(cfg)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("firngo: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, fc: firnpb.NewFirnServiceClient(conn), key: apiKey}, nil
}

// DialWithTLS is a convenience for clients that already have a tls.Config.
func DialWithTLS(ctx context.Context, addr, apiKey string, cfg *tls.Config) (*Client, error) {
	creds := credentials.NewTLS(cfg)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("firngo: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, fc: firnpb.NewFirnServiceClient(conn), key: apiKey}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) authCtx(ctx context.Context) context.Context {
	if c.key == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.key)
}

func timeAdd(d time.Duration) <-chan time.Time { return time.After(d) }

// Produce appends a record under key-based routing, or round-robin when key is nil.
func (c *Client) Produce(ctx context.Context, topic string, key, value []byte) (partition int32, offset int64, err error) {
	resp, err := c.fc.Produce(c.authCtx(ctx), &firnpb.ProduceRequest{Topic: topic, Key: key, Value: value})
	if err != nil {
		return 0, 0, fmt.Errorf("firngo: produce: %w", err)
	}
	return resp.Partition, resp.Offset, nil
}

// KeyValue is one record for ProduceBatch. Key nil = round-robin across partitions.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// BatchResult is where one ProduceBatch record landed.
type BatchResult struct {
	Partition int32
	Offset    int64
}

// ProduceBatch appends up to 500 records with one RPC (vs one RPC per
// record). Per-partition ordering matches request order. NOT atomic: on
// error, records before the failing one are durable and records after are
// not — retry the whole batch; at-least-once may duplicate earlier records
// at new offsets.
func (c *Client) ProduceBatch(ctx context.Context, topic string, records []KeyValue) ([]BatchResult, error) {
	if len(records) == 0 {
		return nil, errors.New("firngo: ProduceBatch requires at least 1 record")
	}
	if len(records) > 500 {
		return nil, errors.New("firngo: ProduceBatch is limited to 500 records per call")
	}
	in := make([]*firnpb.ProduceBatchRecord, 0, len(records))
	for _, r := range records {
		in = append(in, &firnpb.ProduceBatchRecord{Key: r.Key, Value: r.Value})
	}
	resp, err := c.fc.ProduceBatch(c.authCtx(ctx), &firnpb.ProduceBatchRequest{Topic: topic, Records: in})
	if err != nil {
		return nil, fmt.Errorf("firngo: produce batch: %w", err)
	}
	out := make([]BatchResult, 0, len(resp.Records))
	for _, r := range resp.Records {
		out = append(out, BatchResult{Partition: r.Partition, Offset: r.Offset})
	}
	return out, nil
}

// Fetch reads up to max records starting at offset.
func (c *Client) Fetch(ctx context.Context, topic string, partition int32, offset int64, max int) ([]Record, error) {
	resp, err := c.fc.Fetch(c.authCtx(ctx), &firnpb.FetchRequest{Topic: topic, Partition: partition, Offset: offset, MaxRecords: int32(max)})
	if err != nil {
		return nil, fmt.Errorf("firngo: fetch: %w", err)
	}
	out := make([]Record, 0, len(resp.Records))
	for _, r := range resp.Records {
		out = append(out, Record{Offset: r.Offset, Key: append([]byte(nil), r.Key...), Value: append([]byte(nil), r.Value...)})
	}
	return out, nil
}

// TopicMetadata returns the broker's view of the topic.
func (c *Client) TopicMetadata(ctx context.Context, topic string) (*firnpb.TopicMetadata, error) {
	resp, err := c.fc.GetMetadata(c.authCtx(ctx), &firnpb.GetMetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("firngo: get metadata: %w", err)
	}
	for _, t := range resp.Topics {
		if t.Name == topic {
			return t, nil
		}
	}
	return nil, fmt.Errorf("firngo: topic %q not on broker", topic)
}

// Health reports whether the broker can serve reads (metadata call round-trips).
func (c *Client) Health(ctx context.Context) error {
	_, err := c.fc.GetMetadata(c.authCtx(ctx), &firnpb.GetMetadataRequest{})
	return err
}

// SubscribePartition is a simple at-least-once poll loop over one partition.
// handlers returning errors stop the subscriber; skipped records are reported by handler.
// For durable progress, use CommitOffset — this loop is intentionally minimal.
func (c *Client) SubscribePartition(ctx context.Context, topic string, partition int32, startOffset int64, maxBatch int, handler func(Record) error) error {
	off := startOffset
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		recs, err := c.Fetch(ctx, topic, partition, off, maxBatch)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timeAdd(200):
			}
			continue
		}
		for _, r := range recs {
			if err := handler(r); err != nil {
				return err
			}
			off = r.Offset + 1
		}
	}
}

// CommitOffset commits a group offset (idempotent under consumer-group rebalance).
func (c *Client) CommitOffset(ctx context.Context, groupID, topic string, partition int32, offset int64) error {
	_, err := c.fc.CommitOffset(c.authCtx(ctx), &firnpb.CommitOffsetRequest{
		GroupId:      groupID,
		Topic:        topic,
		Partition:    partition,
		Offset:       offset,
		GenerationId: 1,
	})
	if err != nil {
		return fmt.Errorf("firngo: commit offset: %w", err)
	}
	return nil
}

// JoinGroup asks the broker to rebalance; the response includes the member's assignment.
func (c *Client) JoinGroup(ctx context.Context, groupID, memberID string, topics []string) (*firnpb.JoinGroupResponse, error) {
	resp, err := c.fc.JoinGroup(c.authCtx(ctx), &firnpb.JoinGroupRequest{
		GroupId:  groupID,
		MemberId: memberID,
		Topics:   topics,
		Protocol: "range",
	})
	if err != nil {
		return nil, fmt.Errorf("firngo: join group: %w", err)
	}
	return resp, nil
}

// Delivery is one claimed queue message with its opaque delivery id.
type Delivery struct {
	DeliveryID string
	Queue      string
	Partition  int32
	Offset     int64
	Key        []byte
	Value      []byte
	Attempts   int32
	DeadlineMs int64
}

// Pull claims up to maxMessages deliveries from queue for the visibility
// timeout. maxMessages == 0 selects the server default (10).
func (c *Client) Pull(ctx context.Context, queue string, maxMessages int, visibilityTimeoutMs int64) ([]Delivery, error) {
	resp, err := c.fc.Pull(c.authCtx(ctx), &firnpb.PullRequest{
		Queue:               queue,
		MaxMessages:         int32(maxMessages),
		VisibilityTimeoutMs: visibilityTimeoutMs,
	})
	if err != nil {
		return nil, fmt.Errorf("firngo: pull: %w", err)
	}
	out := make([]Delivery, 0, len(resp.Deliveries))
	for _, d := range resp.Deliveries {
		out = append(out, Delivery{
			DeliveryID: d.DeliveryId,
			Queue:      d.Queue,
			Partition:  d.Partition,
			Offset:     d.Offset,
			Key:        append([]byte(nil), d.Key...),
			Value:      append([]byte(nil), d.Value...),
			Attempts:   d.Attempts,
			DeadlineMs: d.DeadlineMs,
		})
	}
	return out, nil
}

// Ack settles one delivery by id.
func (c *Client) Ack(ctx context.Context, queue, deliveryID string) error {
	_, err := c.fc.Ack(c.authCtx(ctx), &firnpb.AckRequest{Queue: queue, DeliveryId: deliveryID})
	if err != nil {
		return fmt.Errorf("firngo: ack: %w", err)
	}
	return nil
}

// AckBatchResult reports how many deliveries were settled and how many
// ids were unknown/already-settled.
type AckBatchResult struct {
	Acked   int
	Unknown int
}

// AckBatch settles up to 500 deliveries in one RPC. Unknown or
// already-settled ids are counted, not failed.
func (c *Client) AckBatch(ctx context.Context, queue string, deliveryIDs []string) (AckBatchResult, error) {
	if len(deliveryIDs) == 0 {
		return AckBatchResult{}, fmt.Errorf("firngo: ack batch requires at least 1 delivery id")
	}
	if len(deliveryIDs) > 500 {
		return AckBatchResult{}, fmt.Errorf("firngo: ack batch is limited to 500 ids")
	}
	resp, err := c.fc.AckBatch(c.authCtx(ctx), &firnpb.AckBatchRequest{Queue: queue, DeliveryIds: deliveryIDs})
	if err != nil {
		return AckBatchResult{}, fmt.Errorf("firngo: ack batch: %w", err)
	}
	return AckBatchResult{Acked: int(resp.Acked), Unknown: int(resp.Unknown)}, nil
}
