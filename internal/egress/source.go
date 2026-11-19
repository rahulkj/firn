package egress

import (
	"context"
	"fmt"
	"time"

	"github.com/rahulkj/firn/sdk/firngo"
)

// TopicSource consumes configured topic partitions from one Firn topic.
// Resume position comes from the group's committed offsets (group is
// dedicated to this route; generation 0 commits — monotonic advance only).
const fetchMax = 256

type TopicSource struct {
	client  *firngo.Client
	topic   string
	parts   []int32
	group   string
	next    map[int32]int64
	pending []Delivery // unconfirmed batch; re-served until Confirm succeeds
}

func NewTopicSource(ctx context.Context, client *firngo.Client, topic string, partitions []int32, group string) (*TopicSource, error) {
	meta, err := client.TopicMetadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("egress: topic %q: %w", topic, err)
	}
	n := int(meta.Partitions)
	if n <= 0 {
		n = 1
	}
	parts := partitions
	if len(parts) == 0 {
		parts = make([]int32, 0, n)
		for i := 0; i < n; i++ {
			parts = append(parts, int32(i))
		}
	} else {
		for _, p := range parts {
			if int(p) >= n {
				return nil, fmt.Errorf("egress: route topic %q partition %d out of range (%d partitions)", topic, p, n)
			}
		}
	}
	ts := &TopicSource{client: client, topic: topic, parts: parts, group: group, next: map[int32]int64{}}
	for _, p := range parts {
		off, found, err := client.FetchOffset(ctx, group, topic, p)
		if err != nil {
			return nil, fmt.Errorf("egress: resume offset %q/%s/%d: %w", group, topic, p, err)
		}
		if !found {
			off = 0
		}
		ts.next[p] = off
	}
	return ts, nil
}

func (t *TopicSource) Name() string { return "topic:" + t.topic }

// Lag returns, per partition, how many records remain unread from the
// committed position (scanned, capped at scanCap). Capped estimates are
// returned as-is; callers should disambiguate with a scan cap log line.
func (t *TopicSource) Lag(ctx context.Context, scanCap int) (map[int32]int64, error) {
	if scanCap <= 0 {
		scanCap = 10000
	}
	out := map[int32]int64{}
	for _, p := range t.parts {
		off := t.next[p]
		n := int64(0)
		for n < int64(scanCap) {
			recs, err := t.client.Fetch(ctx, t.topic, p, off, 500)
			if err != nil {
				break
			}
			if len(recs) == 0 {
				break
			}
			n += int64(len(recs))
			off = recs[len(recs)-1].Offset + 1
			if len(recs) < 500 {
				break
			}
		}
		out[p] = n
	}
	return out, nil
}

// Topic returns the source topic name.
func (t *TopicSource) Topic() string { return t.topic }

func (t *TopicSource) Next(ctx context.Context) ([]Delivery, error) {
	if len(t.pending) > 0 {
		return t.pending, nil // unconfirmed batch is re-served until Confirm
	}
	batchCap := fetchMax
	var out []Delivery
	deadline := time.Now().Add(100 * time.Millisecond)
	// Collect across partitions from staged offsets WITHOUT advancing the
	// committed position; the batch is only visible to the sink once.
	staged := map[int32]int64{}
	for _, p := range t.parts {
		staged[p] = t.next[p]
	}
	for {
		progressed := false
		for _, p := range t.parts {
			if len(out) >= batchCap {
				break
			}
			recs, err := t.client.Fetch(ctx, t.topic, p, staged[p], batchCap-len(out))
			if err != nil {
				return nil, err
			}
			for _, r := range recs {
				out = append(out, Delivery{Key: r.Key, Value: r.Value, Offset: r.Offset, Partition: p})
			}
			if len(recs) > 0 {
				progressed = true
				staged[p] = recs[len(recs)-1].Offset + 1
			}
		}
		if len(out) == 0 {
			if time.Now().After(deadline) {
				return nil, nil // no data; caller backs off
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		if !progressed || time.Now().After(deadline) {
			break
		}
	}
	t.pending = out
	return out, nil
}

func (t *TopicSource) Confirm(ctx context.Context, batch []Delivery) error {
	maxPerPart := map[int32]int64{}
	for _, d := range batch {
		if d.Offset+1 > maxPerPart[d.Partition] {
			maxPerPart[d.Partition] = d.Offset + 1
		}
	}
	for p, off := range maxPerPart {
		if err := t.client.CommitOffsetGeneration(ctx, t.group, t.topic, p, off, 0); err != nil {
			return fmt.Errorf("egress: commit %s/%d @%d: %w", t.topic, p, off, err)
		}
	}
	for p, off := range maxPerPart {
		t.next[p] = off
	}
	t.pending = nil
	return nil
}

// QueueSource pulls from one Firn queue. Delivery ids are settled with AckBatch.
type QueueSource struct {
	client *firngo.Client
	queue  string
	max    int
	visMs  int64
}

func NewQueueSource(client *firngo.Client, queue string, max int, visibilityTimeoutMs int64) *QueueSource {
	if max <= 0 || max > 500 {
		max = 100
	}
	if visibilityTimeoutMs <= 0 {
		visibilityTimeoutMs = 30000
	}
	return &QueueSource{client: client, queue: queue, max: max, visMs: visibilityTimeoutMs}
}

func (q *QueueSource) Name() string { return "queue:" + q.queue }

func (q *QueueSource) Next(ctx context.Context) ([]Delivery, error) {
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		ds, err := q.client.Pull(ctx, q.queue, q.max, q.visMs)
		if err != nil {
			return nil, err
		}
		if len(ds) > 0 {
			out := make([]Delivery, 0, len(ds))
			for _, d := range ds {
				out = append(out, Delivery{Key: d.Key, Value: d.Value, DeliveryID: d.DeliveryID, Partition: d.Partition, Offset: d.Offset})
			}
			return out, nil
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (q *QueueSource) Confirm(ctx context.Context, batch []Delivery) error {
	ids := make([]string, 0, len(batch))
	for _, d := range batch {
		if d.DeliveryID != "" {
			ids = append(ids, d.DeliveryID)
		}
	}
	for len(ids) > 0 {
		chunk := ids
		if len(chunk) > 500 {
			chunk = ids[:500]
		}
		if _, err := q.client.AckBatch(ctx, q.queue, chunk); err != nil {
			return fmt.Errorf("egress: ack %s (%d ids): %w", q.queue, len(chunk), err)
		}
		ids = ids[len(chunk):]
	}
	return nil
}
