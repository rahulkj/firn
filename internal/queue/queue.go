// Package queue implements work-queue semantics on top of firn's
// topics: competing consumers, pull-with-visibility-timeout, ack, and a
// dead-letter queue after maxDeliveries failed deliveries.
//
// Delivery model (docs/plans/001-mvp-single-node.md, slice 5):
//
//   - Topics created with mode "queue" are shared queues.
//   - Pull claims unacked messages; each claim starts a visibility lease
//     (default 30s). Other Pulls must not return claimed messages while
//     the lease holds.
//   - Ack consumes the message by advancing the internal consumer group
//     `queue-<name>` over the contiguously acked prefix.
//   - Lease expiry returns the message to the claimable set; the attempt
//     counter (persisted in __queue_attempts) is incremented on every claim.
//   - After maxDeliveries (3) attempts the message is diverted to
//     <queue>.dlq (auto-created, mode stream) instead of being delivered.
//
// Guarantees: at-least-once, per-message; ordering is best-effort under
// concurrent consumers. Deadline state is in-memory: a restart loses
// in-flight claims, which become re-visible with attempts preserved —
// at-least-once is preserved across restarts.
package queue

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/log"
	"github.com/rahulkj/firn/internal/meta"
)

const (
	// maxDeliveries bounds delivery attempts before a message goes to the DLQ.
	maxDeliveries = 3

	defaultMaxMessages = 10
	hardMaxMessages    = 500
	defaultVisibility  = 30 * time.Second
)

// ErrUnknownDelivery is returned when Ack references an unknown/expired id.
var ErrUnknownDelivery = errors.New("firn/queue: unknown delivery id")

// ErrFederatedPassive is returned when a Pull/Ack targets a queue that is
// federated in passive mode on this cluster — the delivery engine stays
// idle until the operator cuts over (ADR-010).
var ErrFederatedPassive = errors.New("firn/queue: queue is federated passively; delivery is fenced until cutover")

// ErrQueueNotFound / ErrInvalidQueue complement the meta errors with
// queue-specific context. Map to codes.NotFound / codes.InvalidArgument.
var (
	ErrQueueNotFound  = errors.New("firn/queue: queue not found")
	ErrNotAQueue      = errors.New("firn/queue: topic is not a queue")
	ErrInvalidRequest = errors.New("firn/queue: invalid request")
)

// Delivery is one claimed message returned by Pull.
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

type pending struct {
	partition int32
	offset    int64
	attempts  int32
	deadline  time.Time
	key       []byte
	value     []byte
}

type redelivery struct {
	partition int32
	offset    int64
	attempts  int32
	key       []byte
	value     []byte
}

type queueState struct {
	partitions int
	nextFetch  []int64                  // in-memory read cursor, seeded from committed offsets
	committed  []int64                  // last committed offset per partition (next offset to consume)
	acked      map[int32]map[int64]bool // acked but not yet committable
	requeue    []redelivery             // lease-expired messages awaiting re-delivery
	pending    map[string]*pending      // delivery id -> in-flight claims
	rr         int                      // round-robin partition cursor
}

// Manager owns queue state. It is safe for concurrent use.
type Manager struct {
	reg      *meta.Registry
	broker   *broker.Broker
	attempts *AttemptStore

	now func() time.Time

	// fedFence is the passive-federation sink fence (ADR-010): queues in
	// scope of a passive inbound peer refuse delivery and acks on this
	// cluster until the operator cuts over. Nil = no fence. Late-bound.
	fedFence func(queue string) bool

	mu     sync.Mutex
	queues map[string]*queueState
}

// Attempts exposes the folded delivery-attempt count for
// (queue, partition, offset) — including mirrored attempts from federation
// peers. Test/inspection surface; concurrent-safe.
func (m *Manager) Attempts(queue string, partition int32, offset int64) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attempts.Attempts(queue, partition, offset)
}

// SetFederationFence binds the passive-federation queue delivery fence.
// Late-bound by the daemon after peer registration is wired; nil fence
// means every queue delivers normally (single-node default).
func (m *Manager) SetFederationFence(f func(queue string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fedFence = f
}

// NewManager builds queue state for the queue-mode topics in reg and seeds
// partition cursors from the committed offsets of the internal queue groups.
func NewManager(reg *meta.Registry, b *broker.Broker, dataDir string) (*Manager, error) {
	if reg == nil || b == nil {
		return nil, fmt.Errorf("firn/queue: registry and broker are required")
	}
	var attempts *AttemptStore
	if dataDir == "" {
		var err error
		attempts, err = NewAttemptStore(dataDir, nil)
		if err != nil {
			return nil, err
		}
	} else {
		fl, ok := b.InternalLog("__queue_attempts")
		if !ok {
			return nil, fmt.Errorf("firn/queue: internal log not opened by broker")
		}
		var err error
		attempts, err = NewAttemptStore(dataDir, fl)
		if err != nil {
			return nil, err
		}
	}
	attempts.SetGate(b.InternalGateBox())
	m := &Manager{reg: reg, broker: b, attempts: attempts, now: time.Now, queues: map[string]*queueState{}}
	for _, t := range reg.List() {
		if t.Mode != meta.ModeQueue {
			continue
		}
		if err := m.initQueueLocked(t); err != nil {
			_ = attempts.Close()
			return nil, err
		}
	}
	return m, nil
}

func (m *Manager) queueStateLocked(topic string) (*queueState, error) {
	if q, ok := m.queues[topic]; ok {
		return q, nil
	}
	t, err := m.reg.Get(topic)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrQueueNotFound, topic)
	}
	if t.Mode != meta.ModeQueue {
		return nil, fmt.Errorf("%w: %s (mode=%s)", ErrNotAQueue, topic, t.Mode)
	}
	if err := m.initQueueLocked(t); err != nil {
		return nil, err
	}
	return m.queues[topic], nil
}

// initQueueLocked seeds a queue's state from the committed offsets of its
// internal group `queue-<topic>` (one entry per partition).
func (m *Manager) initQueueLocked(t meta.Topic) error {
	q := &queueState{
		partitions: t.Partitions,
		nextFetch:  make([]int64, t.Partitions),
		committed:  make([]int64, t.Partitions),
		acked:      map[int32]map[int64]bool{},
		pending:    map[string]*pending{},
	}
	for p := int32(0); p < int32(t.Partitions); p++ {
		v, found, err := m.broker.Groups().FetchOffset("queue-"+t.Name, t.Name, p)
		if err != nil {
			return fmt.Errorf("firn/queue: load committed offset for %q partition %d: %w", t.Name, p, err)
		}
		if found {
			q.committed[p] = v.Offset
			q.nextFetch[p] = v.Offset
		}
	}
	m.queues[t.Name] = q
	return nil
}

// Pull claims up to maxMessages unacked messages. Messages whose visibility
// lease expired are redelivered first. Attempts are persisted before the
// delivery is returned.
func (m *Manager) Pull(ctx context.Context, queue string, maxMessages int, visibilityTimeout time.Duration) ([]Delivery, error) {
	if maxMessages <= 0 {
		maxMessages = defaultMaxMessages
	}
	if maxMessages > hardMaxMessages {
		maxMessages = hardMaxMessages
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = defaultVisibility
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	q, err := m.queueStateLocked(queue)
	if err != nil {
		return nil, err
	}
	if m.fedFence != nil && m.fedFence(queue) {
		return nil, fmt.Errorf("%w: %s", ErrFederatedPassive, queue)
	}
	now := m.now()
	m.expireLocked(now)

	type claimPlan struct {
		partition int32
		offset    int64
		key       []byte
		value     []byte
		attempts  int32
	}
	plans := make([]claimPlan, 0, maxMessages)

	// 1. Redeliver lease-expired messages first.
	for len(plans) < maxMessages && len(q.requeue) > 0 {
		item := q.requeue[0]
		next := item.attempts + 1
		if next > maxDeliveries {
			m.divertToDLQ(queue, item.partition, item.offset, item.key, item.value, uint32(next))
			m.accumulateAckLocked(queue, q, item.partition, item.offset)
			m.commitAckLocked(queue, q, item.partition)
			q.requeue = q.requeue[1:]
			continue
		}
		plans = append(plans, claimPlan{partition: item.partition, offset: item.offset, key: item.key, value: item.value, attempts: next})
		q.requeue = q.requeue[1:]
	}

	// 2. Fresh fetches, round-robin across partitions.
	emptyRounds := 0
	for len(plans) < maxMessages && emptyRounds < q.partitions {
		p := int32(q.rr % q.partitions)
		q.rr++
		pos := q.nextFetch[p]
		recs, err := m.broker.Fetch(ctx, queue, p, pos, 1)
		if err != nil || len(recs) == 0 {
			emptyRounds++
			continue
		}
		rec := recs[0]
		next := int32(m.attempts.Attempts(queue, p, rec.Offset)) + 1
		if next > maxDeliveries {
			m.divertToDLQ(queue, p, rec.Offset, rec.Key, rec.Value, uint32(next))
			m.accumulateAckLocked(queue, q, p, rec.Offset)
			m.commitAckLocked(queue, q, p)
			q.nextFetch[p] = rec.Offset + 1
			emptyRounds = 0
			continue
		}
		plans = append(plans, claimPlan{partition: p, offset: rec.Offset, key: rec.Key, value: rec.Value, attempts: next})
		q.nextFetch[p] = rec.Offset + 1
		emptyRounds = 0
	}

	if len(plans) == 0 {
		return nil, nil
	}
	// 3. Persist attempts for ALL claimed deliveries in ONE fsync before
	// any of them becomes visible — strictly at-least-once.
	batch := make([]log.BatchRecord, 0, len(plans))
	for _, pl := range plans {
		batch = append(batch, log.BatchRecord{
			Key:   attemptKey(queue, pl.partition, pl.offset),
			Value: binary.BigEndian.AppendUint32(nil, uint32(pl.attempts)),
		})
	}
	if err := m.attempts.PutBatch(batch); err != nil {
		return nil, err
	}
	// 4. Register deliveries.
	out := make([]Delivery, 0, len(plans))
	for _, pl := range plans {
		id := newDeliveryID()
		deadline := now.Add(visibilityTimeout)
		q.pending[id] = &pending{partition: pl.partition, offset: pl.offset, attempts: pl.attempts, deadline: deadline, key: pl.key, value: pl.value}
		out = append(out, Delivery{
			DeliveryID: id,
			Queue:      queue,
			Partition:  pl.partition,
			Offset:     pl.offset,
			Key:        pl.key,
			Value:      pl.value,
			Attempts:   pl.attempts,
			DeadlineMs: deadline.UnixMilli(),
		})
	}
	return out, nil
}

// Ack consumes a delivered message: it is removed from the pending set and
// the internal queue group's committed offset advances over the contiguous
// acked prefix.
// AckBatchResult reports the outcome of an AckBatch call.
type AckBatchResult struct {
	Acked   int
	Unknown int
}

// Ack settles a delivery (see Ack). Kept identical in behavior: one commit
// per partition touched when the contiguous committed offset advanced.
func (m *Manager) Ack(ctx context.Context, queue, deliveryID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, err := m.queueStateLocked(queue)
	if err != nil {
		return err
	}
	if m.fedFence != nil && m.fedFence(queue) {
		return fmt.Errorf("%w: %s", ErrFederatedPassive, queue)
	}
	m.expireLocked(m.now())
	p, ok := q.pending[deliveryID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownDelivery, deliveryID)
	}
	delete(q.pending, deliveryID)
	m.accumulateAckLocked(queue, q, p.partition, p.offset)
	m.commitAckLocked(queue, q, p.partition)
	return nil
}

// AckBatch settles many deliveries in one pass: one queue-lock hold, one
// contiguous-advance commit per touched partition (vs one per Ack). Unknown
// or already-settled ids are tolerated and counted, never fail the call.
func (m *Manager) AckBatch(ctx context.Context, queue string, deliveryIDs []string) (AckBatchResult, error) {
	if len(deliveryIDs) == 0 {
		return AckBatchResult{}, fmt.Errorf("%w: delivery_ids must not be empty", ErrInvalidRequest)
	}
	if len(deliveryIDs) > 500 {
		return AckBatchResult{}, fmt.Errorf("%w: delivery_ids must contain at most 500 items", ErrInvalidRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	q, err := m.queueStateLocked(queue)
	if err != nil {
		return AckBatchResult{}, err
	}
	if m.fedFence != nil && m.fedFence(queue) {
		return AckBatchResult{}, fmt.Errorf("%w: %s", ErrFederatedPassive, queue)
	}
	m.expireLocked(m.now())
	partitions := map[int32]bool{}
	res := AckBatchResult{}
	for _, id := range deliveryIDs {
		p, ok := q.pending[id]
		if !ok {
			res.Unknown++
			continue
		}
		delete(q.pending, id)
		m.accumulateAckLocked(queue, q, p.partition, p.offset)
		partitions[p.partition] = true
		res.Acked++
	}
	for partition := range partitions {
		m.commitAckLocked(queue, q, partition)
	}
	return res, nil
}

// accumulateAckLocked records offset as consumed and advances the committed
// offset over the contiguous acked prefix. The committed value advances
// monotonically (invariant 4). It does NOT persist; call commitAckLocked
// after the batch is complete.
func (m *Manager) accumulateAckLocked(queue string, q *queueState, partition int32, offset int64) {
	set := q.acked[partition]
	if set == nil {
		set = map[int64]bool{}
		q.acked[partition] = set
	}
	set[offset] = true
	for set[q.committed[partition]] {
		delete(set, q.committed[partition])
		q.committed[partition]++
	}
}

// commitAckLocked persists the committed offset if the contiguous prefix
// was advanced. Calls are idempotent for equal values (monotonicity is
// enforced by the coordinator), but skipping no-op commits keeps the
// __consumer_offsets log small.
func (m *Manager) commitAckLocked(queue string, q *queueState, partition int32) {
	committed := q.committed[partition]
	if got, found, err := m.broker.Groups().FetchOffset("queue-"+queue, queue, partition); err == nil && found {
		if committed <= got.Offset {
			return // nothing new to persist
		}
	}
	// Generation 0 = unchecked; allowReset=false enforces monotonic advancement.
	_ = m.broker.Groups().CommitOffset("queue-"+queue, queue, partition, committed, 0, false)
}

// divertToDLQ writes the message to <queue>.dlq (auto-created as a stream
// topic). The caller is responsible for marking the source offset consumed.
func (m *Manager) divertToDLQ(queue string, partition int32, offset int64, key, value []byte, attempts uint32) {
	dlq := queue + ".dlq"
	if _, err := m.reg.Get(dlq); err != nil {
		// Auto-create the DLQ; ignore races with concurrent creation.
		_ = m.reg.Create(dlq, 1)
	}
	if len(key) == 0 {
		key = []byte(fmt.Sprintf("offset-%d", offset))
	}
	dlqValue := append([]byte(nil), value...)
	// Best effort: if the DLQ produce fails we still mark the source offset
	// consumed to avoid an infinite redelivery loop on a poisoned DLQ. The
	// failure is observable via the log and admin /topics.
	if _, _, err := m.broker.Produce(context.Background(), dlq, key, dlqValue); err != nil {
		fmt.Printf("firn/queue: divert to DLQ %q failed for %q partition %d offset %d: %v\n", dlq, queue, partition, offset, err)
	}
}

// expireLocked returns lease-expired pendings to the requeue list.
func (m *Manager) expireLocked(now time.Time) int {
	n := 0
	for _, q := range m.queues {
		for id, p := range q.pending {
			if now.After(p.deadline) {
				delete(q.pending, id)
				q.requeue = append(q.requeue, redelivery{partition: p.partition, offset: p.offset, attempts: p.attempts, key: p.key, value: p.value})
				n++
			}
		}
	}
	return n
}

// Expire redeems lease-expired deliveries immediately. Call it periodically.
func (m *Manager) Expire() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.expireLocked(m.now())
}

// QueueStats is a diagnostic snapshot.
type QueueStats struct {
	Topic        string
	Pending      int
	Redeliveries int
}

// Stats reports in-flight counts per queue.
func (m *Manager) Stats() []QueueStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []QueueStats{}
	for name, q := range m.queues {
		out = append(out, QueueStats{Topic: name, Pending: len(q.pending), Redeliveries: len(q.requeue)})
	}
	return out
}

// Close compacts and closes the attempts store.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attempts.Close()
}

func newDeliveryID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
