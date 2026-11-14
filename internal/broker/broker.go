// Package broker orchestrates produce/fetch over the append-only log.
// Storage invariants live in internal/log (untouched here); topic state
// lives in internal/meta; this package wires them together and assigns
// partitions.
//
// Partition assignment (docs/plans/001-mvp-single-node.md, slice 1):
// keyed records go to FNV-1a(key) % partitions; keyless records round-robin.
package broker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/url"
	"path/filepath"

	"github.com/rahulkj/firn/internal/fence"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rahulkj/firn/internal/groups"
	"github.com/rahulkj/firn/internal/log"
	"github.com/rahulkj/firn/internal/meta"
)

// ErrPartitionOutOfRange is returned when a partition index is negative or
// >= the topic's partition count.
var ErrPartitionOutOfRange = errors.New("firn/broker: partition out of range")

// ErrInvalidRequest is returned for negative offsets or max_records < 1.
var ErrInvalidRequest = errors.New("firn/broker: invalid fetch request")

// ErrNotLeaseHolder is returned when a cluster-mode produce is routed to a
// partition this node does not hold the write lease for (ADR-007: only the
// lease holder may append).
var ErrNotLeaseHolder = errors.New("firn/broker: not lease holder for partition")

// ErrFederatedPassive is returned when a produce targets a partition that
// is federated in passive mode on this node — it is a sink, not a writer.
var ErrFederatedPassive = errors.New("firn/broker: topic/partition is federated passively; writes are rejected")

// LeaseAdvisor reports which node holds a partition's write lease.
// firnd wires it in cluster mode; single-node mode leaves it nil.
type LeaseAdvisor interface {
	WriteAllowed(topic string, partition int32) bool
	Holder(topic string, partition int32) string
}

// partitionLog is the storage contract the broker needs. MemLog (slice 1)
// and FileLog (slice 2) both satisfy it.
type partitionLog interface {
	Append(key, value []byte) int64
	Fetch(offset int64, max int) ([]log.Record, error)
	HighWatermark() int64
}

// Broker is a produce/fetch orchestrator. It is safe for concurrent use;
// the underlying logs are not concurrency-safe, so all log access is
// serialized through b.mu.
//
// A Broker created with New keeps everything in memory (slice 1 behavior).
// A Broker created with NewWithDir persists partitions under dir as
// segmented file logs; on restart the data survives, but topics must be
// recreated via the admin API before produce/fetch (topic metadata is
// in-memory until slice 3).
type Broker struct {
	reg             *meta.Registry
	dir             string // empty => in-memory MemLogs
	mu              sync.Mutex
	logs            map[string][]partitionLog
	rr              atomic.Uint64
	groups          *groups.Coordinator
	advisor         LeaseAdvisor
	maxSegmentBytes int64
	sealAge         time.Duration
	flushWindow     time.Duration
	maxBatchBytes   int64
	// ADR-014 slice 4: opt-in flush window for __consumer_offsets and
	// __queue_attempts only (never __audit/__api_keys/__federation*).
	internalFlushWindow     time.Duration
	internalMaxBatchBytes   int64
	groupOpts       []groups.Option
	closed          bool
	cluster         ClusterView
	internalLogs    map[string]*log.FileLog

	// gateBox is the internal-topic write fence (see internal/fence).
	// Late-bound by the daemon after the quorum starts (cluster mode).
	gateBox *fence.GateBox

	federationHWM FederationHWM

	// federationFence is the passive-federation write fence (ADR-010):
	// topics selected by an inbound passive peer are read-only on this
	// broker until an operator cuts over. Late-bound by the daemon.
	federationFence FederationFence
}

// InternalLogNames returns the internal topic names whose FileLogs are
// shared between the broker, its stores, and the replication agent.
var InternalLogNames = []string{"__consumer_offsets", "__queue_attempts", "__audit", "__api_keys", "__federation_peers", "__federation_cursors"}

// InternalGateBox returns the internal-topic write fence box. The daemon
// fills it in (cluster mode) once the quorum leases exist; until then
// writes are allowed (single-node semantics). Stores created by the broker
// already reference the box.
func (b *Broker) InternalGateBox() *fence.GateBox { return b.gateBox }

// InternalLog returns a shared internal FileLog. The broker owns these
// handles; callers must not close them.
func (b *Broker) InternalLog(name string) (*log.FileLog, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fl, ok := b.internalLogs[name]
	return fl, ok
}

// InternalLogs returns all internal-topic-file handles (for diagnostics).
func (b *Broker) InternalLogs() map[string]*log.FileLog {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]*log.FileLog{}
	for k, v := range b.internalLogs {
		out[k] = v
	}
	return out
}

// Option configures a Broker.
type Option func(*Broker)

// WithMaxSegmentBytes sets the segment-roll threshold for partition logs
// (default 64 MiB). Mainly for tests and small-cluster setups.
func WithMaxSegmentBytes(n int64) Option {
	return func(b *Broker) { b.maxSegmentBytes = n }
}

// WithBlockSealing sets the maximum age of an active segment before the
// broker seals it (rolls to a new segment) on its next append. Zero = seal
// on size only.
func WithBlockSealing(d time.Duration) Option {
	return func(b *Broker) { b.sealAge = d }
}

// WithFlushWindow enables group commit (ADR-014): produce appends are
// staged and flushed to the file in windows; a produce acks only after
// its window flushes. Zero (default) keeps fsync-per-append semantics.
// maxBytes bounds the staged bytes per partition window (1 MiB default).
func WithFlushWindow(d time.Duration, maxBytes int64) Option {
	return func(b *Broker) { b.flushWindow, b.maxBatchBytes = d, maxBytes }
}

// WithInternalFlushWindow additionally applies the flush window to the
// __consumer_offsets and __queue_attempts internal logs (opt-in; never
// affects __audit/__api_keys/__federation*, which always fsync-per-append).
//
// Trade-off (documented loudly at startup; see ADR-003 amendments):
// a hard crash may lose up to `d` of committed offsets (consumers rewind —
// at-least-once is preserved) and of delivery-attempt counts (DLQ-after-3
// may trigger late for messages whose attempts it lost). Zero (default)
// preserves stock semantics exactly.
func WithInternalFlushWindow(d time.Duration, maxBytes int64) Option {
	return func(b *Broker) { b.internalFlushWindow, b.internalMaxBatchBytes = d, maxBytes }
}

// FederationHWM computes the advisory federation watermark for
// (topic, partition): the contiguous offset applied from every configured
// passive peer — the minimum across them. 0 = no passive federation (or no
// cursor yet). Advisory only (ADR-010); never consulted for write decisions.
type FederationHWM func(topic string, partition int32) int64

// FederationFence reports whether (topic, partition) is federated in passive
// mode on this cluster. True means the local broker is a sink for the
// partition and must refuse local appends. Default: nil (no fence).
type FederationFence func(topic string, partition int32) bool

// SetFederationFence binds the passive-federation write fence.
func (b *Broker) SetFederationFence(f FederationFence) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.federationFence = f
}

// SetFederationHWM binds the advisory federation watermark hook.
func (b *Broker) SetFederationHWM(f FederationHWM) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.federationHWM = f
}

// FederationHWM returns the advisory watermark for (topic, partition),
// or 0 when no hook is bound or nothing is federated.
func (b *Broker) FederationHWM(topic string, partition int32) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.federationHWM == nil {
		return 0
	}
	return b.federationHWM(topic, partition)
}

// SetAdvisor wires the cluster write-lease check. Only firnd in cluster
// mode calls this; nil advisor = single-node, all writes allowed.
func (b *Broker) SetAdvisor(a LeaseAdvisor) { b.advisor = a }

// ListTopics returns all topics known to the registry.
func (b *Broker) ListTopics() []meta.Topic { return b.reg.List() }

// TopicMeta returns metadata for a topic.
func (b *Broker) TopicMeta(name string) (meta.Topic, error) { return b.reg.Get(name) }

// Topics returns the names of all registered topics, including queue-mode topics.
func (b *Broker) Topics() []string {
	out := []string{}
	for _, t := range b.reg.List() {
		out = append(out, t.Name)
	}
	return out
}

// Advisor returns the configured lease advisor (nil in single-node mode).
func (b *Broker) Advisor() LeaseAdvisor { return b.advisor }

// WithGroupsOption adapts a groups.Option (coordinator configuration) into a
// Broker Option.
func WithGroupsOption(o groups.Option) Option {
	return func(b *Broker) { b.groupOpts = append(b.groupOpts, o) }
}

// New returns an in-memory Broker backed by reg.
func New(reg *meta.Registry, opts ...Option) *Broker {
	b := &Broker{reg: reg, logs: make(map[string][]partitionLog)}
	for _, o := range opts {
		o(b)
	}
	store, err := groups.NewOffsetStore("", nil)
	if err != nil {
		panic(fmt.Sprintf("firn/broker: in-memory offsets store: %v", err))
	}
	coord, err := groups.NewCoordinator(reg, store, b.groupOpts...)
	if err != nil {
		panic(fmt.Sprintf("firn/broker: coordinator: %v", err))
	}
	b.groups = coord
	b.internalLogs = map[string]*log.FileLog{}
	b.gateBox = fence.NewGateBox()
	if store, ok := coord.Store(); ok {
		store.SetGate(b.gateBox)
	}
	return b
}

// NewWithDir returns a Broker whose partition logs are FileLogs rooted at
// dir, reopening any existing data (partition logs and committed group
// offsets) on start.
func NewWithDir(reg *meta.Registry, dir string, opts ...Option) (*Broker, error) {
	b := &Broker{reg: reg, dir: dir, logs: make(map[string][]partitionLog)}
	for _, o := range opts {
		o(b)
	}
	b.gateBox = fence.NewGateBox()
	internalLogs := map[string]*log.FileLog{}
	for _, name := range InternalLogNames {
		opts := []log.Option{}
		if b.internalFlushWindow > 0 && (name == "__consumer_offsets" || name == "__queue_attempts") {
			opts = append(opts, log.WithFlushWindow(b.internalFlushWindow), log.WithMaxBatchBytes(b.internalMaxBatchBytes))
		}
		fl, err := log.Open(filepath.Join(dir, name+"-0"), opts...)
		if err != nil {
			return nil, fmt.Errorf("open internal log %q: %w", name, err)
		}
		internalLogs[name] = fl
	}
	b.internalLogs = internalLogs
	store, err := groups.NewOffsetStore(dir, internalLogs["__consumer_offsets"])
	if err == nil {
		store.SetGate(b.gateBox)
	}
	if err != nil {
		return nil, fmt.Errorf("open offsets store: %w", err)
	}
	coord, err := groups.NewCoordinator(reg, store, b.groupOpts...)
	if err != nil {
		return nil, fmt.Errorf("coordinator: %w", err)
	}
	b.groups = coord
	return b, nil
}

// ClusterView advertises this node's identity and the cluster's
// id->address map plus the current quorum leader. firnd wires it in
// cluster mode; nil in single-node mode.
type ClusterView interface {
	ID() string
	AdvertisedAddr() string
	NodeAddr(id string) (string, bool)
	AllNodes() []string
	Leader() string
}

// SetClusterView wires cluster metadata into /firnd for GetMetadata.
func (b *Broker) SetClusterView(v ClusterView) { b.cluster = v }

// ClusterView returns the configured cluster view (nil in single-node mode).
func (b *Broker) ClusterView() ClusterView { return b.cluster }

// Groups returns the group coordinator (membership, assignment, offsets).
func (b *Broker) Groups() *groups.Coordinator { return b.groups }

// Retain applies segment retention to every partition log. ttl is the max
// segment age (0 disables); maxBytes caps each partition log's on-disk size
// (0 disables). The active segment is never deleted. In-memory (MemLog)
// partitions are skipped. It returns the number of segments deleted.
func (b *Broker) Retain(ttl time.Duration, maxBytes int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	deleted := 0
	for topic, parts := range b.logs {
		for p, pl := range parts {
			fl, ok := pl.(*log.FileLog)
			if !ok {
				continue
			}
			if ttl > 0 {
				d, err := fl.RetainOlderThan(time.Now().Add(-ttl))
				if err != nil {
					return deleted, fmt.Errorf("retain ttl %s partition %d: %w", topic, p, err)
				}
				deleted += d
			}
			if maxBytes > 0 {
				d, err := fl.RetainOverBytes(maxBytes)
				if err != nil {
					return deleted, fmt.Errorf("retain bytes %s partition %d: %w", topic, p, err)
				}
				deleted += d
			}
		}
	}
	return deleted, nil
}

// Close flushes and closes all partition logs and the offsets store.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for topic, parts := range b.logs {
		for p, pl := range parts {
			if fl, ok := pl.(*log.FileLog); ok {
				if err := fl.Close(); err != nil && firstErr == nil {
					firstErr = fmt.Errorf("close %s partition %d: %w", topic, p, err)
				}
			}
		}
	}
	if b.groups != nil {
		if err := b.groups.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close offsets store: %w", err)
		}
	}
	b.logs = map[string][]partitionLog{}
	b.closed = true
	return firstErr
}

// Produce appends value to the partition for key (or the next partition in
// round-robin order when key is empty/nil) and returns the assigned
// partition and offset.
func (b *Broker) Produce(ctx context.Context, topic string, key, value []byte) (int32, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if b.closed {
		return 0, 0, errors.New("firn/broker: broker is closed")
	}
	t, err := b.reg.Get(topic)
	if err != nil {
		return 0, 0, fmt.Errorf("produce: %w", err)
	}

	partition := b.assign(t.Partitions, key)

	if b.federationFence != nil && b.federationFence(topic, partition) {
		return 0, 0, fmt.Errorf("%w: %s partition %d", ErrFederatedPassive, topic, partition)
	}

	if b.advisor != nil && !b.advisor.WriteAllowed(topic, partition) {
		return 0, 0, fmt.Errorf("%w: %s partition %d", ErrNotLeaseHolder, topic, partition)
	}

	// The map lookup is brief; the FileLog serializes its own frames. Holding
	// b.mu across a blocking Append would serialize ALL producers on one
	// mutex and defeat group commit (ADR-014) — every produce would wait
	// out the whole window behind every other produce.
	b.mu.Lock()
	logs, err := b.partitionLogs(t)
	b.mu.Unlock()
	if err != nil {
		return 0, 0, fmt.Errorf("produce: %w", err)
	}
	offset := logs[partition].Append(key, value)
	return partition, offset, nil
}

// Fetch returns up to maxRecords records starting at offset. A fetch at the
// high watermark returns an empty slice; beyond it, an error. Records at
// offset >= the high watermark are never returned (invariant 2, ADR-003).
func (b *Broker) Fetch(ctx context.Context, topic string, partition int32, offset int64, maxRecords int) ([]log.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || maxRecords < 1 {
		return nil, fmt.Errorf("%w: offset=%d max_records=%d", ErrInvalidRequest, offset, maxRecords)
	}
	t, err := b.reg.Get(topic)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	if partition < 0 || int(partition) >= t.Partitions {
		return nil, fmt.Errorf("%w: topic %q partition %d (partitions=%d)", ErrPartitionOutOfRange, topic, partition, t.Partitions)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	logs, err := b.partitionLogs(t)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	plog := logs[partition]

	hw := plog.HighWatermark()
	if offset == hw {
		return nil, nil
	}
	if offset > hw {
		return nil, fmt.Errorf("firn/broker: offset %d beyond high watermark %d", offset, hw)
	}
	return plog.Fetch(offset, maxRecords)
}

// assign picks a partition for key. Keyed records: FNV-1a(key) % partitions
// (plan slice 1). Keyless records: round-robin.
func (b *Broker) assign(partitions int, key []byte) int32 {
	if len(key) == 0 {
		return int32(b.rr.Add(1)-1) % int32(partitions)
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int32(h.Sum32() % uint32(partitions))
}

// partitionLogs returns (creating/opening if needed) the per-partition logs
// for t. Callers must hold b.mu. With a data dir configured, the FileLog
// for <topic>-<partition> is opened on first touch, performing crash
// recovery; without one, an in-memory MemLog is used.
func (b *Broker) partitionLogs(t meta.Topic) ([]partitionLog, error) {
	logs, ok := b.logs[t.Name]
	if ok {
		return logs, nil
	}
	logs = make([]partitionLog, t.Partitions)
	if b.dir == "" {
		for i := range logs {
			logs[i] = &log.MemLog{}
		}
		b.logs[t.Name] = logs
		return logs, nil
	}
	for i := range logs {
		fl, err := log.Open(partitionLogDir(b.dir, t.Name, i), logOptsFrom(b)...)
		if err != nil {
			// Close anything we opened so far; fail loudly.
			for _, pl := range logs {
				if pl != nil {
					if fl2, ok := pl.(*log.FileLog); ok {
						_ = fl2.Close()
					}
				}
			}
			return nil, fmt.Errorf("open partition log for topic %q partition %d: %w", t.Name, i, err)
		}
		logs[i] = fl
	}
	b.logs[t.Name] = logs
	return logs, nil
}

// logOptsFrom builds FileLog options from broker configuration.
func logOptsFrom(b *Broker) []log.Option {
	var opts []log.Option
	if b.maxSegmentBytes > 0 {
		opts = append(opts, log.WithMaxSegmentBytes(b.maxSegmentBytes))
	}
	if b.sealAge > 0 {
		opts = append(opts, log.WithBlockSealing(b.sealAge))
	}
	if b.flushWindow > 0 {
		opts = append(opts, log.WithFlushWindow(b.flushWindow), log.WithMaxBatchBytes(b.maxBatchBytes))
	}
	return opts
}

// partitionLogDir maps a topic/partition to its on-disk directory. The
// topic name is path-escaped so arbitrary names cannot escape the data dir.
func partitionLogDir(dir, topic string, partition int) string {
	return fmt.Sprintf("%s/%s-%d", dir, url.PathEscape(topic), partition)
}

// SealAll rolls every partition log's active segment that has outlived the
// configured block-seal timer. Returns the number of blocks sealed.
func (b *Broker) SealAll() int {
	if b.sealAge <= 0 {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	sealed := 0
	for _, parts := range b.logs {
		for _, pl := range parts {
			if fl, ok := pl.(*log.FileLog); ok {
				if ok, err := fl.SealIfOlderThan(b.sealAge); err == nil && ok {
					sealed++
				}
			}
		}
	}
	return sealed
}

// PartitionBlockDescriptor is a sealed block descriptor tagged with its partition — used by the replication agent.
type PartitionBlockDescriptor struct {
	Partition  int32
	Descriptor log.BlockDescriptor
}

// PartitionHighWatermark returns the partition's high watermark. Safe for
// the cluster agent to call concurrently with broker appends.
func (b *Broker) PartitionHighWatermark(ctx context.Context, topic string, partition int32) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	t, err := b.reg.Get(topic)
	if err != nil {
		return 0, fmt.Errorf("hwm: %w", err)
	}
	if partition < 0 || int(partition) >= t.Partitions {
		return 0, fmt.Errorf("%w: topic %q partition %d", ErrPartitionOutOfRange, topic, partition)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	logs, err := b.partitionLogs(t)
	if err != nil {
		return 0, fmt.Errorf("hwm: %w", err)
	}
	return logs[partition].HighWatermark(), nil
}

// Blocks returns sealed block descriptors across all partitions of topic,
// or an error in in-memory mode (replication is for durable clusters).
func (b *Broker) Blocks(topic string) ([]PartitionBlockDescriptor, error) {
	t, err := b.reg.Get(topic)
	if err != nil {
		return nil, fmt.Errorf("blocks: %w", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	logs, err := b.partitionLogs(t)
	if err != nil {
		return nil, fmt.Errorf("blocks: %w", err)
	}
	out := make([]PartitionBlockDescriptor, 0, len(logs))
	for i, pl := range logs {
		fl, ok := pl.(*log.FileLog)
		if !ok {
			return nil, fmt.Errorf("firn/broker: partition %d is not file-backed", i)
		}
		for _, d := range fl.Blocks() {
			out = append(out, PartitionBlockDescriptor{Partition: int32(i), Descriptor: d})
		}
	}
	return out, nil
}

// AppendReplicatedFrames appends replication frames in order, preserving
// timestamps, and is atomic with respect to the broker's normal append
// path. Frames with offsets below the current high watermark are skipped
// (already replicated); a frame beyond it is a hard error (fork/gap).
// Returns the resulting high watermark.
func (b *Broker) AppendReplicatedFrames(topic string, partition int32, frames []log.Frame) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, err := b.reg.Get(topic)
	if err != nil {
		return 0, fmt.Errorf("append frames: %w", err)
	}
	if partition < 0 || int(partition) >= t.Partitions {
		return 0, fmt.Errorf("%w: topic %q partition %d", ErrPartitionOutOfRange, topic, partition)
	}
	logs, err := b.partitionLogs(t)
	if err != nil {
		return 0, fmt.Errorf("append frames: %w", err)
	}
	pl := logs[partition]
	fl, ok := pl.(*log.FileLog)
	if !ok {
		return 0, fmt.Errorf("firn/broker: partition %q/%d is not file-backed", topic, partition)
	}
	for _, f := range frames {
		hwm := fl.HighWatermark()
		if f.Offset < hwm {
			continue // already replicated
		}
		if f.Offset != hwm {
			return 0, fmt.Errorf("replica fork: frame offset %d, local high watermark %d", f.Offset, hwm)
		}
		fl.AppendReplicated(f.Ts, f.Key, f.Value)
	}
	return fl.HighWatermark(), nil
}

// AppendReoriginedFrames applies foreign frames with FRESH local offsets,
// preserving key/value/ts/order (active-active federation, ADR-010 slice 4).
// Unlike AppendReplicatedFrames there is no fork check, no skip-below-HWM
// rule: offsets are assigned locally and monotonically. Returns the first
// assigned local offset and the new high watermark. MemLog is not supported.
func (b *Broker) AppendReoriginedFrames(topic string, partition int32, frames []log.Frame) (int64, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, err := b.reg.Get(topic)
	if err != nil {
		return 0, 0, fmt.Errorf("reorigin append: %w", err)
	}
	if partition < 0 || int(partition) >= t.Partitions {
		return 0, 0, fmt.Errorf("%w: topic %q partition %d", ErrPartitionOutOfRange, topic, partition)
	}
	logs, err := b.partitionLogs(t)
	if err != nil {
		return 0, 0, fmt.Errorf("reorigin append: %w", err)
	}
	pl := logs[partition]
	fl, ok := pl.(*log.FileLog)
	if !ok {
		return 0, 0, fmt.Errorf("firn/broker: partition %q/%d is not file-backed", topic, partition)
	}
	first := fl.HighWatermark()
	for _, f := range frames {
		fl.AppendReplicated(f.Ts, f.Key, f.Value)
	}
	return first, fl.HighWatermark(), nil
}

// ReadRange returns the raw bytes of frames [firstOffset, lastOffset] for
// the given partition — the replication read path. MemLog is not supported.
func (b *Broker) ReadRange(ctx context.Context, topic string, partition int32, firstOffset, lastOffset int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t, err := b.reg.Get(topic)
	if err != nil {
		return nil, fmt.Errorf("read range: %w", err)
	}
	if partition < 0 || int(partition) >= t.Partitions {
		return nil, fmt.Errorf("%w: topic %q partition %d", ErrPartitionOutOfRange, topic, partition)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	logs, err := b.partitionLogs(t)
	if err != nil {
		return nil, fmt.Errorf("read range: %w", err)
	}
	fl, ok := logs[partition].(*log.FileLog)
	if !ok {
		return nil, fmt.Errorf("firn/broker: partition %q/%d is not file-backed", topic, partition)
	}
	return fl.ReadRange(firstOffset, lastOffset)
}

// RawAppender is what a replication follower needs: AppendReplicated to
// preserve timestamps plus a high watermark for in-order checks.
type RawAppender interface {
	AppendReplicated(ts int64, key, value []byte) int64
	HighWatermark() int64
}

// RawAppenderFor returns the replication appender for a partition, opening
// the FileLog on first use. Returns an error in in-memory mode (replication
// is only for durable clusters).
func (b *Broker) RawAppenderFor(topic string, partition int32) (RawAppender, error) {
	t, err := b.reg.Get(topic)
	if err != nil {
		return nil, fmt.Errorf("raw appender: %w", err)
	}
	if partition < 0 || int(partition) >= t.Partitions {
		return nil, fmt.Errorf("%w: topic %q partition %d", ErrPartitionOutOfRange, topic, partition)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	logs, err := b.partitionLogs(t)
	if err != nil {
		return nil, fmt.Errorf("raw appender: %w", err)
	}
	fl, ok := logs[partition].(*log.FileLog)
	if !ok {
		return nil, fmt.Errorf("firn/broker: partition %q/%d is not file-backed", topic, partition)
	}
	return fl, nil
}
