// Package replication implements ADR-007's data plane: sealed-block
// replication to replica nodes and write-lease management. The agent runs
// in cluster mode (firnd with -node-id/-peers) and keeps every node's
// partition replicas byte-identical to the lease holder's (block IDs are
// content-addressed, so equality is checkable).
//
// Push on seal, pull on recovery:
//
//  1. Sealer: every interval, find sealed blocks without a committed
//     descriptor, push them to peers via ReplicaPut, then propose
//     EntryBlockCommit to the metadata quorum.
//  2. Repairer: for every committed BlockCommit in the applied state,
//     ensure the block exists locally; if not, ReplicaFetch it from any
//     peer and apply via AppendReplicated (identical bytes).
//  3. Leaser: assigned partitions (partition % clusterSize) are claimed
//     via EntryLeaseGrant with epoch increments; leases expire
//     (GrantedAtMs + ttl) so a crashed holder loses the slot.
//
// Fencing: broker.Produce consults the lease table before appending
// (broker.SetAdvisor). The stale holder is rejected cluster-side.
package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/log"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/quorum"
)

const (
	defaultInterval = 250 * time.Millisecond
	defaultLeaseTTL = 3 * time.Second
)

// Config wires an agent to a node.
type Config struct {
	NodeID     string
	Peers      []string          // peer gRPC addresses (excluding self)
	All        []string          // all node ids incl self, for lease assignment
	Advertised map[string]string // id -> advertised gRPC address, all nodes incl self
	Registry   *meta.Registry
	Broker     *broker.Broker
	Quorum     *quorum.Node
	Dial       func(ctx context.Context, addr string) (*grpc.ClientConn, error)
	LeaseTTL   time.Duration
	Interval   time.Duration
	DataDir    string
	slogLogger *slog.Logger

	// claimEpoch records the epoch of the lease we last performed pull
	// recovery for, keyed by topic\x00partition. Pull recovery runs once
	// per self-granted epoch — it is idempotent.
	claimEpoch map[string]uint64

	// internalLogs caches broker-owned internal FileLog handles.
	internalLogs map[string]*log.FileLog

	// stop/done implement the same stop/wait handshake the puller uses:
	// Stop halts the loop and waits for Run to return, so teardown never
	// races in-flight ticks against broker Close or t.TempDir() removal.
	stop chan struct{}
	done chan struct{}
}

// internalLog returns the broker-owned FileLog handle for an internal topic.
// The broker owns the handle; the replicator never closes it.
func (c *Config) internalLog(name string) (*log.FileLog, error) {
	if fl, ok := c.internalLogs[name]; ok {
		return fl, nil
	}
	fl, ok := c.Broker.InternalLog(name)
	if !ok {
		return nil, fmt.Errorf("internal log %q not opened by broker", name)
	}
	c.internalLogs[name] = fl
	return fl, nil
}

// maintainInternalLease grants/renews one lease per internal topic across
// the cluster. Internal topics follow the same lease/TTL discipline as user
// topics; the quorum is the arbiter for which node may push/pull.
func (c *Config) maintainInternalLease(ctx context.Context, name string) {
	cur, ok := c.Quorum.Lease(name, 0)
	now := time.Now().UnixMilli()
	ttlMs := int64(c.LeaseTTL / time.Millisecond)
	half := ttlMs / 2
	if c.Quorum.Role() == quorum.Leader {
		fresh := ok && now < cur.GrantedAtMs+ttlMs
		if !ok || !fresh {
			holder := c.assignmentHolder(name, 0)
			if ok {
				holder = c.NodeID
			}
			epoch := uint64(1)
			if ok {
				epoch = cur.Epoch + 1
			}
			lease := quorum.Lease{Topic: name, Partition: 0, Holder: holder, Epoch: epoch, GrantedAtMs: now}
			data, _ := json.Marshal(lease)
			_, _, _ = c.Quorum.ProposeNamed(quorum.EntryLeaseGrant, data)
		}
	} else if ok && cur.Holder == c.NodeID && now >= cur.GrantedAtMs+half {
		epoch := cur.Epoch + 1
		lease := quorum.Lease{Topic: name, Partition: 0, Holder: c.NodeID, Epoch: epoch, GrantedAtMs: now}
		data, _ := json.Marshal(lease)
		_, _, _ = c.Quorum.ProposeNamed(quorum.EntryLeaseGrant, data)
		key := fmt.Sprintf("\x00%s", name)
		if c.claimEpoch[key] != epoch {
			c.claimEpoch[key] = epoch
			c.pullFromInternalPeers(ctx, name)
		}
	}
}

// replicateInternalCommitted pulls committed internal blocks missing locally.
func (c *Config) replicateInternalCommitted(ctx context.Context, name string, fl *log.FileLog) {
	committed := c.Quorum.Blocks(name, 0)
	if len(committed) == 0 {
		return
	}
	local := map[string]bool{}
	for _, d := range fl.Blocks() {
		local[hex.EncodeToString(d.ID[:])] = true
	}
	for _, blk := range committed {
		key := hex.EncodeToString(blk.ID)
		if local[key] {
			continue
		}
		for _, peer := range c.Peers {
			if err := c.pullBlockFromInternal(ctx, peer, name, blk); err != nil {
				slog.Warn("internal block pull failed", "peer", peer, "err", err)
				continue
			}
			break
		}
	}
}

// pushInternalSealed pushes sealed blocks not yet committed in the quorum.
func (c *Config) pushInternalSealed(ctx context.Context, name string, fl *log.FileLog) {
	committed := c.committedIDs(name, 0)
	blockCount := len(fl.Blocks())
	if blockCount > 0 {
		slog.Debug("pushInternalSealed", "topic", name, "blocks", blockCount, "committed", len(committed))
	}
	for _, d := range fl.Blocks() {
		key := hex.EncodeToString(d.ID[:])
		if committed[key] {
			continue
		}
		raw, err := fl.ReadRange(d.FirstOffset, d.LastOffset)
		if err != nil {
			slog.Warn("internal block read", "err", err)
			continue
		}
		pushed := 1
		for _, peer := range c.Peers {
			if err := c.pushBlock(ctx, peer, name, 0, raw, d.FirstOffset); err != nil {
				slog.Warn("internal block push failed", "peer", peer, "err", err)
				continue
			}
			pushed++
		}
		if pushed >= 2 {
			c.proposeInternalBlockCommit(name, 0, d)
		}
	}
}

// pullBlockFromInternal pulls one committed block from a peer.
func (c *Config) pullBlockFromInternal(ctx context.Context, peer, name string, blk quorum.BlockCommit) error {
	conn, err := c.Dial(ctx, peer)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := firnpb.NewReplicaServiceClient(conn)
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fr, err := client.ReplicaFetch(ctx2, &firnpb.ReplicaFetchRequest{
		Topic:       name,
		Partition:   0,
		FirstOffset: blk.FirstOffset,
		LastOffset:  blk.LastOffset,
	})
	if err != nil {
		return err
	}
	frames, err := log.ParseFrames(fr.Frames)
	if err != nil {
		return fmt.Errorf("pull %s: %w", name, err)
	}
	if len(frames) != int(blk.RecordCount) {
		return fmt.Errorf("pull %s: got %d frames, want %d", name, len(frames), blk.RecordCount)
	}
	if frames[0].Offset != blk.FirstOffset || frames[len(frames)-1].Offset != blk.LastOffset {
		return fmt.Errorf("pull %s: offsets mismatch", name)
	}
	id := sha256Of(fr.Frames)
	if !bytes.Equal(id[:], blk.ID) {
		return fmt.Errorf("pull %s: block id mismatch", name)
	}
	fl, err := c.internalLog(name)
	if err != nil {
		return err
	}

	// Apply via AppendReplicated, preserving offset+ts and encoding identity.
	appHWM := fl.HighWatermark()
	for _, f := range frames {
		if f.Offset < appHWM {
			continue
		}
		if f.Offset != appHWM {
			return fmt.Errorf("pull %s: gap (got %d, want %d)", name, f.Offset, appHWM)
		}
		fl.AppendReplicated(f.Ts, f.Key, f.Value)
		appHWM++
	}
	return nil
}

// proposeInternalBlockCommit pushes a sealed internal block's descriptor onto
// the metadata log after a majority holds a durable copy.
func (c *Config) proposeInternalBlockCommit(name string, partition int32, d log.BlockDescriptor) {
	dc := quorum.BlockCommit{
		Topic:       name,
		Partition:   partition,
		BaseOffset:  d.BaseOffset,
		FirstOffset: d.FirstOffset,
		LastOffset:  d.LastOffset,
		Size:        d.Size,
		RecordCount: d.RecordCount,
		CRCSet:      d.CRCSet,
		SealedAtMs:  d.SealedAt.UnixMilli(),
		ID:          d.ID[:],
	}
	data, _ := json.Marshal(dc)
	_, _, _ = c.Quorum.ProposeNamed(quorum.EntryBlockCommit, data)
}

// pullFromInternalPeers mirrors pullFromPeers for internal topics.
func (c *Config) pullFromInternalPeers(ctx context.Context, name string) {
	fl, err := c.internalLog(name)
	if err != nil {
		return
	}
	localHWM := fl.HighWatermark()
	for _, peer := range c.Peers {
		conn, err := c.Dial(ctx, peer)
		if err != nil {
			continue
		}
		client := firnpb.NewReplicaServiceClient(conn)
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		fr, err := client.ReplicaFetch(ctx2, &firnpb.ReplicaFetchRequest{
			Topic:       name,
			Partition:   0,
			FirstOffset: localHWM,
			LastOffset:  1 << 62,
		})
		cancel()
		_ = conn.Close()
		if err != nil {
			slog.Warn("pull", "peer", peer, "err", err)
			continue
		}
		frames, err := log.ParseFrames(fr.Frames)
		if err != nil || len(frames) == 0 {
			continue
		}
		for _, f := range frames {
			if f.Offset < fl.HighWatermark() {
				continue
			}
			fl.AppendReplicated(f.Ts, f.Key, f.Value)
		}
		return
	}
}

// ID returns this node's cluster identity.
func (c *Config) ID() string { return c.NodeID }

// AdvertisedAddr returns this node's advertised gRPC address.
func (c *Config) AdvertisedAddr() string { return c.Advertised[c.NodeID] }

// NodeAddr looks up the advertised address of a node id.
func (c *Config) NodeAddr(id string) (string, bool) {
	addr, ok := c.Advertised[id]
	return addr, ok
}

// Leader returns the current quorum leader id, or "" if none.
func (c *Config) Leader() string { return c.Quorum.Leader() }

// AllNodes returns all node ids in the cluster (including self).
func (c *Config) AllNodes() []string { return append([]string(nil), c.All...) }

// NewAgent constructs a replication agent.
func NewAgent(cfg Config) *Config {
	if cfg.Dial == nil {
		cfg.Dial = func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	}
	if cfg.Interval == 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = defaultLeaseTTL
	}
	cfg.slogLogger = slog.Default()
	cfg.claimEpoch = map[string]uint64{}
	cfg.internalLogs = map[string]*log.FileLog{}
	cfg.stop = make(chan struct{})
	cfg.done = make(chan struct{})
	return &cfg
}

// Stop asks the agent loop to exit and waits for it to return. Idempotent.
func (c *Config) Stop() {
	select {
	case <-c.done:
		return
	default:
		select {
		case <-c.stop:
		default:
			close(c.stop)
		}
		<-c.done
	}
}

// Run starts the agent; it stops when ctx is cancelled or Stop is called.
// It closes done on return, letting Stop wait for in-flight ticks.
func (c *Config) Run(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stop:
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Config) tick(ctx context.Context) {
	c.maintainLeases(ctx)
	for _, t := range c.Registry.List() {
		for i := 0; i < t.Partitions; i++ {
			c.replicateCommitted(ctx, t.Name, int32(i))
			c.pushSealed(ctx, t.Name, int32(i))
		}
	}
	// Internal topics (ADR-007: replicate like user topics). The internal
	// logs are opened lazily and kept for the lifetime of the agent.
	for _, name := range InternalTopics {
		fl, err := c.internalLog(name)
		if err != nil {
			slog.Warn("open internal log", "topic", name, "err", err)
			continue
		}
		sealed, _ := fl.SealIfOlderThan(c.Interval)
		slog.Debug("internalTick", "topic", name, "hwm", fl.HighWatermark(), "sealed", sealed)
		c.maintainInternalLease(ctx, name)
		c.replicateInternalCommitted(ctx, name, fl)
		c.pushInternalSealed(ctx, name, fl)
	}
	c.Broker.SealAll()
}

// InternalTopics are internal single-partition logs that replicate like
// ordinary user topics. They are discovered, not configured: if present they
// get replicated; absent, they are ignored on open.
var InternalTopics = []string{"__consumer_offsets", "__queue_attempts", "__audit", "__api_keys", "__federation_peers", "__federation_cursors"}

func fnv64(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// maintainLeases grants/renews write leases for partitions. Only the quorum
// leader may propose, so this runs on the leader only:
//
//   - missing lease: grant to the statically-assigned holder (deterministic)
//   - expired lease: grant to the leader itself (known-alive) — v1
//     limitation, documented: balanced re-assignment needs node health
//     probing, deferred to a later phase
//   - own fresh lease: nothing to do
func (c *Config) maintainLeases(ctx context.Context) {
	leader := c.Quorum.Role() == quorum.Leader
	now := time.Now().UnixMilli()
	ttlMs := int64(c.LeaseTTL / time.Millisecond)
	half := ttlMs / 2
	for _, t := range c.Registry.List() {
		for i := 0; i < t.Partitions; i++ {
			p := int32(i)
			cur, ok := c.Quorum.Lease(t.Name, p)
			fresh := ok && now < cur.GrantedAtMs+ttlMs

			switch {
			case ok && cur.Holder == c.NodeID:
				// Self-granted epoch change: pull anything we're missing
				// from peers (ADR-007 unseal->re-seal handover).
				key := fmt.Sprintf("%s\x00%d", t.Name, p)
				if c.claimEpoch[key] != cur.Epoch {
					c.claimEpoch[key] = cur.Epoch
					c.pullFromPeers(ctx, t.Name, p)
				}
				// Holder renews its own lease (epoch+1 keeps ack monotonicity),
				// but only past half its TTL — renewing on every tick would churn.
				if now >= cur.GrantedAtMs+half {
					epoch := cur.Epoch + 1
					lease := quorum.Lease{Topic: t.Name, Partition: p, Holder: c.NodeID, Epoch: epoch, GrantedAtMs: now}
					data, _ := json.Marshal(lease)
					_, _, _ = c.Quorum.ProposeNamed(quorum.EntryLeaseGrant, data)
				}

			case !ok || !fresh:
				if !leader {
					continue
				}
				// Missing or expired: initial assignment to the statically
				// assigned node; takeover of an expired lease lands on the
				// leader itself (known-alive) — documented v1 limitation.
				holder := c.assignmentHolder(t.Name, p)
				if ok {
					holder = c.NodeID
				}
				epoch := uint64(1)
				if ok {
					epoch = cur.Epoch + 1
				}
				lease := quorum.Lease{Topic: t.Name, Partition: p, Holder: holder, Epoch: epoch, GrantedAtMs: now}
				data, _ := json.Marshal(lease)
				_, _, _ = c.Quorum.ProposeNamed(quorum.EntryLeaseGrant, data)
			}
		}
	}
}

// assignmentHolder computes the static preferred holder for (topic, partition):
// sticky by hash, independent of leader.
func (c *Config) assignmentHolder(topic string, partition int32) string {
	n := len(c.All)
	if n == 0 {
		return c.NodeID
	}
	h := fnv64(topic) ^ uint64(partition+1)*0x9E3779B97F4A7C15
	return c.All[int(h%uint64(n))]
}

// pullFromPeers pulls every frame above our local high watermark from peers.
// Used on self-granted epochs — recovery from a dirty handover.
func (c *Config) pullFromPeers(ctx context.Context, topic string, partition int32) {
	localHWM, err := c.Broker.PartitionHighWatermark(ctx, topic, partition)
	if err != nil {
		return
	}
	for _, peer := range c.Peers {
		conn, err := c.Dial(ctx, peer)
		if err != nil {
			continue
		}
		client := firnpb.NewReplicaServiceClient(conn)
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		fr, err := client.ReplicaFetch(ctx2, &firnpb.ReplicaFetchRequest{
			Topic:       topic,
			Partition:   partition,
			FirstOffset: localHWM,
			LastOffset:  1 << 62, // clamped server-side to end of log
		})
		cancel()
		_ = conn.Close()
		if err != nil {
			slog.Warn("pull", "peer", peer, "err", err)
			continue
		}
		frames, err := log.ParseFrames(fr.Frames)
		if err != nil || len(frames) == 0 {
			continue
		}
		_, _ = c.Broker.AppendReplicatedFrames(topic, partition, frames)
		return // one peer is enough
	}
}

// pushSealed replicates sealed blocks that are not yet committed, then
// proposes EntryBlockCommit once a majority has durable copies.
func (c *Config) pushSealed(ctx context.Context, topic string, partition int32) {
	descs, err := c.Broker.Blocks(topic)
	if err != nil {
		slog.Warn("blocks", "err", err)
		return
	}
	slog.Debug("pushSealed", "topic", topic, "descs", len(descs))
	committed := c.committedIDs(topic, partition)
	for _, d := range descs {
		if d.Partition != partition {
			continue
		}
		key := hex.EncodeToString(d.Descriptor.ID[:])
		if committed[key] {
			continue
		}
		// Push the block bytes to all peers.
		raw, err := c.Broker.ReadRange(ctx, topic, partition, d.Descriptor.FirstOffset, d.Descriptor.LastOffset)
		if err != nil {
			slog.Error("read block", "err", err)
			continue
		}
		pushed := 1 // self counts
		for _, peer := range c.Peers {
			if err := c.pushBlock(ctx, peer, topic, partition, raw, d.Descriptor.FirstOffset); err != nil {
				slog.Warn("block push failed", "peer", peer, "err", err)
				continue
			}
			pushed++
		}
		// Majority of 3 = 2; of 2 = 2. Require 2 pushed (self + 1).
		if pushed >= 2 {
			c.proposeBlockCommit(topic, partition, d.Descriptor)
		}
	}
}

// replicateCommitted pulls committed blocks that are missing locally.
func (c *Config) replicateCommitted(ctx context.Context, topic string, partition int32) {
	committed := c.Quorum.Blocks(topic, partition)
	if len(committed) == 0 {
		return
	}
	descs, err := c.Broker.Blocks(topic)
	if err != nil {
		return
	}
	local := map[string]bool{}
	for _, d := range descs {
		if d.Partition == partition {
			local[hex.EncodeToString(d.Descriptor.ID[:])] = true
		}
	}
	for _, blk := range committed {
		key := hex.EncodeToString(blk.ID)
		if local[key] {
			continue
		}
		// Pull from peers.
		for _, peer := range c.Peers {
			if err := c.pullBlock(ctx, peer, topic, partition, blk); err != nil {
				slog.Warn("block pull failed", "peer", peer, "err", err)
				continue
			}
			break
		}
	}
}

// committedIDs returns the set of block IDs committed in the quorum applied
// state for (topic, partition).
func (c *Config) committedIDs(topic string, partition int32) map[string]bool {
	out := map[string]bool{}
	for _, b := range c.Quorum.Blocks(topic, partition) {
		out[hex.EncodeToString(b.ID)] = true
	}
	return out
}

func (c *Config) pushBlock(ctx context.Context, peer, topic string, partition int32, raw []byte, firstOffset int64) error {
	conn, err := c.Dial(ctx, peer)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := firnpb.NewReplicaServiceClient(conn)
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = client.ReplicaPut(ctx2, &firnpb.ReplicaPutRequest{
		Topic:      topic,
		Partition:  partition,
		Frames:     raw,
		BaseOffset: firstOffset,
	})
	return err
}

func (c *Config) pullBlock(ctx context.Context, peer, topic string, partition int32, blk quorum.BlockCommit) error {
	conn, err := c.Dial(ctx, peer)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := firnpb.NewReplicaServiceClient(conn)
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fr, err := client.ReplicaFetch(ctx2, &firnpb.ReplicaFetchRequest{
		Topic:       topic,
		Partition:   partition,
		FirstOffset: blk.FirstOffset,
		LastOffset:  blk.LastOffset,
	})
	if err != nil {
		return err
	}
	// Verify integrity before applying: frames must decode and cover exactly
	// [first, last].
	frames, err := log.ParseFrames(fr.Frames)
	if err != nil {
		return fmt.Errorf("pull %s: %w", topic, err)
	}
	if len(frames) != int(blk.RecordCount) {
		return fmt.Errorf("pull %s: got %d frames, want %d", topic, len(frames), blk.RecordCount)
	}
	if frames[0].Offset != blk.FirstOffset || frames[len(frames)-1].Offset != blk.LastOffset {
		return fmt.Errorf("pull %s: frame offsets do not match descriptor", topic)
	}
	id := sha256Of(fr.Frames)
	if !bytes.Equal(id[:], blk.ID) {
		return fmt.Errorf("pull %s: block id mismatch (got %x want %x)", topic, id[:8], blk.ID[:8])
	}

	// Apply via the broker's atomic replication path.
	if _, err := c.Broker.AppendReplicatedFrames(topic, partition, frames); err != nil {
		return fmt.Errorf("pull %s: %w", topic, err)
	}
	return nil
}

func (c *Config) proposeBlockCommit(topic string, partition int32, d log.BlockDescriptor) {
	bc := quorum.BlockCommit{
		Topic:       topic,
		Partition:   partition,
		BaseOffset:  d.BaseOffset,
		FirstOffset: d.FirstOffset,
		LastOffset:  d.LastOffset,
		Size:        d.Size,
		RecordCount: d.RecordCount,
		CRCSet:      d.CRCSet,
		SealedAtMs:  d.SealedAt.UnixMilli(),
		ID:          d.ID[:],
	}
	data, _ := json.Marshal(bc)
	_, _, _ = c.Quorum.ProposeNamed(quorum.EntryBlockCommit, data)
}

func sha256Of(b []byte) [32]byte { return sha256.Sum256(b) }
