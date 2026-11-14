// Package federation — passive one-way replication (ADR-010, plan 004 slice 2).
//
// The puller imports byte-identical sealed frame ranges from each configured
// passive peer into the local log, preserving source offsets exactly. It
// never writes anything the source does not produce: the sink stays idle
// until an operator flips the peer to active-active and the cutover runbook
// is executed (docs/plans/004-federation.md, slice 6).
package federation

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/log"
)

// QueueAttemptsTopic is the one internal topic that is mirrored
// byte-identically when any queue is in a passive peer's scope (ADR-010:
// attempts/acks/DLQ state ride along with the queue's records).
const QueueAttemptsTopic = "__queue_attempts"

// LagFunc reports the per-partition federation cursor lag in seconds.
// wired to firn_federation_cursor_lag_seconds in firnd; nil disables the
// metric (tests).
type LagFunc func(peer, topic string, partition int32, lagSeconds float64)

// PullerConfig wires a federation puller into this (destination) cluster.
type PullerConfig struct {
	Broker     *broker.Broker // destination
	Store      *Store         // control-plane store (peers + cursors)
	Interval   time.Duration  // poll cadence (default 500ms, -federation-interval)
	Batch      int            // max frames per fetch window (default 1024, -federation-batch)
	MaxRange   int64          // max bytes fetched per poll (default 1 MiB, -federation-max-range)
	Dial       func(ctx context.Context, peer Peer) (*grpc.ClientConn, error)
	Logger     *slog.Logger
	LagSeconds LagFunc
	// SelfName is this cluster's identity as the peer registers it, sent as
	// requesting_peer so the serving side can omit the frames this cluster
	// donated to it (the echo filter). Empty disables the filter.
	SelfName string
}

type Puller struct {
	cfg    PullerConfig
	logger *slog.Logger
	stop   chan struct{}
	done   chan struct{} // closed when Run returns; Stop waits on it

	started time.Time
	lagMu   sync.Mutex
	lag     map[string]time.Time // peer|topic|partition -> last caught-up time

	rrMu      sync.Mutex
	rr        map[string][]LocalRange // peer|queue|partition -> peer's published foreign ranges
	rrFetched map[string]time.Time    // peer|queue|partition -> when rr was populated (10s TTL)
}

func NewPuller(cfg PullerConfig) *Puller {
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 1024
	}
	if cfg.MaxRange <= 0 {
		cfg.MaxRange = 1 << 20
	}
	if cfg.Dial == nil {
		panic("federation: PullerConfig.Dial is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Puller{cfg: cfg, logger: cfg.Logger, stop: make(chan struct{}), done: make(chan struct{}), started: time.Now(), lag: map[string]time.Time{}, rr: map[string][]LocalRange{}, rrFetched: map[string]time.Time{}}
}

// Run polls until ctx is cancelled or Stop is called. It returns after any
// in-flight poll finishes, so callers may Stop() and then safely close the
// broker without racing a pending append.
func (p *Puller) Run(ctx context.Context) {
	defer close(p.done)
	tick := time.NewTicker(p.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stop:
			return
		case <-tick.C:
			p.poll(ctx)
		}
	}
}

// Stop stops the poll loop and waits for Run to return.
func (p *Puller) Stop() {
	select {
	case <-p.done:
		return
	default:
		close(p.stop)
		<-p.done
	}
}

// poll pulls from every passive- or active-active peer registered with
// direction in|both. Failures are logged and retried next tick; abort none.
func (p *Puller) poll(ctx context.Context) {
	for _, peer := range p.cfg.Store.ListPeers() {
		peer := peer
		if peer.Mode != ModePassive && peer.Mode != ModeActiveActive {
			continue
		}
		if peer.Direction != DirectionIn && peer.Direction != DirectionBoth {
			continue
		}
		p.pullPeer(ctx, peer)
	}
}

// pullPeer imports every selected topic/partition from one peer.
func (p *Puller) pullPeer(ctx context.Context, peer Peer) {
	for _, sel := range peer.Selectors {
		// Topics. The documented rule (ADR-010): only a completely empty
		// selector ("all topics everywhere") expands to every local topic.
		// A queues-only peer imports only its named queues — never
		// unrelated streams. Internal names are never pulled by selector.
		topics := sel.Topics
		if len(topics) == 0 && len(sel.Queues) == 0 {
			topics = p.localTopics()
		}
		for _, topic := range topics {
			if strings.HasPrefix(topic, internalPrefix) {
				continue // defense in depth: blocklist enforced at registration too
			}
			p.pullTopic(ctx, peer, topic, sel)
		}
		// Queue messages themselves ride along with their queue: the queue's
		// message log is a queue-mode topic, imported byte-identically like
		// any stream topic. A queue in the selector means the whole queue
		// (messages + attempts), not the attempts bookkeeper alone.
		for _, q := range sel.Queues {
			if strings.HasPrefix(q, internalPrefix) {
				continue
			}
			p.pullTopic(ctx, peer, q, sel)
		}
		// __queue_attempts mirror: fires whenever a queue-like topic is in
		// scope (named in Topics or Queues and registered locally as a
		// queue-mode topic, or any queue named, or selector unrestricted).
		// Attempts are tiny; the byte-identical passive mirror moves the
		// whole log, the active-active path re-keys in scope and drops the
		// rest deliberately.
		if p.hasQueueInScope(peer) {
			p.importQueueAttempts(ctx, peer)
		}
	}
}

// hasQueueInScope reports whether the peer's selectors may cover a queue.
// A queue IS a topic (queue-mode), so a queue named as a topic counts; the
// registry decides mode locally. Defaults to true when unsurered so the
// attempts mirror only runs when a queue is provably in scope.
func (p *Puller) hasQueueInScope(peer Peer) bool {
	for _, sel := range peer.Selectors {
		if len(sel.Topics) == 0 && len(sel.Queues) == 0 {
			return true // unrestricted
		}
		for _, q := range sel.Queues {
			if q != "" {
				return true
			}
		}
		for _, t := range sel.Topics {
			if tm, err := p.cfg.Broker.TopicMeta(t); err == nil && tm.Mode == "queue" {
				return true
			}
		}
	}
	return false
}

func (p *Puller) localTopics() []string {
	out := []string{}
	for _, t := range p.cfg.Broker.Topics() {
		if !strings.HasPrefix(t, internalPrefix) {
			out = append(out, t)
		}
	}
	return out
}

// pullTopic imports every partition of one topic.
func (p *Puller) pullTopic(ctx context.Context, peer Peer, topic string, sel Selector) {
	tmeta, err := p.cfg.Broker.TopicMeta(topic)
	if err != nil {
		p.logger.Warn("federation: topic not known on sink; pre-create it before importing", "peer", peer.Name, "topic", topic)
		return
	}
	for pi := 0; pi < tmeta.Partitions; pi++ {
		p.pullPartition(ctx, peer, topic, int32(pi))
	}
}

func (p *Puller) pullPartition(ctx context.Context, peer Peer, topic string, partition int32) {
	cursor, hasCursor := p.cursorValue(peer.Name, topic, partition)
	first := int64(0)
	if hasCursor {
		first = cursor.LastSourceOffset + 1
	}

	conn, err := p.cfg.Dial(ctx, peer)
	if err != nil {
		p.observeLag(peer.Name, topic, partition, false)
		p.logger.Warn("federation: dial peer failed", "peer", peer.Name, "err", err)
		return
	}
	defer func() { _ = conn.Close() }()
	client := firnpb.NewFederationServiceClient(conn)

	// Registration validation (ADR-010): partition counts must match on both
	// sides. A mismatch is fatal for the import — log loudly, skip, move on.
	// Also the only cheap place to detect a peer that died between ticks.
	ctxRpt, cancelRpt := context.WithTimeout(outgoingBearer(ctx, peer.Key), 30*time.Second)
	rpt, err := client.FederationReport(ctxRpt, &firnpb.FederationReportRequest{Topic: topic, Partition: partition})
	cancelRpt()
	if err != nil {
		p.observeLag(peer.Name, topic, partition, false)
		p.logger.Warn("federation: report failed", "peer", peer.Name, "topic", topic, "err", err)
		return
	}
	tmeta, err := p.cfg.Broker.TopicMeta(topic)
	if err != nil {
		p.observeLag(peer.Name, topic, partition, false)
		p.logger.Warn("federation: topic not known on sink; pre-create it", "peer", peer.Name, "topic", topic)
		return
	}
	if rpt.Partitions != int32(tmeta.Partitions) {
		p.observeLag(peer.Name, topic, partition, false)
		p.logger.Error("federation: partition count mismatch — create the topic on both sides with the SAME partition count", "peer", peer.Name, "topic", topic, "source", rpt.Partitions, "local", tmeta.Partitions)
		return
	}

	ctx2, cancel := context.WithTimeout(outgoingBearer(ctx, peer.Key), 30*time.Second)
	fr, err := client.FederationFetch(ctx2, &firnpb.FederationFetchRequest{
		Topic:          topic,
		Partition:      partition,
		FirstOffset:    first,
		LastOffset:     -1, // clamp to source high watermark
		RequestingPeer: p.cfg.SelfName,
	})
	cancel()
	if err != nil {
		p.observeLag(peer.Name, topic, partition, false)
		p.logger.Warn("federation: fetch failed", "peer", peer.Name, "topic", topic, "partition", partition, "err", err)
		return
	}

	if len(fr.Frames) == 0 {
		// Everything in the requested window was echo content the peer
		// filtered out — advance the cursor to the source HWM so the echo
		// does not re-fetch forever. If we already had a cursor, keep it.
		if hasCursor && fr.SourceHwm > 0 && fr.SourceHwm-1 > cursor.LastSourceOffset {
			cursor.LastSourceOffset = fr.SourceHwm - 1
			if hw, err := p.cfg.Broker.PartitionHighWatermark(ctx, topic, partition); err == nil && hw > 0 {
				cursor.LastLocalOffset = hw - 1
			}
			_ = p.cfg.Store.SetCursor(cursor)
		}
		p.observeLag(peer.Name, topic, partition, true)
		return
	}

	frames, err := log.ParseFrames(fr.Frames)
	if err != nil {
		p.observeLag(peer.Name, topic, partition, false)
		p.logger.Error("federation: CRC/parse failure; dropping range, cursor untouched", "peer", peer.Name, "topic", topic, "partition", partition, "err", err)
		return
	}
	// Redelivery safety: skip any served frame at or below the source offset
	// already imported (server-side filter may be absent).
	if hasCursor && cursor.LastSourceOffset >= 0 {
		kept := frames[:0]
		for _, f := range frames {
			if f.Offset > cursor.LastSourceOffset {
				kept = append(kept, f)
			}
		}
		frames = kept
	}

	var firstLocal, hwm, sourceStart int64 = -1, -1, -1
	if len(frames) > 0 {
		if peer.Mode == ModeActiveActive {
			firstLocal, hwm, err = p.cfg.Broker.AppendReoriginedFrames(topic, partition, frames)
			if err != nil {
				p.observeLag(peer.Name, topic, partition, false)
				p.logger.Error("federation: reorigin apply failed; cursor untouched", "peer", peer.Name, "topic", topic, "partition", partition, "err", err)
				return
			}
			sourceStart = frames[0].Offset // first non-skipped frame
		} else {
			// Passive: byte-identical import via the replicated-apply path.
			preHWM, _ := p.cfg.Broker.PartitionHighWatermark(ctx, topic, partition)
			hw, err := p.cfg.Broker.AppendReplicatedFrames(topic, partition, frames)
			if err != nil {
				p.observeLag(peer.Name, topic, partition, false)
				p.logger.Error("federation: apply failed; cursor untouched", "peer", peer.Name, "topic", topic, "partition", partition, "err", err)
				return
			}
			hwm = hw
			firstLocal = preHWM
			sourceStart = preHWM // offsets are identical in passive mode
		}
	}
	p.observeLag(peer.Name, topic, partition, true)

	lastLocal := int64(-1)
	if hw, err := p.cfg.Broker.PartitionHighWatermark(ctx, topic, partition); err == nil {
		lastLocal = hw - 1
	}
	cursor = Cursor{
		Peer:             peer.Name,
		Topic:            topic,
		Partition:        partition,
		LastSourceOffset: fr.SourceHwm - 1,
		LastLocalOffset:  lastLocal,
		Mode:             peer.Mode,
		Origin:           "remote",
		ForeignRanges:    cursor.ForeignRanges,
	}
	if len(frames) > 0 && firstLocal >= 0 {
		cursor.ForeignRanges = append(cursor.ForeignRanges, LocalRange{Start: firstLocal, End: hwm - 1, SourceStart: sourceStart})
	}
	_ = p.cfg.Store.SetCursor(cursor)
	p.logger.Info("federation: imported", "peer", peer.Name, "topic", topic, "partition", partition, "mode", peer.Mode.String(), "frames", len(frames))
}

// importQueueAttempts mirrors the source's __queue_attempts log
// byte-identically. The sink must not deliver from the mirrored state until
// the operator cuts the queue over (passive → cutover runbook).
//
// Only an empty local attempts log imports cleanly; a sink that has ever
// served a queue federated here must be reseeded (ADR-010). The fork check
// in applyFramesToReplicatedLog makes the failure loud, never silent.
func (p *Puller) importQueueAttempts(ctx context.Context, peer Peer) {
	fl, ok := p.cfg.Broker.InternalLog(QueueAttemptsTopic)
	if !ok {
		p.logger.Warn("federation: no shared __queue_attempts handle on sink; skipping mirror", "peer", peer.Name)
		return
	}
	cursor, hasCursor := p.cursorValue(peer.Name, QueueAttemptsTopic, 0)
	first := int64(0)
	if hasCursor {
		first = cursor.LastSourceOffset + 1
	}

	conn, err := p.cfg.Dial(ctx, peer)
	if err != nil {
		p.observeLag(peer.Name, QueueAttemptsTopic, 0, false)
		p.logger.Warn("federation: dial peer failed", "peer", peer.Name, "err", err)
		return
	}
	defer func() { _ = conn.Close() }()

	ctx2, cancel := context.WithTimeout(outgoingBearer(ctx, peer.Key), 30*time.Second)
	defer cancel()
	fr, err := firnpb.NewFederationServiceClient(conn).FederationFetch(ctx2, &firnpb.FederationFetchRequest{
		Topic:          QueueAttemptsTopic,
		Partition:      0,
		FirstOffset:    first,
		LastOffset:     -1,
		RequestingPeer: p.cfg.SelfName,
	})
	if err != nil {
		p.observeLag(peer.Name, QueueAttemptsTopic, 0, false)
		p.logger.Warn("federation: attempts fetch failed", "peer", peer.Name, "err", err)
		return
	}
	if len(fr.Frames) == 0 {
		// All echoes filtered out: advance the cursor so the filter
		// boundary does not re-fetch forever.
		if hasCursor && fr.SourceHwm > 0 && fr.SourceHwm-1 > cursor.LastSourceOffset {
			cursor.LastSourceOffset = fr.SourceHwm - 1
			cursor.LastLocalOffset = fl.HighWatermark() - 1
			_ = p.cfg.Store.SetCursor(cursor)
		}
		p.observeLag(peer.Name, QueueAttemptsTopic, 0, true)
		return // caught up
	}

	frames, err := log.ParseFrames(fr.Frames)
	if err != nil {
		p.observeLag(peer.Name, QueueAttemptsTopic, 0, false)
		p.logger.Error("federation: attempts CRC failure; cursor untouched", "peer", peer.Name, "err", err)
		return
	}

	if peer.Mode == ModeActiveActive {
		p.importAttemptsRekeyed(ctx, fl, peer, cursor, hasCursor, frames, fr.SourceHwm)
		return
	}

	// Redelivery safety + shared skip logic: server filters echoes, we skip
	// at-or-below the cursor defensively.
	if hasCursor && cursor.LastSourceOffset >= 0 {
		kept := frames[:0]
		for _, f := range frames {
			if f.Offset > cursor.LastSourceOffset {
				kept = append(kept, f)
			}
		}
		frames = kept
	}
	preHWM := fl.HighWatermark()
	if _, err := applyFramesToReplicatedLog(fl, frames); err != nil {
		p.observeLag(peer.Name, QueueAttemptsTopic, 0, false)
		p.logger.Error("federation: attempts apply failed; reconcile the sink reseed before cutover", "peer", peer.Name, "err", err)
		return
	}
	p.observeLag(peer.Name, QueueAttemptsTopic, 0, true)
	hwm := fl.HighWatermark()
	cursor = Cursor{
		Peer:             peer.Name,
		Topic:            QueueAttemptsTopic,
		Partition:        0,
		LastSourceOffset: fr.SourceHwm - 1,
		LastLocalOffset:  hwm - 1,
		Mode:             ModePassive,
		Origin:           "remote",
		ForeignRanges:    cursor.ForeignRanges,
	}
	if hwm > preHWM {
		first := preHWM // passive byte-identical: source offsets == local
		cursor.ForeignRanges = append(cursor.ForeignRanges, LocalRange{Start: preHWM, End: hwm - 1, SourceStart: first})
	}
	_ = p.cfg.Store.SetCursor(cursor)
	p.logger.Info("federation: mirrored attempts", "peer", peer.Name, "frames", len(frames))
}

// importAttemptsRekeyed mirrors attempts for active-active queue federation:
// attempt keys carry the source's message offsets, which differ from this
// cluster's, so each frame is re-keyed into the local offset space via the
// queue topic's foreign-applied ranges. Frames whose message has not been
// imported yet are skipped this tick; the cursor stops at the last translated
// frame, so they are served again next round (at-least-once).
func (p *Puller) importAttemptsRekeyed(ctx context.Context, fl *log.FileLog, peer Peer, cursor Cursor, hasCursor bool, frames []log.Frame, sourceHWM int64) {
	held := false
	rekeyed := make([]log.Frame, 0, len(frames))
	firstSource := int64(-1)
	lastSource := int64(-1)
	for _, f := range frames {
		q, part, off, ok := parseAttemptKey(f.Key)
		if !ok {
			continue // malformed key: nothing to apply, nothing to lose
		}
		if !attemptKeyInScope(peer, q) {
			continue // out of scope: deliberate drop
		}
		localRanges := p.cfg.Store.ForeignRanges(peer.Name, q, part)
		localOff, ok := translateOffset(localRanges, off)
		if !ok {
			// Not imported by this node — use the peer's published ranges
			// for the inverse mapping instead (federation_report).
			localOff, ok = translateOffsetInverse(p.remoteRanges(ctx, peer, q, part), off)
		}
		if !ok {
			// The message record hasn't imported locally yet — hold the
			// cursor at the last translated frame so the source re-sends
			// this one once the record lands.
			held = true
			break
		}
		rekeyed = append(rekeyed, log.Frame{Ts: f.Ts, Key: encodeAttemptKey(q, part, localOff), Value: f.Value})
		if firstSource < 0 {
			firstSource = f.Offset
		}
		lastSource = f.Offset
	}

	if len(rekeyed) == 0 {
		if !held {
			// Every frame was out of scope or malformed — account for them.
			cursor.LastSourceOffset = sourceHWM - 1
			cursor.LastLocalOffset = fl.HighWatermark() - 1
			cursor.Mode = ModeActiveActive
			cursor.Origin = "remote"
			_ = p.cfg.Store.SetCursor(cursor)
		}
		p.observeLag(peer.Name, QueueAttemptsTopic, 0, !held)
		return
	}

	firstLocal, hwm := applyRekeyedAttempts(fl, rekeyed)
	p.observeLag(peer.Name, QueueAttemptsTopic, 0, true)

	advance := lastSource
	if !held {
		advance = sourceHWM - 1
	}
	cursor = Cursor{
		Peer:             peer.Name,
		Topic:            QueueAttemptsTopic,
		Partition:        0,
		LastSourceOffset: advance,
		LastLocalOffset:  hwm - 1,
		Mode:             ModeActiveActive,
		Origin:           "remote",
		ForeignRanges:    cursor.ForeignRanges,
	}
	if firstLocal >= 0 {
		cursor.ForeignRanges = append(cursor.ForeignRanges, LocalRange{Start: firstLocal, End: hwm - 1, SourceStart: firstSource})
	}
	_ = p.cfg.Store.SetCursor(cursor)
	p.logger.Info("federation: mirrored attempts (re-keyed)", "peer", peer.Name, "frames", len(rekeyed), "held", held)
}

// applyFramesToReplicatedLog is the byte-identical apply path for an
// internal *log.FileLog without a meta-registry topic: frames below HWM are
// skipped, a frame beyond HWM is a hard fork error, contiguous frames are
// appended preserving ts/key/value and re-stamping offsets in order.
func applyFramesToReplicatedLog(fl *log.FileLog, frames []log.Frame) (int64, error) {
	for _, f := range frames {
		hwm := fl.HighWatermark()
		if f.Offset < hwm {
			continue
		}
		if f.Offset != hwm {
			return hwm, fmt.Errorf("federation: replica fork: frame offset %d, local high watermark %d", f.Offset, hwm)
		}
		fl.AppendReplicated(f.Ts, f.Key, f.Value)
	}
	return fl.HighWatermark(), nil
}

// outgoingBearer attaches the peer-issued federation key to outgoing RPC
// metadata. The peer verifies it against its own federation-scoped keys.
func outgoingBearer(ctx context.Context, key string) context.Context {
	if key == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
}

// cursorValue looks up a persisted cursor, zero when none exists.
func (p *Puller) cursorValue(peer, topic string, partition int32) (Cursor, bool) {
	for _, c := range p.cfg.Store.ListCursors() {
		if c.Peer == peer && c.Topic == topic && c.Partition == partition {
			return c, true
		}
	}
	return Cursor{}, false
}

var _ = binary.BigEndian // reserved: queue-attempt value decode lands in slice 5

// observeLag records whether (peer, topic, partition) is caught up and
// reports time since it last was. On a fetch failure the timestamp is left
// alone, so consecutive failures grow the reported lag.
func (p *Puller) observeLag(peer, topic string, partition int32, caughtUp bool) {
	if p.cfg.LagSeconds == nil {
		return
	}
	key := fmt.Sprintf("%s|%s|%d", peer, topic, partition)
	p.lagMu.Lock()
	since, ok := p.lag[key]
	if !ok {
		since = p.started
		p.lag[key] = since
	}
	if caughtUp {
		since = time.Now()
		p.lag[key] = since
	}
	lag := time.Since(since).Seconds()
	p.lagMu.Unlock()
	p.cfg.LagSeconds(peer, topic, partition, lag)
}

// remoteRanges returns (cached, TTL 10s) the peer's published foreign ranges
// for (topic, partition) via FederationReport. Used to re-key attempt keys
// from the source's offset space into ours when this node never imported
// the records from that peer (it authored them locally).
func (p *Puller) remoteRanges(ctx context.Context, peer Peer, topic string, partition int32) []LocalRange {
	key := fmt.Sprintf("%s|%s|%d", peer.Name, topic, partition)
	p.rrMu.Lock()
	if rs, ok := p.rr[key]; ok && time.Since(p.rrFetched[key]) < 10*time.Second {
		p.rrMu.Unlock()
		return rs
	}
	p.rrMu.Unlock()

	conn, err := p.cfg.Dial(ctx, peer)
	if err != nil {
		p.logger.Warn("federation: dial peer failed", "peer", peer.Name, "err", err)
		return nil
	}
	defer func() { _ = conn.Close() }()
	ctx2, cancel := context.WithTimeout(outgoingBearer(ctx, peer.Key), 30*time.Second)
	defer cancel()
	rep, err := firnpb.NewFederationServiceClient(conn).FederationReport(ctx2, &firnpb.FederationReportRequest{
		Topic:     topic,
		Partition: partition,
	})
	if err != nil {
		p.logger.Warn("federation: report for ranges failed", "peer", peer.Name, "topic", topic, "partition", partition, "err", err)
		return nil
	}
	out := []LocalRange{}
	for _, r := range rep.GetForeignRanges() {
		out = append(out, LocalRange{Start: r.Start, End: r.End, SourceStart: r.SourceStart})
	}
	p.rrMu.Lock()
	p.rr[key] = out
	p.rrFetched[key] = time.Now()
	p.rrMu.Unlock()
	return out
}
