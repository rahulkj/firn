// Package federation implements ADR-010: cross-cluster event replication.
// This file defines the control plane (peer registration + cursors) and the
// data-plane types used by slices 2+. Slice 1 adds registration only — no
// data motion.
package federation

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rahulkj/firn/internal/log"
)

type partitionLog interface {
	Append(key, value []byte) int64
	Fetch(offset int64, max int) ([]log.Record, error)
	HighWatermark() int64
}

// Direction describes which side(s) of a peer link produce.
type Direction uint8

const (
	DirectionIn   Direction = 1 // receive from peer; do not push
	DirectionOut  Direction = 2 // push to peer; do not receive
	DirectionBoth Direction = 3 // bidirectional
)

func (d Direction) String() string {
	switch d {
	case DirectionIn:
		return "in"
	case DirectionOut:
		return "out"
	case DirectionBoth:
		return "both"
	default:
		return "unknown"
	}
}

func ParseDirection(s string) (Direction, error) {
	switch strings.ToLower(s) {
	case "in":
		return DirectionIn, nil
	case "out":
		return DirectionOut, nil
	case "both":
		return DirectionBoth, nil
	default:
		return 0, fmt.Errorf("federation: unknown direction %q (want: in|out|both)", s)
	}
}

// Mode picks the replication strategy.
type Mode uint8

const (
	ModePassive      Mode = 1 // byte-identical import; HWM preserved; no writes accepted locally until cutover
	ModeActiveActive Mode = 2 // frame re-origin; offsets independent per cluster; concurrent writes accepted
)

func (m Mode) String() string {
	switch m {
	case ModePassive:
		return "passive"
	case ModeActiveActive:
		return "active-active"
	default:
		return "unknown"
	}
}

func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(s) {
	case "passive", "1-way":
		return ModePassive, nil
	case "active-active", "aa", "full", "both-way":
		return ModeActiveActive, nil
	default:
		return 0, fmt.Errorf("federation: unknown mode %q (want: passive|active-active)", s)
	}
}

// Selector is a list of topics and queues the peer link covers. Empty lists
// mean "no restriction for that category". Both empty means "all records
// everywhere".
type Selector struct {
	Topics []string `json:"topics"`
	Queues []string `json:"queues"`
}

// Peer is one registered peer cluster. Name is unique within this cluster.
// CACrtPEM is the peer's mesh CA certificate; outbound TLS dial verifies the
// peer against it as the root pool. Key is the bearer token this cluster
// presents to the peer's FederationService — issued by the peer out-of-band
// (ADR-010). Stored only in the cluster-local replicated internal log; never
// returned by the admin API (key material stays write-only, like API keys).
// ServerName overrides the TLS server name used when verifying the peer
// (defaults to the host part of Addr); required when the peer cert's DNS SANs
// don't match the dial address.
type Peer struct {
	Name        string     `json:"name"`
	Addr        string     `json:"addr"`
	CACrtPEM    []byte     `json:"ca_crt_pem,omitempty"`
	ServerName  string     `json:"server_name,omitempty"`
	Key         string     `json:"key,omitempty"`
	Direction   Direction  `json:"direction"`
	Mode        Mode       `json:"mode"`
	Selectors   []Selector `json:"selectors"`
	CreatedUnix int64      `json:"created_unix"`
	UpdatedUnix int64      `json:"updated_unix"`
}

// LocalRange is a contiguous interval of LOCAL offsets [Start, End] that
// this cluster imported from a peer (the foreign-applied ranges of ADR-010).
// The serving filter uses one peer's ranges to omit exactly the frames that
// peer donated to us, closing the A→B→A echo without frame headers.
type LocalRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// mergeLocalRanges sorts and coalesces overlapping or adjacent ranges.
func mergeLocalRanges(rs []LocalRange) []LocalRange {
	if len(rs) == 0 {
		return nil
	}
	out := append([]LocalRange(nil), rs...)
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	merged := []LocalRange{out[0]}
	for _, r := range out[1:] {
		last := &merged[len(merged)-1]
		if r.Start <= last.End+1 {
			if r.End > last.End {
				last.End = r.End
			}
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

// RangeContains reports whether offset falls inside any range.
func RangeContains(rs []LocalRange, offset int64) bool {
	for _, r := range rs {
		if offset >= r.Start && offset <= r.End {
			return true
		}
	}
	return false
}

// ForeignRanges returns the merged foreign-applied ranges recorded for
// (peer, topic, partition) on this node: the local ranges WE imported from
// that peer. Used by the serving-side echo filter.
func (s *Store) ForeignRanges(peer, topic string, partition int32) []LocalRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.loadLocked()
	c, ok := s.curCache[fmt.Sprintf("%s/%s/%d", peer, topic, partition)]
	if !ok {
		return nil
	}
	return mergeLocalRanges(c.ForeignRanges)
}

// Cursor tracks per-(peer, topic, partition) import progress.
// Written by the puller; read by every node for `/federation/cursors`.
type Cursor struct {
	Peer             string       `json:"peer"`
	Topic            string       `json:"topic"`
	Partition        int32        `json:"partition"`
	LastSourceOffset int64        `json:"last_source_offset"`
	LastLocalOffset  int64        `json:"last_local_offset"`
	Mode             Mode         `json:"mode"`
	UpdatedUnix      int64        `json:"updated_unix"`
	Origin           string       `json:"origin,omitempty"`         // "remote" for federation-applied imports
	ForeignRanges    []LocalRange `json:"foreign_ranges,omitempty"` // local ranges applied from the peer (echo filter)
}

// internalPrefix is the reserved namespace for firn-internal topics. No
// selector may reference it: internal topics are cluster-local by design.
// The single exception is __queue_attempts, which the puller mirrors
// byte-identically on its own when queues are in scope — never via a selector.
const internalPrefix = "__"

var policyPrefixes = []string{"__federation_", "__api_keys", "__audit", "__consumer_offsets", "__queue_attempts"}

// ValidateShape reports structural errors in a Peer (without checking the
// network or the peer's cert chain). Used by the admin HTTP endpoint.
func ValidateShape(p Peer) error {
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return errors.New("federation: peer name must not be empty")
	}
	for _, prefix := range policyPrefixes {
		if strings.HasPrefix(name, prefix) {
			return fmt.Errorf("federation: peer name %q must not shadow the internal topic namespace", name)
		}
	}
	if p.Addr == "" {
		return errors.New("federation: peer addr must not be empty")
	}
	switch p.Direction {
	case DirectionIn, DirectionOut, DirectionBoth:
	default:
		return fmt.Errorf("federation: direction %d out of range", p.Direction)
	}
	switch p.Mode {
	case ModePassive, ModeActiveActive:
	default:
		return fmt.Errorf("federation: mode %d out of range", p.Mode)
	}
	switch p.Direction {
	case DirectionIn, DirectionBoth:
		if strings.TrimSpace(p.Key) == "" {
			return errors.New("federation: direction in|both requires the peer-issued bearer key (--key)")
		}
	}
	for _, sel := range p.Selectors {
		for _, t := range sel.Topics {
			if t == "" {
				return errors.New("federation: topic in selector must not be empty")
			}
			if strings.HasPrefix(t, internalPrefix) {
				return fmt.Errorf("federation: internal topic %q cannot be federated", t)
			}
		}
		for _, q := range sel.Queues {
			if q == "" {
				return errors.New("federation: queue in selector must not be empty")
			}
			if strings.HasPrefix(q, internalPrefix) {
				return fmt.Errorf("federation: internal name %q cannot be federated", q)
			}
		}
	}
	return nil
}

const cursorFile = "__federation_cursors-0"
const peerFile = "__federation_peers-0"

// Store persists peers + cursors in two internal logs shared via the broker
// (ADR-010). All reads are fold-once + cache; the cache is owned by this
// process and the internal topics are the replication source of truth.
type Store struct {
	peers   partitionLog
	cursors partitionLog
	mu      sync.Mutex

	peersHWM int64
	curHWM   int64

	peerCache map[string]Peer
	curCache  map[string]Cursor

	cacheFresh bool
	cacheAt    time.Time
}

// OpenFromDir opens the two internal federation logs rooted at dir.
func OpenFromDir(dir string) (*Store, error) {
	var peers, cursors *log.FileLog
	var err error
	peers, err = log.Open(filepath.Join(dir, peerFile), log.WithBlockSealing(250*time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("federation: open peers log: %w", err)
	}
	cursors, err = log.Open(filepath.Join(dir, cursorFile), log.WithBlockSealing(250*time.Millisecond))
	if err != nil {
		_ = peers.Close()
		return nil, fmt.Errorf("federation: open cursors log: %w", err)
	}
	return &Store{
		peers:     peers,
		cursors:   cursors,
		peerCache: map[string]Peer{},
		curCache:  map[string]Cursor{},
	}, nil
}

// OpenInMemory returns a Store backed by MemLogs (tests, in-memory mode).
func OpenInMemory() *Store {
	return &Store{
		peers:     &log.MemLog{},
		cursors:   &log.MemLog{},
		peerCache: map[string]Peer{},
		curCache:  map[string]Cursor{},
	}
}

// SetPeersLog allows wiring shared handles from the broker.
func (s *Store) SetPeersLog(l partitionLog)   { s.peers = l }
func (s *Store) SetCursorsLog(l partitionLog) { s.cursors = l }

// AddPeer persists a peer. Overwrites an existing peer by name.
func (s *Store) AddPeer(p Peer) error {
	if err := ValidateShape(p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	if old, ok := s.peerCache[p.Name]; ok {
		p.CreatedUnix = old.CreatedUnix
	} else {
		p.CreatedUnix = now
	}
	p.UpdatedUnix = now
	data, _ := json.Marshal(p)
	s.peers.Append([]byte("peer:"+p.Name), data)
	s.peersHWM = s.peers.HighWatermark()
	s.peerCache[p.Name] = p
	return nil
}

// RemovePeer deletes a peer by name. No-op if absent.
func (s *Store) RemovePeer(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers.Append([]byte("peer:"+name), []byte(fmt.Sprintf(`{"op":"delete","name":%q}`, name)))
	s.peersHWM = s.peers.HighWatermark()
	delete(s.peerCache, name)
	return nil
}

// ListPeers returns the current registered peers in deterministic order.
func (s *Store) ListPeers() []Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.loadLocked()
	out := make([]Peer, 0, len(s.peerCache))
	for _, p := range s.peerCache {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ListPeersCached returns the peer list, refolding the internal logs at most
// once per maxAge. Used by the produce/queue hot path: internal logs are
// tiny but the append path must not re-fold on every write.
func (s *Store) ListPeersCached(maxAge time.Duration) []Peer {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cacheFresh && now.Sub(s.cacheAt) < maxAge {
		return sortedPeers(s.peerCache)
	}
	if err := s.loadLocked(); err != nil {
		// Serve the stale cache on read errors rather than blocking producers.
		return sortedPeers(s.peerCache)
	}
	s.cacheFresh = true
	s.cacheAt = now
	return sortedPeers(s.peerCache)
}

// ListCursorsCached mirrors ListPeersCached for the cursors log.
func (s *Store) ListCursorsCached(maxAge time.Duration) []Cursor {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cacheFresh && now.Sub(s.cacheAt) < maxAge {
		return sortedCursors(s.curCache)
	}
	if err := s.loadLocked(); err != nil {
		return sortedCursors(s.curCache)
	}
	s.cacheFresh = true
	s.cacheAt = now
	return sortedCursors(s.curCache)
}

func sortedCursors(cache map[string]Cursor) []Cursor {
	out := make([]Cursor, 0, len(cache))
	for _, c := range cache {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Peer != out[j].Peer {
			return out[i].Peer < out[j].Peer
		}
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

func sortedPeers(cache map[string]Peer) []Peer {
	out := make([]Peer, 0, len(cache))
	for _, p := range cache {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// GetPeer returns one peer by name.
func (s *Store) GetPeer(name string) (Peer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.loadLocked()
	p, ok := s.peerCache[name]
	return p, ok
}

// SetCursor persists an import progress marker.
func (s *Store) SetCursor(c Cursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	c.UpdatedUnix = now
	c.ForeignRanges = mergeLocalRanges(c.ForeignRanges)
	data, _ := json.Marshal(c)
	s.cursors.Append([]byte(fmt.Sprintf("cursor:%s/%s/%d", c.Peer, c.Topic, c.Partition)), data)
	s.curHWM = s.cursors.HighWatermark()
	s.curCache[fmt.Sprintf("%s/%s/%d", c.Peer, c.Topic, c.Partition)] = c
	return nil
}

// ListCursors returns all persisted cursors in deterministic order.
func (s *Store) ListCursors() []Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.loadLocked()
	out := make([]Cursor, 0, len(s.curCache))
	for _, c := range s.curCache {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Peer != out[j].Peer {
			return out[i].Peer < out[j].Peer
		}
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// loadLocked folds the two internal logs into in-memory caches. Idempotent.
func (s *Store) loadLocked() error {
	s.peerCache = map[string]Peer{}
	if hw := s.peers.HighWatermark(); hw > 0 {
		recs, err := s.peers.Fetch(0, int(hw))
		if err != nil {
			return err
		}
		for _, r := range recs {
			applyPeer(s.peerCache, r.Value)
		}
	}
	s.peersHWM = s.peers.HighWatermark()

	s.curCache = map[string]Cursor{}
	if hwc := s.cursors.HighWatermark(); hwc > 0 {
		recs, err := s.cursors.Fetch(0, int(hwc))
		if err != nil {
			return err
		}
		for _, r := range recs {
			applyCursor(s.curCache, r.Value)
		}
	}
	s.curHWM = s.cursors.HighWatermark()
	s.cacheFresh = true
	s.cacheAt = time.Now()
	return nil
}

func applyPeer(cache map[string]Peer, payload []byte) {
	var probe struct {
		Name string `json:"name"`
		Op   string `json:"op"`
	}
	if err := json.Unmarshal(payload, &probe); err == nil && probe.Op == "delete" && probe.Name != "" {
		delete(cache, probe.Name)
		return
	}
	var p Peer
	if err := json.Unmarshal(payload, &p); err == nil && p.Name != "" {
		cache[p.Name] = p
	}
}

func applyCursor(cache map[string]Cursor, payload []byte) {
	var probe struct {
		Op        string `json:"op"`
		Peer      string `json:"peer"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
	}
	if err := json.Unmarshal(payload, &probe); err == nil && probe.Op == "delete" && probe.Peer != "" {
		key := fmt.Sprintf("%s/%s/%d", probe.Peer, probe.Topic, probe.Partition)
		delete(cache, key)
		return
	}
	var c Cursor
	if err := json.Unmarshal(payload, &c); err == nil && c.Peer != "" {
		cache[fmt.Sprintf("%s/%s/%d", c.Peer, c.Topic, c.Partition)] = c
	}
}

// RemoveWholePeer is the reseed path: RemovePeer + delete every cursor
// belonging to that peer. Retention of orphaned cursors is the difference
// between "restart clean" and "double-import after reseed".
func (s *Store) RemoveWholePeer(name string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers.Append([]byte("peer:"+name), []byte(fmt.Sprintf(`{"op":"delete","name":%q}`, name)))
	s.peersHWM = s.peers.HighWatermark()
	delete(s.peerCache, name)

	removed := []string{}
	for k, c := range s.curCache {
		if c.Peer != name {
			continue
		}
		key := fmt.Sprintf("cursor:%s/%s/%d", c.Peer, c.Topic, c.Partition)
		s.cursors.Append([]byte(key), []byte(`{"op":"delete","peer":"`+c.Peer+`","topic":"`+c.Topic+`","partition":`+fmtPar(c.Partition)+`}`))
		removed = append(removed, key)
		delete(s.curCache, k)
	}
	return removed
}

func fmtPar(p int32) string {
	return fmt.Sprintf("%d", p)
}
