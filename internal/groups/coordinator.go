// Package groups implements consumer groups on top of the append-only log:
// a compacted __consumer_offsets internal topic (offset_store.go) and a
// group coordinator-lite (this file) with join/sync, range + roundrobin
// assignment, and session expiry via heartbeat.
package groups

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rahulkj/firn/internal/meta"
)

// Errors surfaced to the API layer.
var (
	ErrInvalidRequest   = errors.New("firn/groups: invalid request")
	ErrMemberNotFound   = errors.New("firn/groups: member not found")
	ErrGroupNotFound    = errors.New("firn/groups: group not found")
	ErrStaleGeneration  = errors.New("firn/groups: stale generation (rebalance)")
	ErrOffsetRegression = errors.New("firn/groups: committed offset regression")
	ErrOffsetNotFound   = errors.New("firn/groups: offset not committed")
)

// DefaultSessionTimeout is the member lease when none is requested.
const DefaultSessionTimeout = 10 * time.Second

// CommitMarker is returned on commit so callers can log the outcome.
type CommittedOffsetEntry struct {
	Group      string
	Topic      string
	Partition  int32
	Offset     int64
	Generation int64
	Leader     string
}

// MemberInfo describes one group member for admin/diagnostics.
type MemberInfo struct {
	ID            string
	Topics        []string
	Assignment    map[string][]int32
	LastHeartbeat time.Time
}

// GroupInfo describes one group for admin/diagnostics.
type GroupInfo struct {
	Name       string
	Generation int64
	Leader     string
	Protocol   string
	Members    []MemberInfo
}

// JoinResult is what a member needs to start consuming.
type JoinResult struct {
	MemberID   string
	Generation int64
	Leader     bool
	Assignment map[string][]int32
	Members    []string
	Protocol   string
}

// Option customizes a Coordinator.
type Option func(*Coordinator)

// WithSessionTimeout overrides the default member lease.
func WithSessionTimeout(d time.Duration) Option {
	return func(c *Coordinator) { c.sessionTimeout = d }
}

// withClock overrides the time source (tests).
func withClock(now func() time.Time) Option {
	return func(c *Coordinator) { c.now = now }
}

type member struct {
	id            string
	joinSeq       uint64
	topics        []string
	lastHeartbeat time.Time
	timeout       time.Duration
	assignment    map[string][]int32
}

type groupState struct {
	name       string
	protocol   string
	generation int64
	leader     string
	joinOrder  []string
	members    map[string]*member
}

// Coordinator owns group membership, assignment, and the offsets store.
// It is safe for concurrent use.
type Coordinator struct {
	reg            *meta.Registry
	store          *OffsetStore
	sessionTimeout time.Duration
	now            func() time.Time

	mu         sync.Mutex
	groups     map[string]*groupState
	memberSeq  uint64
	rebalances atomic.Uint64
}

// NewCoordinator wires the registry to the offsets store.
func NewCoordinator(reg *meta.Registry, store *OffsetStore, opts ...Option) (*Coordinator, error) {
	if reg == nil || store == nil {
		return nil, fmt.Errorf("firn/groups: registry and store are required")
	}
	c := &Coordinator{reg: reg, store: store, sessionTimeout: DefaultSessionTimeout, now: time.Now, groups: map[string]*groupState{}}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Close flushes/compacts and closes the offsets store.
func (c *Coordinator) Close() error {
	return c.store.Close()
}

// Store returns the underlying offsets store. The broker uses this to bind
// the internal-topic write fence to the store after construction.
func (c *Coordinator) Store() (*OffsetStore, bool) {
	return c.store, c.store != nil
}

func validateGroupAndTopics(group string, topics []string, protocol string) error {
	if group == "" {
		return fmt.Errorf("%w: empty group id", ErrInvalidRequest)
	}
	if strings.ContainsRune(group, 0) {
		return fmt.Errorf("%w: NUL in group id", ErrInvalidRequest)
	}
	if len(topics) == 0 {
		return fmt.Errorf("%w: at least one topic must be subscribed", ErrInvalidRequest)
	}
	for _, t := range topics {
		if t == "" || strings.ContainsRune(t, 0) {
			return fmt.Errorf("%w: invalid topic %q", ErrInvalidRequest, t)
		}
	}
	switch protocol {
	case "", "range", "roundrobin":
	default:
		return fmt.Errorf("%w: unknown protocol %q (want range|roundrobin)", ErrInvalidRequest, protocol)
	}
	return nil
}

// JoinGroup adds (or re-adds) a member, rebalances on membership change, and
// returns the member's assignment. An empty memberID creates a new member.
func (c *Coordinator) JoinGroup(group, memberID string, topics []string, protocol string, sessionTimeout time.Duration) (JoinResult, error) {
	if err := validateGroupAndTopics(group, topics, protocol); err != nil {
		return JoinResult{}, err
	}
	// Validate topics exist.
	for _, t := range topics {
		if _, err := c.reg.Get(t); err != nil {
			return JoinResult{}, fmt.Errorf("%w: %s", ErrUnknownTopic, t)
		}
	}
	if protocol == "" {
		protocol = "range"
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(c.now())

	g := c.groups[group]
	if g == nil {
		g = &groupState{name: group, protocol: protocol, members: map[string]*member{}}
		c.groups[group] = g
	} else if protocol != "" && g.protocol != protocol && len(g.members) > 0 {
		// Protocol is fixed at group creation while members are present.
		return JoinResult{}, fmt.Errorf("%w: group already uses protocol %q", ErrInvalidRequest, g.protocol)
	}
	if g.protocol == "" {
		g.protocol = protocol
	}

	now := c.now()
	memberKey := memberID
	if memberKey == "" {
		c.memberSeq++
		memberKey = fmt.Sprintf("member-%d", c.memberSeq)
	}
	m := g.members[memberKey]
	if m == nil {
		c.memberSeq++
		m = &member{id: memberKey, joinSeq: c.memberSeq}
		g.members[memberKey] = m
		g.joinOrder = append(g.joinOrder, memberKey)
	}
	m.topics = append([]string(nil), topics...)
	m.lastHeartbeat = now
	m.timeout = sessionTimeout
	if m.timeout <= 0 {
		m.timeout = c.sessionTimeout
	}

	// Rebalance if membership changed.
	g.generation++
	c.assignLocked(g)

	return JoinResult{
		MemberID:   m.id,
		Generation: g.generation,
		Leader:     g.leader == m.id,
		Assignment: copyAssignment(m.assignment),
		Members:    append([]string(nil), g.joinOrder...),
		Protocol:   g.protocol,
	}, nil
}

// SyncGroup returns the member's current assignment.
func (c *Coordinator) SyncGroup(group, memberID string, generation int64) (map[string][]int32, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(c.now())
	g := c.groups[group]
	if g == nil {
		return nil, 0, fmt.Errorf("%w: %s", ErrGroupNotFound, group)
	}
	m := g.members[memberID]
	if m == nil {
		return nil, 0, fmt.Errorf("%w: %s in group %s", ErrMemberNotFound, memberID, group)
	}
	if generation != 0 && generation < g.generation {
		return nil, g.generation, fmt.Errorf("%w: got %d, current %d", ErrStaleGeneration, generation, g.generation)
	}
	return copyAssignment(m.assignment), g.generation, nil
}

// Heartbeat refreshes the member lease; it errors on stale generations.
func (c *Coordinator) Heartbeat(group, memberID string, generation int64) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(c.now())
	g := c.groups[group]
	if g == nil {
		return 0, false, fmt.Errorf("%w: %s", ErrGroupNotFound, group)
	}
	m := g.members[memberID]
	if m == nil {
		return 0, false, fmt.Errorf("%w: %s in group %s", ErrMemberNotFound, memberID, group)
	}
	if generation != 0 && generation < g.generation {
		return g.generation, true, fmt.Errorf("%w: got %d, current %d", ErrStaleGeneration, generation, g.generation)
	}
	m.lastHeartbeat = c.now()
	return g.generation, false, nil
}

// LeaveGroup removes a member and rebalances.
func (c *Coordinator) LeaveGroup(group, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(c.now())
	g := c.groups[group]
	if g == nil {
		return fmt.Errorf("%w: %s", ErrGroupNotFound, group)
	}
	if _, ok := g.members[memberID]; !ok {
		return fmt.Errorf("%w: %s in group %s", ErrMemberNotFound, memberID, group)
	}
	delete(g.members, memberID)
	for i, id := range g.joinOrder {
		if id == memberID {
			g.joinOrder = append(g.joinOrder[:i], g.joinOrder[i+1:]...)
			break
		}
	}
	g.generation++
	c.assignLocked(g)
	return nil
}

// CommitOffset stores a committed offset. The committed group offsets
// advance monotonically unless allowReset is set (invariant 4).
func (c *Coordinator) CommitOffset(group, topic string, partition int32, offset, generation int64, allowReset bool) error {
	if group == "" || strings.ContainsRune(group, 0) {
		return fmt.Errorf("%w: invalid group id", ErrInvalidRequest)
	}
	if offset < 0 {
		return fmt.Errorf("%w: negative offset", ErrInvalidRequest)
	}
	t, err := c.reg.Get(topic)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrUnknownTopic, topic)
	}
	if partition < 0 || int(partition) >= t.Partitions {
		return fmt.Errorf("%w: partition %d out of range", ErrInvalidRequest, partition)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if generation != 0 && g != nil && generation < g.generation {
		return fmt.Errorf("%w: got %d, current %d", ErrStaleGeneration, generation, g.generation)
	}
	leader := ""
	if g != nil {
		leader = g.leader
	}
	key := CommitKey(group, topic, partition)
	cur, found := c.store.Get(key)
	if found && offset < cur.Offset && !allowReset {
		return fmt.Errorf("%w: %d < committed %d", ErrOffsetRegression, offset, cur.Offset)
	}
	return c.store.Commit(key, CommittedOffset{Offset: offset, Generation: generationOr(g, generation), Leader: leader})
}

// FetchOffset returns the folded committed offset for (group, topic, partition).
func (c *Coordinator) FetchOffset(group, topic string, partition int32) (CommittedOffset, bool, error) {
	if _, err := c.reg.Get(topic); err != nil {
		return CommittedOffset{}, false, fmt.Errorf("%w: %s", ErrUnknownTopic, topic)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(c.now())
	v, ok := c.store.Get(CommitKey(group, topic, partition))
	return v, ok, nil
}

// FetchGroupOffsets lists the folded committed offsets for a group.
func (c *Coordinator) FetchGroupOffsets(group string) ([]CommittedOffsetEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(c.now())
	out := []CommittedOffsetEntry{}
	for k, v := range c.store.All() {
		parts := strings.Split(k, "\x00")
		if len(parts) != 3 || parts[0] != group {
			continue
		}
		var p int32
		if _, err := fmt.Sscanf(parts[2], "%d", &p); err != nil {
			return nil, fmt.Errorf("firn/groups: corrupt offsets store key %q", k)
		}
		out = append(out, CommittedOffsetEntry{Group: group, Topic: parts[1], Partition: p, Offset: v.Offset, Generation: v.Generation, Leader: v.Leader})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}

// ExpireSessions removes members whose lease expired and rebalances the
// affected groups. Returns the number of members expired.
func (c *Coordinator) ExpireSessions() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.expireLocked(c.now())
}

// Groups lists group state for admin/diagnostics.
func (c *Coordinator) Groups() []GroupInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(c.now())
	out := []GroupInfo{}
	for _, g := range c.groups {
		gi := GroupInfo{Name: g.name, Generation: g.generation, Leader: g.leader, Protocol: g.protocol}
		for _, id := range g.joinOrder {
			m := g.members[id]
			if m == nil {
				continue
			}
			gi.Members = append(gi.Members, MemberInfo{ID: m.id, Topics: append([]string(nil), m.topics...), Assignment: copyAssignment(m.assignment), LastHeartbeat: m.lastHeartbeat})
		}
		out = append(out, gi)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Rebalances returns the number of group rebalances since process start.
func (c *Coordinator) Rebalances() uint64 { return c.rebalances.Load() }

// assignLocked recomputes assignments in join order.
func (c *Coordinator) assignLocked(g *groupState) {
	c.rebalances.Add(1)
	// Gather the topics subscribed by surviving members.
	topicSet := map[string]bool{}
	for _, id := range g.joinOrder {
		for _, t := range g.members[id].topics {
			topicSet[t] = true
		}
	}
	// Reset assignments.
	for _, m := range g.members {
		m.assignment = map[string][]int32{}
	}
	for topic := range topicSet {
		t, err := c.reg.Get(topic)
		if err != nil {
			continue // topic deleted under us; skip
		}
		var subs []*member
		for _, id := range g.joinOrder {
			if containsString(g.members[id].topics, topic) {
				subs = append(subs, g.members[id])
			}
		}
		if len(subs) == 0 {
			continue
		}
		switch g.protocol {
		case "roundrobin":
			for p := int32(0); p < int32(t.Partitions); p++ {
				m := subs[int(p)%len(subs)]
				m.assignment[topic] = append(m.assignment[topic], p)
			}
		default: // range
			base := t.Partitions / len(subs)
			rem := t.Partitions % len(subs)
			idx := int32(0)
			for i, m := range subs {
				n := int32(base)
				if i < rem {
					n++
				}
				part := make([]int32, 0, n)
				for j := int32(0); j < n; j++ {
					part = append(part, idx)
					idx++
				}
				m.assignment[topic] = append(m.assignment[topic], part...)
			}
		}
	}
	if len(g.joinOrder) > 0 {
		g.leader = g.joinOrder[0]
	} else {
		g.leader = ""
	}
	// Drop empty groups.
	if len(g.members) == 0 && len(g.joinOrder) == 0 {
		delete(c.groups, g.name)
	}
}

// expireLocked removes members whose lease lapsed and rebalances.
func (c *Coordinator) expireLocked(now time.Time) int {
	expired := 0
	for _, g := range c.groups {
		changed := false
		for id, m := range g.members {
			cutoff := m.timeout
			if cutoff <= 0 {
				cutoff = c.sessionTimeout
			}
			if now.Sub(m.lastHeartbeat) > cutoff {
				delete(g.members, id)
				for i, jid := range g.joinOrder {
					if jid == id {
						g.joinOrder = append(g.joinOrder[:i], g.joinOrder[i+1:]...)
						break
					}
				}
				changed = true
				expired++
			}
		}
		if changed {
			g.generation++
			c.assignLocked(g)
		}
	}
	return expired
}

func generationOr(g *groupState, generation int64) int64 {
	if generation != 0 {
		return generation
	}
	if g == nil {
		return 0
	}
	return g.generation
}

func copyAssignment(in map[string][]int32) map[string][]int32 {
	out := make(map[string][]int32, len(in))
	for k, v := range in {
		out[k] = append([]int32(nil), v...)
	}
	return out
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// ErrUnknownTopic is wrapped by NotFound-mapped errors at the API layer.
var ErrUnknownTopic = errors.New("firn/groups: topic not found")
