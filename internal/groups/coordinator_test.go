package groups

import (
	"testing"
	"time"

	"github.com/rahulkj/slipway/internal/meta"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time          { return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.t = f.t.Add(d) }

func newTestCoordinatorOpts(t *testing.T, topics map[string]int, opts ...Option) (*Coordinator, *meta.Registry) {
	t.Helper()
	reg := meta.NewRegistry()
	for name, partitions := range topics {
		if err := reg.Create(name, partitions); err != nil {
			t.Fatalf("create topic %q: %v", name, err)
		}
	}
	store, err := NewOffsetStore("", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c, err := NewCoordinator(reg, store, opts...)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	return c, reg
}

func newTestCoordinator(t *testing.T, topics map[string]int, sessionTimeout time.Duration) (*Coordinator, *meta.Registry) {
	t.Helper()
	return newTestCoordinatorOpts(t, topics, WithSessionTimeout(sessionTimeout))
}

func TestRangeAssignmentSplitsPartitions(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 3}, 10*time.Second)

	m1, err := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	if err != nil {
		t.Fatalf("m1 join: %v", err)
	}
	m2, err := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	if err != nil {
		t.Fatalf("m2 join: %v", err)
	}
	// After m2 joins, assignments must cover all 3 partitions, disjoint.
	total := append(append([]int32{}, m2.Assignment["events"]...), 0)
	_ = total
	got1 := m2.Assignment["events"]
	// m1's assignment must come from SyncGroup.
	s1, _, err := c.SyncGroup("g1", m1.MemberID, 0)
	if err != nil {
		t.Fatalf("m1 sync: %v", err)
	}
	s2, _, err := c.SyncGroup("g1", m2.MemberID, 0)
	if err != nil {
		t.Fatalf("m2 sync: %v", err)
	}
	a1, a2 := s1["events"], s2["events"]
	if len(a1)+len(a2) != 3 {
		t.Fatalf("assignments %v and %v do not cover 3 partitions", a1, a2)
	}
	seen := map[int32]bool{}
	for _, p := range append(append([]int32{}, a1...), a2...) {
		if seen[p] {
			t.Fatalf("partition %d assigned twice", p)
		}
		seen[p] = true
	}
	// 3 partitions across 2 members: 2/1 split in join order (m1 gets 2).
	if len(a1) != 2 || len(a2) != 1 {
		t.Fatalf("assignments %v / %v, want 2/1 split", a1, a2)
	}
	if got1 == nil {
		t.Fatal("m2 join response missing assignment")
	}
}

func TestRoundRobinAssignmentSpreadsEvenly(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 5}, 10*time.Second)
	m1, _ := c.JoinGroup("g1", "", []string{"events"}, "roundrobin", 0)
	m2, _ := c.JoinGroup("g1", "", []string{"events"}, "roundrobin", 0)
	s1, _, _ := c.SyncGroup("g1", m1.MemberID, 0)
	s2, _, _ := c.SyncGroup("g1", m2.MemberID, 0)
	if len(s1["events"]) != 3 || len(s2["events"]) != 2 {
		t.Fatalf("roundrobin split = %v / %v, want 3/2", s1["events"], s2["events"])
	}
}

func TestMemberCrashExpiresAndSurvivorTakesAll(t *testing.T) {
	fc := &fakeClock{t: time.Now()}
	c, _ := newTestCoordinatorOpts(t, map[string]int{"events": 3},
		WithSessionTimeout(50*time.Millisecond), withClock(fc.Now))

	// m1 joins at T0; m2 joins at T0+30ms (both alive, 30 < 50ms lease).
	_, m1err := c.JoinGroup("g1", "m1", []string{"events"}, "range", 0)
	if m1err != nil {
		t.Fatalf("m1 join: %v", m1err)
	}
	fc.Advance(30 * time.Millisecond)
	m2, err := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	if err != nil {
		t.Fatalf("m2 join: %v", err)
	}
	gen2 := m2.Generation

	// T0+79ms: m1's lease (lastHB=T0) lapsed; m2 refreshes its lease.
	fc.Advance(49 * time.Millisecond)
	if n := c.ExpireSessions(); n != 1 {
		t.Fatalf("expired %d members, want 1 (m1 only)", n)
	}
	_, gen3, err := c.SyncGroup("g1", m2.MemberID, 0)
	if err != nil {
		t.Fatalf("sync after expiry: %v", err)
	}
	if _, _, err := c.Heartbeat("g1", m2.MemberID, gen3); err != nil {
		t.Fatalf("m2 heartbeat: %v", err)
	}
	s2, gen, err := c.SyncGroup("g1", m2.MemberID, 0)
	if err != nil {
		t.Fatalf("sync after expiry: %v", err)
	}
	if len(s2["events"]) != 3 {
		t.Fatalf("survivor assignment = %v, want all 3 partitions", s2["events"])
	}
	if gen <= gen2 {
		t.Fatalf("generation = %d after expiry, want > %d", gen, gen2)
	}
}

func TestHeartbeatHonorsGeneration(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 1}, 10*time.Second)
	m1, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	m2, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)

	// m1's generation is stale after m2 joined and caused a rebalance.
	if _, _, err := c.Heartbeat("g1", m1.MemberID, m1.Generation); err == nil {
		t.Fatal("stale heartbeat must be rejected")
	}
	// m2's current generation must succeed.
	gen, rebalance, err := c.Heartbeat("g1", m2.MemberID, m2.Generation)
	if err != nil {
		t.Fatalf("heartbeat current generation: %v", err)
	}
	if rebalance {
		t.Fatal("current generation flagged for rebalance")
	}
	if gen != m2.Generation {
		t.Fatalf("generation = %d, want %d", gen, m2.Generation)
	}
}

func TestCommitFetchRoundTripAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	reg := meta.NewRegistry()
	_ = reg.Create("events", 2)

	// First "process": join, commit.
	store1, err := NewOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	c1, err := NewCoordinator(reg, store1)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	m1, err := c1.JoinGroup("g1", "", []string{"events"}, "range", 0)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := c1.CommitOffset("g1", "events", 0, 42, m1.Generation, false); err != nil {
		t.Fatalf("commit p0: %v", err)
	}
	if err := c1.CommitOffset("g1", "events", 1, 100, m1.Generation, false); err != nil {
		t.Fatalf("commit p1: %v", err)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Second "process": fetch resumes exactly at committed offsets.
	store2, err := NewOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("store reopen: %v", err)
	}
	defer func() { _ = store2.Close() }()
	c2, err := NewCoordinator(reg, store2)
	if err != nil {
		t.Fatalf("coordinator reopen: %v", err)
	}
	v0, found, err := c2.FetchOffset("g1", "events", 0)
	if err != nil || !found {
		t.Fatalf("fetch p0: found=%v err=%v", found, err)
	}
	v1, found, err := c2.FetchOffset("g1", "events", 1)
	if err != nil || !found {
		t.Fatalf("fetch p1: found=%v err=%v", found, err)
	}
	if v0.Offset != 42 || v1.Offset != 100 {
		t.Fatalf("offsets = %d/%d, want 42/100", v0.Offset, v1.Offset)
	}
}

func TestCommitMonotonicityEnforcedUnlessReset(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 1}, 10*time.Second)
	m, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	if err := c.CommitOffset("g1", "events", 0, 50, m.Generation, false); err != nil {
		t.Fatalf("commit 50: %v", err)
	}
	if err := c.CommitOffset("g1", "events", 0, 49, m.Generation, false); err == nil {
		t.Fatal("commit below committed offset must be rejected")
	}
	if err := c.CommitOffset("g1", "events", 0, 60, m.Generation, false); err != nil {
		t.Fatalf("commit 60: %v", err)
	}
	if err := c.CommitOffset("g1", "events", 0, 10, m.Generation, true); err != nil {
		t.Fatalf("explicit reset must be allowed: %v", err)
	}
}

func TestCommitUnknownTopic(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 1}, 10*time.Second)
	if err := c.CommitOffset("g1", "nope", 0, 1, 0, false); err == nil {
		t.Fatal("commit to unknown topic must error")
	}
}

func TestCommitStaleGenerationRejected(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 1}, 10*time.Second)
	m1, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	m2, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	_ = m2
	// m1's generation is now stale (m2 joined => rebalance bumped it).
	if err := c.CommitOffset("g1", "events", 0, 1, m1.Generation, false); err == nil {
		t.Fatal("commit with stale generation must be rejected")
	}
}

func TestFetchAllOffsetsForGroup(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 2}, 10*time.Second)
	m, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	_ = c.CommitOffset("g1", "events", 0, 10, m.Generation, false)
	_ = c.CommitOffset("g1", "events", 1, 20, m.Generation, false)
	_ = c.CommitOffset("g1", "events", 0, 30, m.Generation, false) // update p0

	all, err := c.FetchGroupOffsets("g1")
	if err != nil {
		t.Fatalf("fetch all: %v", err)
	}
	got := map[int32]int64{}
	for _, e := range all {
		got[e.Partition] = e.Offset
	}
	if len(got) != 2 || got[0] != 30 || got[1] != 20 {
		t.Fatalf("offsets = %v, want {0:30, 1:20}", got)
	}
}

func TestCompactPreservesLastPerKey(t *testing.T) {
	dir := t.TempDir()
	reg := meta.NewRegistry()
	_ = reg.Create("events", 2)
	store, err := NewOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	c, err := NewCoordinator(reg, store)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	// Commit the same keys repeatedly; compaction should shrink the log.
	m, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	for i := 0; i < 10; i++ {
		if err := c.CommitOffset("g1", "events", 0, int64(i+1), m.Generation, true); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	store2, err := NewOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = store2.Close() }()
	c2, err := NewCoordinator(reg, store2)
	if err != nil {
		t.Fatalf("coordinator reopen: %v", err)
	}
	v, found, err := c2.FetchOffset("g1", "events", 0)
	if err != nil || !found {
		t.Fatalf("fetch: found=%v err=%v", found, err)
	}
	if v.Offset != 10 {
		t.Fatalf("offset = %d, want 10", v.Offset)
	}
}

func TestGroupsValidation(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 1}, 10*time.Second)
	if _, err := c.JoinGroup("", "", []string{"events"}, "range", 0); err == nil {
		t.Fatal("empty group id must error")
	}
	if _, err := c.JoinGroup("g", "", []string{"events", "ev\x00il"}, "range", 0); err == nil {
		t.Fatal("topic with NUL must error")
	}
	if _, err := c.JoinGroup("g", "", nil, "range", 0); err == nil {
		t.Fatal("no topics subscribed must error")
	}
	if _, err := c.JoinGroup("g", "", []string{"events"}, "bogus", 0); err == nil {
		t.Fatal("unknown protocol must error")
	}
	if _, err := c.JoinGroup("g\x00", "", []string{"events"}, "range", 0); err == nil {
		t.Fatal("group with NUL must error")
	}
}

func TestFetchGroupOffsetsFoldsRepeatedKeys(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 1}, 10*time.Second)
	m, _ := c.JoinGroup("g2", "", []string{"events"}, "range", 0)
	_ = c.CommitOffset("g2", "events", 0, 5, m.Generation, false)
	_ = c.CommitOffset("g2", "events", 0, 15, m.Generation, false)
	v, found, _ := c.FetchOffset("g2", "events", 0)
	if !found || v.Offset != 15 {
		t.Fatalf("folded offset = %d found=%v, want 15 true", v.Offset, found)
	}
}

// TestTwoMembersCommitIndependently models the 2-client flow used by the
// acceptance scenario.
func TestTwoMembersCommitIndependently(t *testing.T) {
	c, _ := newTestCoordinator(t, map[string]int{"events": 4}, 10*time.Second)
	m1, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	m2, _ := c.JoinGroup("g1", "", []string{"events"}, "range", 0)
	s1, gen1, _ := c.SyncGroup("g1", m1.MemberID, 0)
	s2, gen2, _ := c.SyncGroup("g1", m2.MemberID, 0)
	if err := c.CommitOffset("g1", "events", s1["events"][0], 100, gen1, false); err != nil {
		t.Fatalf("m1 commit: %v", err)
	}
	if err := c.CommitOffset("g1", "events", s2["events"][0], 200, gen2, false); err != nil {
		t.Fatalf("m2 commit: %v", err)
	}
	all, _ := c.FetchGroupOffsets("g1")
	if len(all) != 2 {
		t.Fatalf("committed entries = %+v, want 2", all)
	}
}
