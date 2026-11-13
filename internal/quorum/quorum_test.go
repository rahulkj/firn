package quorum

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func newTestCluster(t *testing.T, n int, root string) []*Node {
	t.Helper()
	nodes := make([]*Node, 0, n)
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = fmt.Sprintf("n%d", i+1)
	}
	for _, id := range peers {
		tr := NewLocalTransport(id)
		node, err := NewNode(id, peers, fmt.Sprintf("%s/%s", root, id), tr, &NodeOptions{
			ElectionTimeout:   150 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		RegisterLocalNode(node)
		nodes = append(nodes, node)
	}
	for _, node := range nodes {
		node := node
		t.Cleanup(func() {
			UnregisterLocalNode(node)
			node.Close()
		})
	}
	return nodes
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func clusterLeader(nodes []*Node) *Node {
	for _, n := range nodes {
		if n.Role() == Leader {
			return n
		}
	}
	return nil
}

func startAll(t *testing.T, nodes []*Node) {
	t.Helper()
	for _, n := range nodes {
		n.Start()
	}
}

func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if l := clusterLeader(nodes); l != nil {
			// Do not return on the first node to self-report leader; wait
			// until the whole cluster agrees, so proposals do not race a
			// still-incomplete election.
			agree := true
			for _, n := range nodes {
				if n.Leader() != l.ID() {
					agree = false
					break
				}
			}
			if agree {
				return l
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within 3s")
	return nil
}

func proposeTopic(t *testing.T, n *Node, name string) uint64 {
	t.Helper()
	data, _ := json.Marshal(TopicMetadata{Name: name, Partitions: 1, Mode: "stream", ReplicationFactor: 1})
	idx, _, err := n.ProposeNamed(EntryTopicCreate, data)
	if err != nil {
		t.Fatalf("propose topic %q: %v", name, err)
	}
	return idx
}

func TestClusterElectsSingleLeader(t *testing.T) {
	root := t.TempDir()
	nodes := newTestCluster(t, 3, root)
	startAll(t, nodes)

	leader := waitForLeader(t, nodes)
	waitFor(t, "all nodes to agree on leader", 3*time.Second, func() bool {
		for _, n := range nodes {
			if n.Leader() != leader.ID() {
				return false
			}
		}
		return true
	})
	if got := clusterLeader(nodes); got != leader {
		t.Fatalf("leader changed after stabilization: %s -> %s", leader.ID(), got.ID())
	}
}

func TestBlockCommitVisibleOnAllNodes(t *testing.T) {
	root := t.TempDir()
	nodes := newTestCluster(t, 3, root)
	startAll(t, nodes)
	leader := waitForLeader(t, nodes)

	proposeTopic(t, leader, "orders")

	block := BlockCommit{Topic: "orders", Partition: 0, BaseOffset: 0, FirstOffset: 0, LastOffset: 3, Size: 4096, RecordCount: 4, CRCSet: 0xdeadbeef, SealedAtMs: time.Now().UnixMilli()}
	block.ID = make([]byte, 32)
	for i := range block.ID {
		block.ID[i] = byte(i)
	}
	data, _ := json.Marshal(block)
	if _, _, err := leader.ProposeNamed(EntryBlockCommit, data); err != nil {
		t.Fatalf("propose block: %v", err)
	}

	waitFor(t, "block commit visible everywhere", 3*time.Second, func() bool {
		for _, n := range nodes {
			blocks := n.Blocks("orders", 0)
			if len(blocks) != 1 {
				return false
			}
			if blocks[0].RecordCount != 4 || blocks[0].CRCSet != 0xdeadbeef || string(blocks[0].ID) != string(block.ID) {
				return false
			}
		}
		return true
	})

	// All nodes agree the topic exists.
	for _, n := range nodes {
		topics := n.Topics()
		if len(topics) != 1 || topics[0].Name != "orders" {
			t.Fatalf("node %s topics = %+v, want [orders]", n.ID(), topics)
		}
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	root := t.TempDir()
	nodes := newTestCluster(t, 3, root)
	startAll(t, nodes)
	leader := waitForLeader(t, nodes)
	_ = leader

	for _, n := range nodes {
		if n.Role() != Leader {
			if _, _, err := n.ProposeNamed(EntryTopicCreate, []byte(`{"name":"x"}`)); err == nil {
				t.Fatal("follower accepted a proposal")
			}
		}
	}
}

func TestLeaderFailoverPromotesSurvivor(t *testing.T) {
	root := t.TempDir()
	nodes := newTestCluster(t, 3, root)
	startAll(t, nodes)
	old := waitForLeader(t, nodes)

	// Kill the leader (hard stop, like node loss).
	UnregisterLocalNode(old)
	old.Close()

	var survivors []*Node
	for _, n := range nodes {
		if n != old {
			survivors = append(survivors, n)
		}
	}

	waitFor(t, "new leader among survivors", 10*time.Second, func() bool {
		for _, n := range survivors {
			if n.Role() == Leader {
				return true
			}
		}
		return false
	})
	newLeader := clusterLeader(survivors)
	if newLeader == nil {
		t.Fatal("no new leader among survivors")
	}
	if newLeader.ID() == old.ID() {
		t.Fatal("dead leader still considered leader")
	}

	proposeTopic(t, newLeader, "post-failover")
	waitFor(t, "post-failover entry visible on survivors", 10*time.Second, func() bool {
		for _, n := range nodes {
			if n == old {
				continue
			}
			found := false
			for _, topic := range n.Topics() {
				if topic.Name == "post-failover" {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
}

func TestCommittedStateSurvivesFullRestart(t *testing.T) {
	root := t.TempDir()
	nodes := newTestCluster(t, 3, root)
	startAll(t, nodes)
	leader := waitForLeader(t, nodes)

	proposeTopic(t, leader, "durable")
	block := BlockCommit{Topic: "durable", Partition: 0, FirstOffset: 0, LastOffset: 9, Size: 8192, RecordCount: 10, CRCSet: 0x1234abcd, SealedAtMs: time.Now().UnixMilli()}
	block.ID = make([]byte, 32)
	for i := range block.ID {
		block.ID[i] = byte(i)
	}
	data, _ := json.Marshal(block)
	if _, _, err := leader.ProposeNamed(EntryBlockCommit, data); err != nil {
		t.Fatalf("propose block: %v", err)
	}
	waitFor(t, "block commit everywhere", 3*time.Second, func() bool {
		for _, n := range nodes {
			if len(n.Blocks("durable", 0)) != 1 {
				return false
			}
		}
		return true
	})

	// Stop the whole cluster.
	for _, n := range nodes {
		UnregisterLocalNode(n)
		n.Close()
	}

	// Reopen the same directories.
	nodes2 := newTestCluster(t, 3, root)
	for _, n := range nodes2 {
		n.Start()
	}
	// Committed state is visible immediately (recovered from disk), without
	// waiting for a new election.
	for _, n := range nodes2 {
		topics := n.Topics()
		if len(topics) != 1 || topics[0].Name != "durable" {
			t.Fatalf("node %s topics after restart = %+v, want [durable]", n.ID(), topics)
		}
		blocks := n.Blocks("durable", 0)
		if len(blocks) != 1 || blocks[0].RecordCount != 10 {
			t.Fatalf("node %s blocks after restart = %+v", n.ID(), blocks)
		}
	}
}
