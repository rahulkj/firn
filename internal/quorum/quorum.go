// Package quorum implements firn's embedded metadata quorum: a minimal,
// correct Raft over a small metadata log. It exists because ADR-007 needs a
// tiny, embeddable consensus for cluster metadata (topics, block
// descriptors, leases, API keys) without an external dependency.
//
// Scope for plan 002-ha phase B: leader election with pre-vote, log
// replication with the standard AppendEntries/RequestVote protocol, crash
// recovery of hard state and the metadata log (CRC-validated append-only
// files, same discipline as internal/log), snapshot-free replay. TCP
// transport lands in phase C with broker-broker replication; tests use an
// in-process transport.
package quorum

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
)

// EntryType tags a metadata log entry.
type EntryType uint8

const (
	EntryNoop EntryType = iota
	EntryTopicCreate
	EntryBlockCommit
	EntryLeaseGrant
)

func (t EntryType) String() string {
	switch t {
	case EntryNoop:
		return "noop"
	case EntryTopicCreate:
		return "topic-create"
	case EntryBlockCommit:
		return "block-commit"
	case EntryLeaseGrant:
		return "lease-grant"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// Entry is one metadata log entry.
type Entry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data"` // JSON payload for Type != noop
}

// ApplyHook, if set, is called synchronously on the node after a metadata
// entry is applied (both for local proposals and entries replicated from
// the leader). Used by firnd to fan topic-create into local registries.
type ApplyHook func(Entry)

// Role is the Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// TopicMetadata is the materialized form of an EntryTopicCreate.
type TopicMetadata struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	Mode              string `json:"mode"`
	ReplicationFactor int    `json:"replication_factor"`
}

// BlockCommit is the materialized form of an EntryBlockCommit.
type BlockCommit struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	BaseOffset  int64  `json:"base_offset"`
	FirstOffset int64  `json:"first_offset"`
	LastOffset  int64  `json:"last_offset"`
	Size        int64  `json:"size"`
	RecordCount int64  `json:"record_count"`
	CRCSet      uint32 `json:"crc_set"`
	SealedAtMs  int64  `json:"sealed_at_ms"`
	ID          []byte `json:"id"` // 32-byte SHA-256 of the block bytes
}

// Lease is the materialized form of an EntryLeaseGrant. The holder of a
// partition's write lease is the only node allowed to append to it.
type Lease struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Holder      string `json:"holder"`
	Epoch       uint64 `json:"epoch"`
	GrantedAtMs int64  `json:"granted_at_ms"`
}

// Transport delivers messages between nodes. LocalTransport is used by tests;
// a TCP transport lands with phase C.
type Transport interface {
	Send(to string, msg Message) error
	Addr() string
}

// Message is the wire format for Raft messages.
type Message struct {
	Type MsgType `json:"type"`
	From string  `json:"from"`
	Term uint64  `json:"term"`
	// RequestVote / reply
	CandidateID  string `json:"candidate_id,omitempty"`
	LastLogIndex uint64 `json:"last_log_index,omitempty"`
	LastLogTerm  uint64 `json:"last_log_term,omitempty"`
	VoteGranted  bool   `json:"vote_granted,omitempty"`
	// AppendEntries / reply
	LeaderID     string  `json:"leader_id,omitempty"`
	PrevLogIndex uint64  `json:"prev_log_index,omitempty"`
	PrevLogTerm  uint64  `json:"prev_log_term,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit,omitempty"`
	Success      bool    `json:"success,omitempty"`
	MatchIndex   uint64  `json:"match_index,omitempty"`
}

// MsgType enumerates message kinds.
type MsgType uint8

const (
	MsgRequestVote MsgType = iota
	MsgRequestVoteReply
	MsgPreVote
	MsgPreVoteReply
	MsgAppendEntries
	MsgAppendEntriesReply
)

// NodeOptions configure a node.
type NodeOptions struct {
	// ElectionTimeout bounds follower silence before an election runs.
	ElectionTimeout time.Duration
	// HeartbeatInterval is the leader's AppendEntries cadence.
	HeartbeatInterval time.Duration
	// ApplyHook, if set, is called synchronously for each entry applied
	// during recovery replay and at runtime. Used by firnd to fan
	// replicated topic-create entries into local registries.
	ApplyHook ApplyHook
}

func (o *NodeOptions) withDefaults() NodeOptions {
	out := NodeOptions{ElectionTimeout: 120 * time.Millisecond, HeartbeatInterval: 40 * time.Millisecond}
	if o != nil {
		if o.ElectionTimeout > 0 {
			out.ElectionTimeout = o.ElectionTimeout
		}
		if o.HeartbeatInterval > 0 {
			out.HeartbeatInterval = o.HeartbeatInterval
		}
	}
	return out
}

// Node is one Raft participant. It owns a metadata directory:
//
//	<dir>/hardstate.json   currentTerm, votedFor (atomic rename)
//	<dir>/log.bin          crc32-framed append-only metadata log
type Node struct {
	id    string
	peers []string
	dir   string
	tr    Transport
	opts  NodeOptions

	mu          sync.Mutex
	role        Role
	currentTerm uint64
	votedFor    string
	leaderID    string

	log         []Entry
	logBytes    int64
	commitIndex uint64
	lastApplied uint64

	// leader-only replication state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// election pacing
	lastHeartbeat   time.Time
	electionTimeout time.Duration
	votesGranted    int
	preVotes        int
	inPreVote       bool

	// materialized state machine
	topics    map[string]TopicMetadata
	blocks    map[string][]BlockCommit
	leases    map[string]Lease
	applyHook ApplyHook

	stopCh    chan struct{}
	started   bool
	mailbox   chan Message
	closeOnce sync.Once
}

// NewNode creates (or reopens) a node rooted at dir.
func NewNode(id string, peers []string, dir string, tr Transport, opts *NodeOptions) (*Node, error) {
	if id == "" || len(peers) < 1 {
		return nil, errors.New("firn/quorum: id and at least one peer are required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("firn/quorum: mkdir %s: %w", dir, err)
	}
	baseOpts := opts.withDefaults()
	n := &Node{
		id:              id,
		peers:           append([]string(nil), peers...),
		dir:             dir,
		tr:              tr,
		opts:            baseOpts,
		role:            Follower,
		lastHeartbeat:   time.Now(),
		electionTimeout: baseOpts.ElectionTimeout + time.Duration(rand.Int63n(int64(baseOpts.ElectionTimeout/2))),
		nextIndex:       map[string]uint64{},
		matchIndex:      map[string]uint64{},
		topics:          map[string]TopicMetadata{},
		blocks:          map[string][]BlockCommit{},
		leases:          map[string]Lease{},
		stopCh:          make(chan struct{}),
		mailbox:         make(chan Message, 1024),
	}
	if opts != nil && opts.ApplyHook != nil {
		n.applyHook = opts.ApplyHook
	}
	sort.Strings(n.peers)
	if err := n.recover(); err != nil {
		return nil, err
	}
	return n, nil
}

// ID returns the node id.
func (n *Node) ID() string { return n.id }

// Deliver injects an inbound message into the node's mailbox. TCP and
// future transports call this from their read loops.
func (n *Node) Deliver(msg Message) {
	select {
	case n.mailbox <- msg:
	default: // full: drop (sender will retry)
	}
}

// SetApplyHook registers (or clears) the synchronous apply hook.
func (n *Node) SetApplyHook(h ApplyHook) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyHook = h
}

// Role returns the node's current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Leader hint, "" if unknown.
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Addr returns the transport address.
func (n *Node) Addr() string { return n.tr.Addr() }

// Topics returns a copy of the materialized topic metadata.
func (n *Node) Topics() []TopicMetadata {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]TopicMetadata, 0, len(n.topics))
	for _, t := range n.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Blocks returns the committed block descriptors for (topic, partition).
func (n *Node) Blocks(topic string, partition int32) []BlockCommit {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]BlockCommit, 0, len(n.blocks[blockKey(topic, partition)]))
	out = append(out, n.blocks[blockKey(topic, partition)]...)
	return out
}

// Lease returns the latest committed lease for (topic, partition), if any.
func (n *Node) Lease(topic string, partition int32) (Lease, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	l, ok := n.leases[blockKey(topic, partition)]
	return l, ok
}

// CommitIndex returns the node's current commit index.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func blockKey(topic string, partition int32) string { return fmt.Sprintf("%s\x00%d", topic, partition) }

// LogBytes returns the size of the replayed on-disk log (diagnostics).
func (n *Node) LogBytes() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.logBytes
}
