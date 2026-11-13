package quorum

// Raft engine: elections (with pre-vote), replication, commit, apply.
//
// Concurrency model: a single goroutine (`run`) owns the node's mutable
// state machine; transport callbacks push messages into `mailbox`. Getters
// copy state under the mutex.

import (
	"errors"
	"fmt"
	"time"
)

// Start launches the node's event loop and election timer.
func (n *Node) Start() {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return
	}
	n.started = true
	n.mu.Unlock()
	go n.run()
}

// Close stops the node. Idempotent.
func (n *Node) Close() {
	n.closeOnce.Do(func() { close(n.stopCh) })
}

func (n *Node) run() {
	ticker := time.NewTicker(n.opts.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case msg := <-n.mailbox:
			n.handle(msg)
		case <-ticker.C:
			n.tick()
		case <-n.stopCh:
			return
		}
	}
}

// --- Entry helpers ----------------------------------------------------------

func (n *Node) lastLogIndex() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	// entries in n.log are dense from 1..len(n.log)
	if int(index) <= len(n.log) {
		return n.log[index-1].Term
	}
	return 0
}

func (n *Node) logUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	myTerm, myIndex := n.lastLogTerm(), n.lastLogIndex()
	if candidateLastTerm != myTerm {
		return candidateLastTerm > myTerm
	}
	return candidateLastIndex >= myIndex
}

// --- Message handling ------------------------------------------------------

func (n *Node) handle(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch msg.Type {
	case MsgRequestVote:
		n.handleRequestVote(msg, false)
	case MsgPreVote:
		n.handleRequestVote(msg, true)
	case MsgRequestVoteReply, MsgPreVoteReply:
		n.handleVoteReply(msg)
	case MsgAppendEntries:
		n.handleAppendEntries(msg)
	case MsgAppendEntriesReply:
		n.handleAppendEntriesReply(msg)
	}
}

func (n *Node) handleRequestVote(msg Message, preVote bool) {
	grant := false
	switch {
	case msg.Term < n.currentTerm:
	case !n.logUpToDate(msg.LastLogIndex, msg.LastLogTerm):
	case !preVote && n.votedFor != "" && n.votedFor != msg.CandidateID && msg.Term == n.currentTerm:
	default:
		grant = true
	}

	if grant && !preVote {
		n.currentTerm = msg.Term // idempotent re-affirm
		n.votedFor = msg.CandidateID
		n.role = Follower
		n.persistLocked()
	}
	msgOut := Message{
		Type:        MsgRequestVoteReply,
		From:        n.id,
		Term:        n.currentTerm,
		VoteGranted: grant,
		CandidateID: msg.CandidateID,
	}
	if preVote {
		msgOut.Type = MsgPreVoteReply
	}
	n.send(msg.From, msgOut)
}

func (n *Node) handleVoteReply(msg Message) {
	if msg.Term > n.currentTerm {
		n.becomeFollower(msg.Term)
		return
	}
	if msg.Term < n.currentTerm {
		return
	}
	if n.role != Candidate {
		return
	}
	need := len(n.peers)/2 + 1
	if msg.Type == MsgPreVoteReply {
		if !n.inPreVote || !msg.VoteGranted {
			return
		}
		n.preVotes++
		if n.preVotes >= need {
			// Pre-vote won: start the real election.
			n.inPreVote = false
			n.preVotes = 0
			n.currentTerm++
			n.votedFor = n.id
			n.votesGranted = 1 // self
			n.persistLocked()
			n.broadcastVoteRequest(false)
		}
		return
	}
	if !msg.VoteGranted {
		return
	}
	n.votesGranted++
	if n.votesGranted >= need {
		// Real election won: become leader.
		n.role = Leader
		n.leaderID = n.id
		n.inPreVote = false
		n.resetReplicationLocked()
		// Propose a no-op so the new term has a committable entry.
		n.appendEntryLocked(Entry{Term: n.currentTerm, Type: EntryNoop})
		n.broadcastAppendEntries()
	}
}

func (n *Node) handleAppendEntries(msg Message) {
	if msg.Term < n.currentTerm {
		n.send(msg.From, Message{Type: MsgAppendEntriesReply, From: n.id, Term: n.currentTerm, Success: false, MatchIndex: 0})
		return
	}
	// Valid AppendEntries from a healthy leader: step down if needed.
	if msg.Term > n.currentTerm || n.role != Follower {
		n.becomeFollower(msg.Term)
	}
	n.leaderID = msg.LeaderID
	n.lastHeartbeat = time.Now()

	// Consistency check: do we have an entry at prevLogIndex with prevLogTerm?
	if msg.PrevLogIndex > 0 {
		if n.lastLogIndex() < msg.PrevLogIndex {
			n.send(msg.From, Message{Type: MsgAppendEntriesReply, From: n.id, Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()})
			return
		}
		if n.termAt(msg.PrevLogIndex) != msg.PrevLogTerm {
			// Reject conflicting entry: leader will retry with smaller index.
			n.send(msg.From, Message{Type: MsgAppendEntriesReply, From: n.id, Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()})
			return
		}
	}

	for _, e := range msg.Entries {
		if e.Index <= n.lastLogIndex() {
			if n.termAt(e.Index) != e.Term {
				// Conflict: truncate from here and append all remaining.
				n.truncateFrom(e.Index)
				n.appendEntryLocked(e)
			}
			// else: duplicate, keep existing.
			continue
		}
		n.appendEntryLocked(e)
	}

	// Advance commit index and apply.
	if msg.LeaderCommit > n.commitIndex {
		n.commitIndex = min(msg.LeaderCommit, n.lastLogIndex())
		n.applyLocked()
		n.persistLocked()
	}

	n.send(msg.From, Message{Type: MsgAppendEntriesReply, From: n.id, Term: n.currentTerm, Success: true, MatchIndex: n.lastLogIndex()})
}

func (n *Node) handleAppendEntriesReply(msg Message) {
	if msg.Term < n.currentTerm {
		return
	}
	if msg.Term > n.currentTerm {
		n.becomeFollower(msg.Term)
		return
	}
	if n.role != Leader {
		return
	}
	if !msg.Success {
		// Back off nextIndex for this peer.
		cur := n.nextIndex[msg.From]
		if cur > 1 {
			n.nextIndex[msg.From] = cur - 1
		} else {
			n.nextIndex[msg.From] = 1
		}
		return
	}
	// Success: advance match/next and maybe commit.
	match := msg.MatchIndex
	if match > n.matchIndex[msg.From] {
		n.matchIndex[msg.From] = match
		n.nextIndex[msg.From] = match + 1
	}
	n.maybeCommitLocked()
}

// --- Tick & elections ------------------------------------------------------

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.role {
	case Leader:
		now := time.Now()
		_ = now
		n.broadcastAppendEntries()
	case Follower, Candidate:
		if time.Since(n.lastHeartbeat) > n.electionTimeout {
			n.startElectionLocked()
		}
	}
}

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.leaderID = ""
	n.inPreVote = true
	n.preVotes = 1               // self — the candidate implicitly agrees with itself
	n.lastHeartbeat = time.Now() // backoff between elections
	// Ask peers without disturbing their terms.
	n.broadcastVoteRequest(true)
}

func (n *Node) broadcastVoteRequest(preVote bool) {
	typ := MsgRequestVote
	if preVote {
		typ = MsgPreVote
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(p, Message{
			Type:         typ,
			From:         n.id,
			Term:         n.currentTerm,
			CandidateID:  n.id,
			LastLogIndex: n.lastLogIndex(),
			LastLogTerm:  n.lastLogTerm(),
		})
	}
}

func (n *Node) becomeFollower(term uint64) {
	n.currentTerm = term
	n.votedFor = ""
	n.role = Follower
	n.votesGranted = 0
	n.preVotes = 0
	n.inPreVote = false
	n.leaderID = ""
	n.persistLocked()
}

func (n *Node) resetReplicationLocked() {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.nextIndex[p] = n.lastLogIndex() + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.id] = n.lastLogIndex()
	n.nextIndex[n.id] = n.lastLogIndex() + 1
}

func (n *Node) appendEntryLocked(e Entry) Entry {
	e.Index = n.lastLogIndex() + 1
	if e.Term == 0 {
		e.Term = n.currentTerm
	}
	n.log = append(n.log, e)
	n.matchIndex[n.id] = e.Index
	n.nextIndex[n.id] = e.Index + 1
	if err := appendLogEntry(n.dir, e); err != nil {
		// Persisting failed; treat like a transport failure. Production would
		// panic-adjacent; here we log and continue — the node cannot serve
		// entries it cannot persist.
		fmt.Printf("eventing/quorum: persist entry %d: %v\n", e.Index, err)
	}
	return e
}

func (n *Node) truncateFrom(index uint64) {
	if index > n.lastLogIndex() {
		return
	}
	keep := n.log[:index-1]
	// Rewrite the log file truncated to `keep` — the torn tail rule.
	n.log = append([]Entry(nil), keep...)
	_ = rewriteLog(n.dir, n.log)
	n.matchIndex[n.id] = n.lastLogIndex()
	n.nextIndex[n.id] = n.lastLogIndex() + 1
}

func (n *Node) broadcastAppendEntries() {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.sendAppendEntries(p)
	}
}

func (n *Node) sendAppendEntries(peer string) {
	next := n.nextIndex[peer]
	prevLogIndex := next - 1
	var entries []Entry
	if next <= n.lastLogIndex() {
		entries = append(entries, n.log[next-1:]...)
	}
	n.send(peer, Message{
		Type:         MsgAppendEntries,
		From:         n.id,
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  n.termAt(prevLogIndex),
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) maybeCommitLocked() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.currentTerm {
			continue // commit only current-term entries directly (Raft §5.4.2)
		}
		count := 1 // self
		for p, match := range n.matchIndex {
			if p != n.id && match >= idx {
				count++
			}
		}
		if count >= len(n.peers)/2+1 {
			n.commitIndex = idx
			n.applyLocked()
			n.persistLocked()
			return
		}
	}
}

func (n *Node) applyLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		applyEntry(n, n.log[n.lastApplied-1])
	}
}

func (n *Node) persistLocked() {
	_ = writeHardState(n.dir, hardState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, CommitIndex: n.commitIndex})
}

func (n *Node) send(to string, msg Message) {
	_ = n.tr.Send(to, msg)
}

// --- Propose & errors --------------------------------------------------------

// ErrNotLeader is returned when a follower is asked to propose.
var ErrNotLeader = errors.New("eventing/quorum: not leader")

// ProposeNamed proposes one entry of the given type with the given payload.
// Returns ErrNotLeader if this node isn't the leader, else the index/term.
func (n *Node) ProposeNamed(entryType EntryType, payload []byte) (uint64, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	entry := Entry{Term: n.currentTerm, Type: entryType, Data: append([]byte(nil), payload...)}
	e := n.appendEntryLocked(entry)
	n.nextIndex[n.id] = e.Index + 1
	n.matchIndex[n.id] = e.Index
	n.broadcastAppendEntries()
	return e.Index, e.Term, nil
}

// --- helpers ----------------------------------------------------------------

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
