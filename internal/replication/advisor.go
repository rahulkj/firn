package replication

import (
	"fmt"
	"time"

	"github.com/rahulkj/eventing/internal/broker"
	"github.com/rahulkj/eventing/internal/fence"
	"github.com/rahulkj/eventing/internal/quorum"
)

// Advisor implements broker.LeaseAdvisor against the applied quorum lease
// table. A partition is writable on this node iff (a) a committed lease
// names us as holder, and (b) it is not expired (GrantedAtMs + ttl).
type Advisor struct {
	nodeID string
	q      *quorum.Node
	ttl    time.Duration
}

// NewAdvisor wires the advisor for a node.
func NewAdvisor(nodeID string, q *quorum.Node, ttl time.Duration) *Advisor {
	if ttl == 0 {
		ttl = defaultLeaseTTL
	}
	return &Advisor{nodeID: nodeID, q: q, ttl: ttl}
}

// WriteAllowed reports whether this node holds a fresh write lease.
func (a *Advisor) WriteAllowed(topic string, partition int32) bool {
	l, ok := a.q.Lease(topic, partition)
	if !ok {
		return false
	}
	if l.Holder != a.nodeID {
		return false
	}
	return time.Now().UnixMilli() < l.GrantedAtMs+int64(a.ttl/time.Millisecond)
}

// Holder returns the lease holder for (topic, partition), if known.
func (a *Advisor) Holder(topic string, partition int32) string {
	l, ok := a.q.Lease(topic, partition)
	if !ok {
		return ""
	}
	return l.Holder
}

// InternalGate returns a fence.Gate allowing writes only on the node
// holding a fresh lease for (topic, 0). This is the write fence for the
// single-partition internal topics (see internal/fence and plan 002 phase F
// completion): the node that owns the lease is the single writer; everyone
// else fails closed with a retryable error.
func (a *Advisor) InternalGate() fence.Gate {
	return func(topic string) error {
		if a.WriteAllowed(topic, 0) {
			return nil
		}
		holder := a.Holder(topic, 0)
		if holder == "" {
			return fmt.Errorf("%w: %s (no fresh lease; retry shortly)", fence.ErrFenced, topic)
		}
		return fmt.Errorf("%w: %s held by %s; retry on the holder", fence.ErrFenced, topic, holder)
	}
}

var _ broker.LeaseAdvisor = (*Advisor)(nil)
