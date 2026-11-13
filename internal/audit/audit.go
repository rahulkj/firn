// Package audit appends audit records to the replicated __audit topic.
// Every admin mutation is recorded here — topic create/delete, group reset,
// DLQ redrive, key create/revoke, authz failures — with the acting key's
// name and role where known, or "unknown". Writes are deliberately
// best-effort: a failed audit append does not fail the admin operation, and
// a replicated gap is better than a silent drop. Audits are not secret;
// the topic is internal like the others.
package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/log"
)

// ErrBudgetExhausted is returned by Append when the audit byte budget is
// reached. Callers map it to HTTP 503 (or gRPC Unavailable); reads are
// unaffected.
var ErrBudgetExhausted = errors.New("firn/audit: byte budget exhausted")

// auditLog is the minimal logging contract the audit store shares with
// both the shared FileLog (persisted, replicated) and an in-memory MemLog
// for dev/tests.
type auditLog interface {
	Append(key, value []byte) int64
	HighWatermark() int64
}

// Record is one audit entry.
type Record struct {
	TS     int64  `json:"ts"`
	Who    string `json:"who"`
	Action string `json:"action"`
	Target string `json:"target"`
	Result string `json:"result"`
}

// Store is the audit writer. It reuses the broker's shared FileLog.
type Store struct {
	fl         auditLog
	gate       *fence.GateBox
	budget     int64 // bytes; 0 = unlimited
	used       int64
	exceedOnce sync.Once
	exceeded   atomic.Bool
}

// NewStore returns a Store bound to an opened FileLog. The broker owns the
// handle; delete or reset is out of scope for v1 (__audit should be retained).
func NewStore(fl *log.FileLog, byteBudget int64) *Store {
	return &Store{fl: fl, budget: byteBudget}
}

// NewMemStore returns an in-memory audit store (dev/tests/broker without -data-dir).
func NewMemStore(budget int64) *Store {
	return &Store{fl: &log.MemLog{}, budget: budget}
}

// SetBudget overrides the byte budget at runtime (0 disables the check).
func (s *Store) SetBudget(bytes int64) { s.budget = bytes }

// BudgetExceeded reports whether the store refuses further appends.
func (s *Store) BudgetExceeded() bool { return s.exceeded.Load() }

// BytesWritten reports total bytes committed to the __audit topic by this
// process. (Replication from other nodes is not included; the budget is per-node.)
func (s *Store) BytesWritten() int64 {
	return s.used
}

// SetGate binds the store to the internal-topic write fence (see
// internal/fence). Fenced (cluster-mode) nodes fail audit writes closed;
// callers must treat the error as best-effort and log it.
func (s *Store) SetGate(g *fence.GateBox) { s.gate = g }

// Append records one action. The key is ignored; partition 0 for all topics.
func (s *Store) Append(who, action, target, result string) error {
	if s == nil || s.fl == nil {
		return nil // audit silenced or not configured — not an error
	}
	if err := s.gate.Allow("__audit"); err != nil {
		return err
	}
	r := Record{TS: time.Now().UnixMilli(), Who: who, Action: action, Target: target, Result: result}
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	bytes := int64(len(payload))
	if s.budget > 0 && s.used+bytes > s.budget {
		s.exceeded.Store(true)
		s.exceedOnce.Do(func() {
			slog.Error("audit byte budget exhausted; writes rejected. Remediation: stop the broker, mv <data-dir>/__audit-0/00000000000000000000.log* <data-dir>/__audit-0/00000000000000000000.log.1, restart; or restart with -audit-max-bytes=0",
				"budget_bytes", s.budget, "used_bytes", s.used+bytes)
		})
		return ErrBudgetExhausted
	}
	s.fl.Append([]byte("audit"), payload)
	s.used += bytes
	return nil
}

// MustAppend is like Append but panics on I/O failure (used by tests).
func (s *Store) MustAppend(who, action, target, result string) {
	if err := s.Append(who, action, target, result); err != nil {
		panic(fmt.Sprintf("firn/audit: %v", err))
	}
}
