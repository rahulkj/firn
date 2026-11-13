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
	"fmt"
	"time"

	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/log"
)

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
	fl   *log.FileLog
	gate *fence.GateBox
}

// NewStore returns a Store bound to an opened FileLog. The broker owns the
// handle; delete or reset is out of scope for v1 (__audit should be retained).
func NewStore(fl *log.FileLog) *Store { return &Store{fl: fl} }

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
	s.fl.Append([]byte("audit"), payload)
	return nil
}

// MustAppend is like Append but panics on I/O failure (used by tests).
func (s *Store) MustAppend(who, action, target, result string) {
	if err := s.Append(who, action, target, result); err != nil {
		panic(fmt.Sprintf("firn/audit: %v", err))
	}
}
