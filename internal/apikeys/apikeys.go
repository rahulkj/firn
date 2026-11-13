// Package apikeys implements hashed, file-backed API keys with roles.
// Keys are SHA-256 hashed at rest (fast collisions-irrelevant lookup);
// plaintext is shown once at creation. The underlying log is the internal
// `__api_keys` topic shared via the broker, so keys replicate in cluster
// mode through the same block push/pull path as any other topic.
package apikeys

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rahulkj/eventing/internal/fence"
	"github.com/rahulkj/eventing/internal/log"
)

// Role is the access level for an API key.
type Role uint8

const (
	RoleViewer   Role = 1
	RoleOperator Role = 2
	RoleAdmin    Role = 3
)

func (r Role) String() string {
	switch r {
	case RoleViewer:
		return "viewer"
	case RoleOperator:
		return "operator"
	case RoleAdmin:
		return "admin"
	default:
		return "unknown"
	}
}

// ParseRole converts a role string to a Role.
func ParseRole(s string) (Role, error) {
	switch strings.ToLower(s) {
	case "viewer":
		return RoleViewer, nil
	case "operator":
		return RoleOperator, nil
	case "admin":
		return RoleAdmin, nil
	default:
		return 0, fmt.Errorf("apikeys: unknown role %q", s)
	}
}

// Key is one API key record. Hash is the full SHA-256 (hex-encoded) of the
// plaintext key; the plaintext itself is never persisted.
type Key struct {
	Name        string `json:"name"`
	Role        string `json:"role"`
	Hash        string `json:"hash"`
	CreatedUnix int64  `json:"created_unix"`
	Revoked     bool   `json:"revoked"`
}

// Store folds the __api_keys log into an in-memory map. Single-handle sharing
// (broker-owned FileLog) keeps replication consistent.
type Store struct {
	fl        *log.FileLog
	mu        sync.Mutex
	keys      map[string]Key
	foldedHWM int64
	loaded    bool
	gate      *fence.GateBox
}

// SetGate binds the store to an internal-topic write fence (see
// internal/fence). Fenced (cluster-mode) stores fail closed on
// create/revoke; reads stay open on every node.
func (s *Store) SetGate(g *fence.GateBox) { s.gate = g }

// NewStore returns a Store over an opened FileLog. Call OpenDir to open one.
func NewStore(fl *log.FileLog) *Store { return &Store{fl: fl, keys: map[string]Key{}} }

// OpenDir opens the shared `__api_keys-0` log rooted at dir.
func OpenDir(dir string) (*Store, error) {
	fl, err := log.Open(dir+"/__api_keys-0", log.WithBlockSealing(250*time.Millisecond))
	if err != nil {
		return nil, err
	}
	return NewStore(fl), nil
}

type record struct {
	Action    string `json:"action"` // "upsert" | "revoke"
	Name      string `json:"name"`
	Role      string `json:"role,omitempty"`
	Hash      string `json:"hash"`
	CreatedAt int64  `json:"created_at"`
	Revoked   bool   `json:"revoked,omitempty"`
}

// Create generates a new key (plaintext returned once) and persists it.
func (s *Store) Create(name, role string) (string, error) {
	if err := s.gate.Allow("__api_keys"); err != nil {
		return "", err
	}
	if name == "" {
		return "", errors.New("apikeys: name must not be empty")
	}
	if _, err := ParseRole(role); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return "", err
	}
	if _, exists := s.keys[name]; exists && !s.keys[name].Revoked {
		return "", fmt.Errorf("apikeys: key %q already exists", name)
	}
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	plaintext := "ev-" + base64.RawURLEncoding.EncodeToString(buf)
	rec := record{Action: "upsert", Name: name, Role: strings.ToLower(role), Hash: hashKey(plaintext), CreatedAt: time.Now().UnixMilli()}
	payload, _ := json.Marshal(rec)
	s.fl.Append([]byte("key:"+name), payload)
	s.foldedHWM = s.fl.HighWatermark()
	s.keys[name] = Key{
		Name:        name,
		Role:        strings.ToLower(role),
		Hash:        hashKey(plaintext),
		CreatedUnix: rec.CreatedAt,
	}
	return plaintext, nil
}

// HasAnyKey reports whether at least one non-revoked key has been created.
// Callers use this to decide between open-by-default and enforced modes.
func (s *Store) HasAnyKey() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.refreshIfMoved()
	for _, k := range s.keys {
		if !k.Revoked {
			return true
		}
	}
	return false
}

// List returns non-revoked keys (hash omitted in the public view via callers).
func (s *Store) List() []Key {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.refreshIfMoved()
	out := make([]Key, 0, len(s.keys))
	for _, k := range s.keys {
		out = append(out, k)
	}
	return out
}

// Revoke marks a key name as revoked; subsequent Verify calls reject it.
func (s *Store) Revoke(name string) error {
	if err := s.gate.Allow("__api_keys"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	key, ok := s.findByNameLocked(name)
	if !ok || key.Revoked {
		return fmt.Errorf("apikeys: key %q not found", name)
	}
	rec := record{Action: "revoke", Name: name, Hash: key.Hash, CreatedAt: time.Now().UnixMilli(), Revoked: true}
	payload, _ := json.Marshal(rec)
	s.fl.Append(append([]byte("key:"+name), 0), payload)
	s.foldedHWM = s.fl.HighWatermark()
	key.Revoked = true
	s.keys[key.Name] = key
	return nil
}

// Verify returns the resolved role/name for a plaintext key, or ok=false.
func (s *Store) Verify(plaintext string) (Key, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.refreshIfMoved()
	for _, k := range s.keys {
		if k.Revoked {
			continue
		}
		if k.Hash == hashKey(plaintext) {
			return k, true
		}
	}
	return Key{}, false
}

// Allow reports whether the key (verified) meets the minimum role.
func (s *Store) Allow(plaintext string, min Role) bool {
	k, ok := s.Verify(plaintext)
	if !ok {
		return false
	}
	r, err := ParseRole(k.Role)
	if err != nil {
		return false
	}
	return r >= min
}

func (s *Store) findByNameLocked(name string) (Key, bool) {
	for _, k := range s.keys {
		if k.Name == name {
			return k, true
		}
	}
	return Key{}, false
}

// loadLocked folds the log once (first call).
func (s *Store) loadLocked() error {
	if s.fl == nil {
		return nil
	}
	if s.loaded {
		return nil
	}
	return s.refoldLocked()
}

// refoldLocked replaces the folded state with a fresh scan.
func (s *Store) refoldLocked() error {
	if s.fl == nil {
		return nil
	}
	hw := s.fl.HighWatermark()
	fresh := map[string]Key{}
	if hw > 0 {
		recs, err := s.fl.Fetch(0, int(hw))
		if err != nil {
			return err
		}
		for _, r := range recs {
			applyRecord(&fresh, r.Value)
		}
	}
	s.keys = fresh
	s.foldedHWM = hw
	s.loaded = true
	return nil
}

// refreshIfMoved re-folds the log only when the high watermark changed
// (local create/revoke, or a replicated apply). The hot verify path is
// O(1) against the cache when the log is unchanged. On fold failure the
// last-known-good cache is kept.
func (s *Store) refreshIfMoved() error {
	if s.fl == nil {
		return nil
	}
	if err := s.loadLocked(); err != nil {
		return err
	}
	hw := s.fl.HighWatermark()
	if hw == s.foldedHWM {
		return nil
	}
	fresh := map[string]Key{}
	if hw > 0 {
		recs, err := s.fl.Fetch(0, int(hw))
		if err != nil {
			return err
		}
		for _, r := range recs {
			applyRecord(&fresh, r.Value)
		}
	}
	s.keys = fresh
	s.foldedHWM = hw
	return nil
}

func applyRecord(out *map[string]Key, payload []byte) {
	var rec record
	if err := json.Unmarshal(payload, &rec); err != nil {
		return
	}
	switch rec.Action {
	case "upsert":
		(*out)[rec.Name] = Key{
			Name:        rec.Name,
			Role:        orDefault(rec.Role, "viewer"),
			Hash:        rec.Hash,
			CreatedUnix: rec.CreatedAt,
			Revoked:     rec.Revoked,
		}
	case "revoke":
		if k, ok := (*out)[rec.Name]; ok {
			k.Revoked = true
			(*out)[rec.Name] = k
		} else {
			(*out)[rec.Name] = Key{Name: rec.Name, Hash: rec.Hash, CreatedUnix: rec.CreatedAt, Revoked: true}
		}
	}
}

func hashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
