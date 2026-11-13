package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/audit"
	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/groups"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/queue"
)

const maxTopicNameLen = 249 // generous bound; comfortably covers every real topic name

// ClusterAdmin wires cluster-aware behavior into the admin surface: topic
// creation is proposed to the metadata quorum and dropped locally per
// apply-hook on every node. Leader-only; followers get 503 + leader address.
type ClusterAdmin struct {
	SelfID             string
	SelfLeader         func() bool
	LeaderAddr         func() string
	ProposeTopicCreate func(name string, partitions int, mode string) error
}

// AdminOption customizes the admin handler.
type AdminOption func(*adminConfig)

type adminConfig struct {
	ca         *ClusterAdmin
	keys       *apikeys.Store
	audit      *audit.Store
	federation *FederationSink
}

// WithClusterAdmin installs cluster-aware topic creation. Pass nil to get
// single-node behavior.
func WithClusterAdmin(ca *ClusterAdmin) AdminOption {
	return func(c *adminConfig) { c.ca = ca }
}

// WithAPIKeys wires the API key store (used to authenticate /apikeys* routes).
func WithAPIKeys(store *apikeys.Store) AdminOption {
	return func(c *adminConfig) { c.keys = store }
}

// WithAudit wires the audit store; admin mutations emit an audit record.
func WithAudit(s *audit.Store) AdminOption {
	return func(c *adminConfig) { c.audit = s }
}

// WithFederation wires the federation control-plane backing store.
// The broker passes shared handles from its internal-topics registry.
func WithFederation(fs *FederationSink) AdminOption {
	return func(c *adminConfig) { c.federation = fs }
}

// NewAdminHandler returns the HTTP/JSON admin surface (docs/adr/002-api.md):
//
//	POST /topics                 {"name": "orders", "partitions": 3, "mode": "stream"|"queue"} -> 201 {"name","partitions","mode"}
//	GET  /topics                                                       -> 200 {"topics": [...]}
//	GET  /groups                                                       -> 200 {"groups": [...]}
//	GET  /groups/{name}/offsets                                        -> 200 {"offsets": [...]}
//	POST /queues/{name}/pull       {"max_messages": 10, "visibility_timeout_ms": 30000} -> 200 {"deliveries": [...]}
//	POST /queues/{name}/ack        {"delivery_id": "..."}              -> 200 {}
func NewAdminHandler(reg *meta.Registry, coord *groups.Coordinator, q *queue.Manager, opts ...AdminOption) http.Handler {
	cfg := &adminConfig{}
	for _, o := range opts {
		o(cfg)
	}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /topics", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitions"`
			Mode       string `json:"mode"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		if req.Name == "" || len(req.Name) > maxTopicNameLen {
			writeError(w, http.StatusBadRequest, "name must be 1-249 characters")
			return
		}
		mode := req.Mode
		if mode == "" {
			mode = meta.ModeStream
		}
		if cfg.ca != nil {
			// Cluster mode: the topic lands in the metadata log on the leader
			// and fans out to every node via the apply hook.
			if !cfg.ca.SelfLeader() {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{
					"error":  "not the cluster leader; create the topic on the leader",
					"leader": cfg.ca.LeaderAddr(),
					"hint":   "firn-cli create-topic works against any node: it finds the leader itself",
				})
				return
			}
			if req.Partitions < 1 {
				writeError(w, http.StatusBadRequest, "partitions must be >= 1")
				return
			}
			if mode != meta.ModeStream && mode != meta.ModeQueue {
				writeError(w, http.StatusBadRequest, "mode must be stream or queue")
				return
			}
			if err := cfg.ca.ProposeTopicCreate(req.Name, req.Partitions, mode); err != nil {
				writeError(w, http.StatusInternalServerError, "propose: "+err.Error())
				return
			}
			if cfg.audit != nil {
				_ = cfg.audit.Append(whoFromCtx(r.Context()), "topic-create-proposed", req.Name, "accepted")
			}
			writeJSON(w, http.StatusAccepted, map[string]any{"name": req.Name, "partitions": req.Partitions, "mode": mode, "status": "propagating"})
			return
		}
		if err := reg.CreateWithMode(req.Name, req.Partitions, req.Mode); err != nil {
			switch {
			case errors.Is(err, meta.ErrInvalidPartitions), errors.Is(err, meta.ErrInvalidTopicMode):
				writeError(w, http.StatusBadRequest, err.Error())
			case errors.Is(err, meta.ErrTopicExists):
				writeError(w, http.StatusConflict, err.Error())
			default:
				writeError(w, http.StatusInternalServerError, "internal error")
			}
			return
		}
		if cfg.audit != nil {
			_ = cfg.audit.Append(whoFromCtx(r.Context()), "topic-created", req.Name, "created")
		}
		writeJSON(w, http.StatusCreated, map[string]any{"name": req.Name, "partitions": req.Partitions, "mode": mode})
	})

	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, r *http.Request) {
		topics := reg.List()
		out := make([]map[string]any, 0, len(topics))
		for _, t := range topics {
			out = append(out, map[string]any{"name": t.Name, "partitions": t.Partitions, "mode": t.Mode})
		}
		writeJSON(w, http.StatusOK, map[string]any{"topics": out})
	})

	mux.HandleFunc("POST /queues/{name}/pull", func(w http.ResponseWriter, r *http.Request) {
		if q == nil {
			writeError(w, http.StatusServiceUnavailable, "queue manager not configured")
			return
		}
		var req struct {
			MaxMessages         int   `json:"max_messages"`
			VisibilityTimeoutMs int64 `json:"visibility_timeout_ms"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		deliveries, err := q.Pull(r.Context(), r.PathValue("name"), req.MaxMessages, time.Duration(req.VisibilityTimeoutMs)*time.Millisecond)
		if err != nil {
			switch {
			case errors.Is(err, queue.ErrQueueNotFound):
				writeError(w, http.StatusNotFound, err.Error())
			case errors.Is(err, queue.ErrNotAQueue):
				writeError(w, http.StatusBadRequest, err.Error())
			case errors.Is(err, fence.ErrFenced):
				writeError(w, http.StatusServiceUnavailable, err.Error())
			default:
				writeError(w, http.StatusInternalServerError, "internal error")
			}
			return
		}
		out := []map[string]any{}
		for _, d := range deliveries {
			out = append(out, map[string]any{
				"deliveryId": d.DeliveryID,
				"queue":      d.Queue,
				"partition":  d.Partition,
				"offset":     d.Offset,
				"key":        string(d.Key),
				"value":      string(d.Value),
				"attempts":   d.Attempts,
				"deadlineMs": d.DeadlineMs,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"deliveries": out})
	})

	mux.HandleFunc("POST /queues/{name}/ack", func(w http.ResponseWriter, r *http.Request) {
		if q == nil {
			writeError(w, http.StatusServiceUnavailable, "queue manager not configured")
			return
		}
		var req struct {
			DeliveryID string `json:"delivery_id"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if req.DeliveryID == "" {
			writeError(w, http.StatusBadRequest, "delivery_id must not be empty")
			return
		}
		if err := q.Ack(r.Context(), r.PathValue("name"), req.DeliveryID); err != nil {
			switch {
			case errors.Is(err, queue.ErrQueueNotFound):
				writeError(w, http.StatusNotFound, err.Error())
			case errors.Is(err, queue.ErrUnknownDelivery):
				writeError(w, http.StatusNotFound, err.Error())
			case errors.Is(err, queue.ErrNotAQueue):
				writeError(w, http.StatusBadRequest, err.Error())
			default:
				writeError(w, http.StatusInternalServerError, "internal error")
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /apikeys", func(w http.ResponseWriter, r *http.Request) {
		if cfg.keys == nil {
			writeError(w, http.StatusServiceUnavailable, "api key store not configured")
			return
		}
		keys := cfg.keys.List()
		out := make([]map[string]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, map[string]any{
				"name":      k.Name,
				"role":      k.Role,
				"createdAt": k.CreatedUnix,
				"revoked":   k.Revoked,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"keys": out})
	})

	mux.HandleFunc("POST /apikeys", func(w http.ResponseWriter, r *http.Request) {
		if cfg.keys == nil {
			writeError(w, http.StatusServiceUnavailable, "api key store not configured")
			return
		}
		var req struct {
			Name       string   `json:"name"`
			Role       string   `json:"role"`
			AllowedIPs []string `json:"allowed_ips,omitempty"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		key, err := cfg.keys.Create(req.Name, req.Role, req.AllowedIPs...)
		if err != nil {
			if errors.Is(err, fence.ErrFenced) {
				writeError(w, http.StatusServiceUnavailable, err.Error())
				return
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if cfg.audit != nil {
			who := whoFromCtx(r.Context())
			_ = cfg.audit.Append(who, "apikey-created", req.Name, "created")
		}
		// The plaintext is returned exactly once. The caller must save it.
		writeJSON(w, http.StatusCreated, map[string]any{"name": req.Name, "role": req.Role, "key": key})
	})

	mux.HandleFunc("DELETE /apikeys/{name}", func(w http.ResponseWriter, r *http.Request) {
		if cfg.keys == nil {
			writeError(w, http.StatusServiceUnavailable, "api key store not configured")
			return
		}
		name := r.PathValue("name")
		if err := cfg.keys.Revoke(name); err != nil {
			if errors.Is(err, fence.ErrFenced) {
				writeError(w, http.StatusServiceUnavailable, err.Error())
				return
			}
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if cfg.audit != nil {
			who := whoFromCtx(r.Context())
			_ = cfg.audit.Append(who, "apikey-revoked", name, "revoked")
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "revoked", "name": name})
	})

	// Federation control plane (ADR-010, slice 1) — only registered when a sink is wired.
	adminRegisterFederationRoutes(mux, cfg)

	mux.HandleFunc("GET /groups", func(w http.ResponseWriter, r *http.Request) {
		if coord == nil {
			writeError(w, http.StatusServiceUnavailable, "group coordinator not configured")
			return
		}
		out := []map[string]any{}
		for _, g := range coord.Groups() {
			members := []map[string]any{}
			for _, m := range g.Members {
				members = append(members, map[string]any{
					"id":            m.ID,
					"topics":        m.Topics,
					"assignment":    m.Assignment,
					"lastHeartbeat": m.LastHeartbeat,
				})
			}
			out = append(out, map[string]any{
				"name":       g.Name,
				"generation": g.Generation,
				"leader":     g.Leader,
				"protocol":   g.Protocol,
				"members":    members,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"groups": out})
	})

	mux.HandleFunc("GET /groups/{name}/offsets", func(w http.ResponseWriter, r *http.Request) {
		if coord == nil {
			writeError(w, http.StatusServiceUnavailable, "group coordinator not configured")
			return
		}
		name := r.PathValue("name")
		offsets, err := coord.FetchGroupOffsets(name)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		out := []map[string]any{}
		for _, e := range offsets {
			out = append(out, map[string]any{
				"group":        e.Group,
				"topic":        e.Topic,
				"partition":    e.Partition,
				"offset":       e.Offset,
				"generationId": e.Generation,
				"leader":       e.Leader,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"group": name, "offsets": out})
	})

	if cfg.audit != nil {
		return auditBudgetGuard(mux, cfg.audit)
	}
	return mux
}

// auditBudgetGuard short-circuits write operations with 503 when the
// audit byte budget is exhausted. Read operations (GET) are allowed
// through unconditionally.
func auditBudgetGuard(next http.Handler, s *audit.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && s != nil && s.BudgetExceeded() {
			writeError(w, http.StatusServiceUnavailable, "audit budget exceeded; increase -audit-max-bytes or restart with a fresh -data-dir")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
