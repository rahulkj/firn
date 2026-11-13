// Federation control plane (ADR-010 slice 1): registration + cursor reads.
// No data motion happens in this file. See docs/plans/004-federation.md.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/audit"
	"github.com/rahulkj/firn/internal/federation"
	"github.com/rahulkj/firn/internal/meta"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/api/firnpb"
)

// FederationSink wraps the federation control-plane store for the admin
// handler. Owned by the broker; shared through the internal-logs registry.
type FederationSink struct {
	store *federation.Store
	mode  FederationMode
}

// FederationMode reports whether the admin surface should allow writes.
// In read-only mode, writes → 503 with an actionable message.
type FederationMode uint8

const (
	FederationModeReadWrite FederationMode = 1
	FederationModeReadOnly  FederationMode = 2
)

// NewFederationSink wires a store and mode. The caller retains ownership of
// the underlying FileLog handles — the broker created them, the broker closes.
func NewFederationSink(store *federation.Store, mode FederationMode) *FederationSink {
	return &FederationSink{store: store, mode: mode}
}

// ErrFederationReadOnly is returned when the cluster is in read-only mode.
type ErrFederationReadOnly struct{}

func (ErrFederationReadOnly) Error() string {
	return "federation: registration is read-only on the follower node; write to the cluster leader"
}

const (
	fieldPeerName      = "name"
	fieldPeerAddr      = "addr"
	fieldPeerDirection = "direction"
	fieldPeerMode      = "mode"
	fieldPeerTopics    = "topics"
	fieldPeerQueues    = "queues"
	fieldPeerCA        = "ca_crt_pem"
)

// MarshalPeerToHTTP projects a Peer into its JSON admin view.
func MarshalPeerToHTTP(p federation.Peer) map[string]any {
	selTopics := []string{}
	selQueues := []string{}
	for _, s := range p.Selectors {
		selTopics = append(selTopics, s.Topics...)
		selQueues = append(selQueues, s.Queues...)
	}
	return map[string]any{
		"name":        p.Name,
		fieldPeerAddr: p.Addr,
		"direction":   p.Direction.String(),
		"mode":        p.Mode.String(),
		"topics":      selTopics,
		"queues":      selQueues,
		"caSet":       len(p.CACrtPEM) > 0,
		"createdAt":   p.CreatedUnix,
		"updatedAt":   p.UpdatedUnix,
	}
}

// MarshalCursorToHTTP projects a Cursor into its JSON admin view.
func MarshalCursorToHTTP(c federation.Cursor) map[string]any {
	return map[string]any{
		"peer":               c.Peer,
		"topic":              c.Topic,
		"partition":          c.Partition,
		"last_source_offset": c.LastSourceOffset,
		"last_local_offset":  c.LastLocalOffset,
		"mode":               c.Mode.String(),
		"updated_unix":       c.UpdatedUnix,
	}
}

func adminRegisterFederationRoutes(mux *http.ServeMux, cfg *adminConfig) {
	cfgCopy := *cfg
	if cfgCopy.federation == nil {
		registerFederationStubs(mux)
		return
	}
	mux.HandleFunc("GET /federation/peers", func(w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, p := range cfgCopy.federation.store.ListPeers() {
			out = append(out, MarshalPeerToHTTP(p))
		}
		writeJSON(w, http.StatusOK, map[string]any{"peers": out})
	})

	mux.HandleFunc("POST /federation/peers", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name      string   `json:"name"`
			Addr      string   `json:"addr"`
			Direction string   `json:"direction"`
			Mode      string   `json:"mode"`
			Topics    []string `json:"topics"`
			Queues    []string `json:"queues"`
			CACrtPEM  string   `json:"ca_crt_pem,omitempty"` // base64-encoded pem body
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		d, err := federation.ParseDirection(req.Direction)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		m, err := federation.ParseMode(req.Mode)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		sel := federation.Selector{}
		if len(req.Topics) > 0 {
			sel.Topics = req.Topics
		}
		if len(req.Queues) > 0 {
			sel.Queues = req.Queues
		}
		peer := federation.Peer{
			Name:      req.Name,
			Addr:      req.Addr,
			Direction: d,
			Mode:      m,
			Selectors: []federation.Selector{sel},
		}
		if req.CACrtPEM != "" {
			crt, err := base64.StdEncoding.DecodeString(req.CACrtPEM)
			if err != nil {
				writeError(w, http.StatusBadRequest, "ca_crt_pem must be base64-encoded PEM")
				return
			}
			peer.CACrtPEM = crt
		}
		if err := cfgCopy.federation.store.AddPeer(peer); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if cfgCopy.audit != nil {
			_ = cfgCopy.audit.Append("anonymous", "federation-peer-added", peer.Name, "created")
		}
		if stored, ok := cfgCopy.federation.store.GetPeer(peer.Name); ok {
			writeJSON(w, http.StatusCreated, MarshalPeerToHTTP(stored))
		} else {
			writeJSON(w, http.StatusCreated, MarshalPeerToHTTP(peer))
		}
	})

	mux.HandleFunc("DELETE /federation/peers/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if err := cfgCopy.federation.store.RemovePeer(name); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if cfgCopy.audit != nil {
			_ = cfgCopy.audit.Append("anonymous", "federation-peer-removed", name, "removed")
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "removed", "name": name})
	})

	mux.HandleFunc("POST /federation/keys", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name       string   `json:"name"`
			AllowedIPs []string `json:"allowed_ips,omitempty"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if cfgCopy.keys == nil {
			writeError(w, http.StatusServiceUnavailable, "api key store not configured")
			return
		}
		key, err := cfgCopy.keys.Create(req.Name, "federation", req.AllowedIPs...)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if cfgCopy.audit != nil {
			_ = cfgCopy.audit.Append("anonymous", "federation-key-created", req.Name, "created")
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"name": req.Name,
			"role": "federation",
			"key":  key,
		})
	})

	mux.HandleFunc("GET /federation/cursors", func(w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, c := range cfgCopy.federation.store.ListCursors() {
			out = append(out, MarshalCursorToHTTP(c))
		}
		writeJSON(w, http.StatusOK, map[string]any{"cursors": out})
	})
}

func registerFederationStubs(mux *http.ServeMux) {
	// Federation control plane disabled: all endpoints return 503 except GET
	// which returns an empty list (so existing CLI tools don't crash).
	mux.HandleFunc("GET /federation/peers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"peers": []any{}})
	})
	mux.HandleFunc("POST /federation/peers", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusServiceUnavailable, "federation not wired; build the broker with --federation-enabled")
	})
	mux.HandleFunc("DELETE /federation/peers/{name}", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusServiceUnavailable, "federation not wired; build the broker with --federation-enabled")
	})
	mux.HandleFunc("POST /federation/keys", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusServiceUnavailable, "federation not wired; build the broker with --federation-enabled")
	})
	mux.HandleFunc("GET /federation/cursors", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"cursors": []any{}})
	})
}

var _ = meta.ErrTopicNotFound    // reserved for later slices
var _ = audit.ErrBudgetExhausted // reserved for later slices
var _ = apikeys.RoleAdmin        // reserved for later slices

// FederationServer implements firnpb.FederationServiceServer.
// Slice 1: registration only — both RPCs return Unimplemented.
type FederationServer struct {
	firnpb.UnimplementedFederationServiceServer
}

func NewFederationServer() *FederationServer { return &FederationServer{} }

func (s *FederationServer) FederationFetch(ctx context.Context, req *firnpb.FederationFetchRequest) (*firnpb.FederationFetchResponse, error) {
	return nil, status.Error(codes.Unimplemented, "federation fetch ships in slice 4; see plan 004")
}

func (s *FederationServer) FederationReport(ctx context.Context, req *firnpb.FederationReportRequest) (*firnpb.FederationReportResponse, error) {
	return nil, status.Error(codes.Unimplemented, "federation report ships in slice 3; see plan 004")
}
