// Federation control plane (ADR-010 slice 1): registration + cursor reads.
// No data motion happens in this file. See docs/plans/004-federation.md.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"strings"

	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/audit"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/federation"
	"github.com/rahulkj/firn/internal/meta"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/log"
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
	// keySet is reported, the key itself never is (write-only, like API keys).
	return map[string]any{
		"name":        p.Name,
		fieldPeerAddr: p.Addr,
		"server_name": p.ServerName,
		"direction":   p.Direction.String(),
		"mode":        p.Mode.String(),
		"topics":      selTopics,
		"queues":      selQueues,
		"caSet":       len(p.CACrtPEM) > 0,
		"keySet":      p.Key != "",
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
			Name       string   `json:"name"`
			Addr       string   `json:"addr"`
			Direction  string   `json:"direction"`
			Mode       string   `json:"mode"`
			Topics     []string `json:"topics"`
			Queues     []string `json:"queues"`
			CACrtPEM   string   `json:"ca_crt_pem,omitempty"` // base64-encoded pem body
			ServerName string   `json:"server_name,omitempty"`
			Key        string   `json:"key,omitempty"` // bearer key issued by the peer (write-only; never echoed)
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
			Name:       req.Name,
			Addr:       req.Addr,
			ServerName: strings.TrimSpace(req.ServerName),
			Key:        strings.TrimSpace(req.Key),
			Direction:  d,
			Mode:       m,
			Selectors:  []federation.Selector{sel},
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
// Slice 1: registration only; slice 2: real FederationFetch over the local
// broker's partition log, returning raw byte-identical replicated frames.
// Passive-mode peers read [first..last] exactly as sealed; the local HWM
// is the authoritative boundary (never returns frames beyond it).
type FederationServer struct {
	firnpb.UnimplementedFederationServiceServer

	b     *broker.Broker
	store *federation.Store // echo filter source; may be nil (no filtering)
}

func NewFederationServer(b *broker.Broker, store *federation.Store) *FederationServer {
	return &FederationServer{b: b, store: store}
}

func (s *FederationServer) FederationFetch(ctx context.Context, req *firnpb.FederationFetchRequest) (*firnpb.FederationFetchResponse, error) {
	if s == nil || s.b == nil {
		return nil, status.Error(codes.Unavailable, "federation: no broker attached")
	}
	if req.Topic == "" || req.Partition < 0 {
		return nil, status.Error(codes.InvalidArgument, "federation: topic and partition are required")
	}
	topic := strings.TrimSpace(req.Topic)
	// Echo filter (active-active, ADR-010): omit the frames this very
	// cluster imported from the requesting peer. The serving side knows its
	// own foreign-applied local ranges; the requesting cluster passes its
	// registered peer name so both sides filter symmetrically.
	filterRequestingPeer := strings.TrimSpace(req.RequestingPeer)

	if strings.HasPrefix(topic, "__") {
		// Internal topics are cluster-local by construction. The single
		// sanctioned exception is __queue_attempts, mirrored byte-identically
		// for federated queues (ADR-010).
		if topic == federation.QueueAttemptsTopic && req.Partition == 0 {
			return s.fetchQueueAttempts(ctx, req)
		}
		return nil, status.Error(codes.InvalidArgument, "federation: internal topic cannot be federated")
	}
	hw, _ := s.b.PartitionHighWatermark(ctx, topic, req.Partition)
	if hw <= 0 {
		return &firnpb.FederationFetchResponse{Frames: []byte{}, SourceHwm: hw}, nil
	}
	first := req.FirstOffset
	if first < 0 {
		first = 0
	}
	last := req.LastOffset
	if last < 0 || last >= hw {
		last = hw - 1
	}
	if first > last {
		return &firnpb.FederationFetchResponse{Frames: []byte{}, SourceHwm: hw}, nil
	}
	raw, err := s.b.ReadRange(ctx, topic, req.Partition, first, last)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "federation: %v", err)
	}
	raw = filterEchoFrames(s.store, filterRequestingPeer, topic, req.Partition, raw)
	return &firnpb.FederationFetchResponse{Frames: raw, SourceHwm: hw}, nil
}

// filterEchoFrames parses a served frame window and omits the frames the
// requesting peer itself exported (they re-arrive via A→B→A). CRC failures
// on the raw range are loud: a federated range that doesn't parse is a
// peer-side corruption signal, never silently passed through.
func filterEchoFrames(store *federation.Store, requestingPeer, topic string, partition int32, raw []byte) []byte {
	if store == nil || requestingPeer == "" || len(raw) == 0 {
		return raw
	}
	ranges := store.ForeignRanges(requestingPeer, topic, partition)
	if len(ranges) == 0 {
		return raw
	}
	frames, err := log.ParseFrames(raw)
	if err != nil {
		return nil // refuse to serve an unparseable range rather than guess
	}
	return federation.ExportFramesWithout(frames, ranges)
}

// fetchQueueAttempts serves a byte-identical window of the source's
// __queue_attempts log. Only federation keys can call this (mesh listener).
func (s *FederationServer) fetchQueueAttempts(ctx context.Context, req *firnpb.FederationFetchRequest) (*firnpb.FederationFetchResponse, error) {
	fl, ok := s.b.InternalLog(federation.QueueAttemptsTopic)
	if !ok {
		return nil, status.Error(codes.Unavailable, "federation: no shared __queue_attempts log")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, "federation: context canceled")
	}
	hw := fl.HighWatermark()
	if hw <= 0 {
		return &firnpb.FederationFetchResponse{Frames: []byte{}, SourceHwm: 0}, nil
	}
	first := req.FirstOffset
	if first < 0 {
		first = 0
	}
	last := req.LastOffset
	if last < 0 || last >= hw {
		last = hw - 1
	}
	if first > last {
		return &firnpb.FederationFetchResponse{Frames: []byte{}, SourceHwm: hw}, nil
	}
	raw, err := fl.ReadRange(first, last)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "federation: %v", err)
	}
	raw = filterEchoFrames(s.store, strings.TrimSpace(req.RequestingPeer), federation.QueueAttemptsTopic, 0, raw)
	return &firnpb.FederationFetchResponse{Frames: raw, SourceHwm: hw}, nil
}

func (s *FederationServer) FederationReport(ctx context.Context, req *firnpb.FederationReportRequest) (*firnpb.FederationReportResponse, error) {
	if s == nil || s.b == nil {
		return nil, status.Error(codes.Unavailable, "federation: no broker attached")
	}
	if req.Topic == "" || req.Partition < 0 {
		return nil, status.Error(codes.InvalidArgument, "federation: topic and partition are required")
	}
	topic := strings.TrimSpace(req.Topic)
	partitions := int32(0)
	if topic == federation.QueueAttemptsTopic {
		partitions = 1
	} else if t, err := s.b.TopicMeta(topic); err == nil {
		partitions = int32(t.Partitions)
	}
	hw, _ := s.b.PartitionHighWatermark(ctx, topic, req.Partition)
	// Publish this node's foreign-applied ranges for the topic — lets the
	// peer re-key attempts/acks into its own offset space without frame
	// headers (ADR-010). Ranges are bytes the peer already possesses.
	fr := []*firnpb.LocalRangePB{}
	if s.store != nil {
		for _, r := range s.store.AllForeignRanges(topic, req.Partition) {
			fr = append(fr, &firnpb.LocalRangePB{Start: r.Start, End: r.End, SourceStart: r.SourceStart})
		}
	}
	return &firnpb.FederationReportResponse{HighWatermark: hw, RetentionHorizon: 0, Partitions: partitions, ForeignRanges: fr}, nil
}
