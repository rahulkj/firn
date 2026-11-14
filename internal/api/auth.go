package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/peer"

	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/audit"
	"github.com/rahulkj/firn/internal/ratelimit"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// authKeyContextKey carries credentials into handlers.
type authKeyContextKey struct{}

// auth is the authenticator for the admin/broker surfaces. It enforces:
//   - Bearer-token authentication against a hashed key store
//   - role-based authorization (viewer < operator < admin)
//   - per-key token-bucket rate limiting
//   - audit records for every refusal (on the audit log if configured)
//
// If the key store is nil, the authenticator is a no-op (dev default).
type auth struct {
	store   *apikeys.Store
	limiter *ratelimit.Bucket
	anon    *ratelimit.SlidingWindow
	audit   *audit.Store

	// uiSessions mints and verifies the short-lived browser sessions used
	// by /ui/* (ADR-013 Slice C). Always non-nil; in-memory only.
	uiSessions *uiSessionStore

	// Security-by-default (ADR-011 slice 1): an unopened broker refuses
	// every request. Opening requires either a key to exist, or the
	// operator to pass -dev / -allow-open-bootstrap, or a bootstrap
	// token printed on stderr at first start.
	allowOpen      bool
	bootstrapToken string
	bootstrapMu    sync.Mutex
	bootstrapUsed  bool
}

// AuthOption tunes the authenticator. Default (no options) is secure-by-default:
// nothing opens until a key exists or the operator asks for open bootstrap.
type AuthOption func(*auth)

// WithOpenBootstrap allows the broker to serve with no keys (development).
func WithOpenBootstrap(on bool) AuthOption { return func(a *auth) { a.allowOpen = on } }

// WithBootstrapToken enables one-time-bootstrap mode: the daemon prints the
// token on stderr; POST /apikeys with that bearer creates the first key.
func WithBootstrapToken(token string) AuthOption { return func(a *auth) { a.bootstrapToken = token } }

// WithAnonRate caps anonymous traffic (pre-auth requests and refused-key
// attempts) to `max` hits per `window` per source IP. Zero max disables.
func WithAnonRate(max int, window time.Duration) AuthOption {
	return func(a *auth) { a.anon = ratelimit.NewSlidingWindow(max, window) }
}

// NewAuthorizer returns an authenticator wired to these dependencies.
// Default (no options) fails closed.
func NewAuthorizer(store *apikeys.Store, limiter *ratelimit.Bucket, audit *audit.Store, opts ...AuthOption) *auth {
	a := &auth{store: store, limiter: limiter, audit: audit, uiSessions: newUISessionStore()}
	for _, o := range opts {
		o(a)
	}
	return a
}

// needsAuth reports whether a request must be authenticated.
type routeRole struct {
	roles  map[string]apikeys.Role // pattern -> min role
	public []string                // path prefixes that need no key
}

var routeTable = routeRole{
	public: []string{"/healthz"},
	roles: map[string]apikeys.Role{
		"POST /topics":     apikeys.RoleOperator,
		"POST /queues/":    apikeys.RoleOperator, // pull/ack/pull/ack... matched by prefix at runtime
		"GET /apikeys":     apikeys.RoleAdmin,
		"POST /apikeys":    apikeys.RoleAdmin,
		"DELETE /apikeys/": apikeys.RoleAdmin,
		"POST /groups/":    apikeys.RoleOperator,
		"GET /groups":      apikeys.RoleViewer,
		"GET /topics":      apikeys.RoleViewer,
	},
}

func roleFor(method, path string) (apikeys.Role, bool) {
	for prefix, role := range routeTable.roles {
		typ := "POST"
		if strings.HasPrefix(prefix, "GET ") {
			typ = "GET"
			prefix = strings.TrimPrefix(prefix, "GET ")
		} else if strings.HasPrefix(prefix, "DELETE ") {
			typ = "DELETE"
			prefix = strings.TrimPrefix(prefix, "DELETE ")
		} else if strings.HasPrefix(prefix, "POST ") {
			prefix = strings.TrimPrefix(prefix, "POST ")
		}
		if method == typ && strings.HasPrefix(path, prefix) {
			return role, true
		}
	}
	// Default: everything requires viewer.
	return apikeys.RoleViewer, true
}

// Wrap applies authentication and per-key rate limiting to next.
// Default is fail-closed (see WithOpenBootstrap).
func (a *auth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.anon != nil && !a.anon.Allow(clientIP(r)) {
			if a.audit != nil {
				_ = a.audit.Append("anonymous", "anon-rate-limit", r.Method+" "+r.URL.Path, "rate limited")
			}
			writeError(w, http.StatusTooManyRequests, "rate limited")
			return
		}
		switch {
		case isPublic(r.URL.Path):
			openCtx := context.WithValue(r.Context(), authKeyContextKey{}, apikeys.Key{Name: "anonymous", Role: "admin"})
			next.ServeHTTP(w, r.WithContext(openCtx))
			return
		case a.allowOpen || (a.store == nil && !a.bootstrap()):
			openCtx := context.WithValue(r.Context(), authKeyContextKey{}, apikeys.Key{Name: "anonymous", Role: "admin"})
			next.ServeHTTP(w, r.WithContext(openCtx))
			return
		case a.bootstrap():
			a.bootstrapGate(w, r, next)
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == r.Header.Get("Authorization") {
			token = "" // no Bearer prefix
		}
		if token == "" {
			// /ui/* only: a browser cannot set Authorization, so it may
			// present the session cookie minted by POST /ui/session instead.
			// The cookie is never accepted on the admin JSON API.
			if isUIPath(r.URL.Path) {
				if c, err := r.Cookie(uiSessionCookieName); err == nil && c.Value != "" {
					if !a.limiter.Allow("ui:" + c.Value) {
						a.auditRefused(r, "rate limited")
						writeError(w, http.StatusTooManyRequests, "rate limited")
						return
					}
					if skey, ok := a.uiSessions.verify(a.store, c.Value); ok {
						if r2, ok := a.authorizeKey(w, r, skey); ok {
							next.ServeHTTP(w, r2)
						}
						return
					}
					// Stale/orphaned session: fall through to the 401.
				}
			}
			a.auditRefused(r, "missing token")
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		if !a.limiter.Allow(token) {
			a.auditRefused(r, "rate limited")
			writeError(w, http.StatusTooManyRequests, "rate limited")
			return
		}
		key, ok := a.store.Verify(token)
		if !ok {
			a.auditRefused(r, "unknown or revoked key")
			writeError(w, http.StatusUnauthorized, "unknown or revoked key")
			return
		}
		if r2, ok := a.authorizeKey(w, r, key); ok {
			next.ServeHTTP(w, r2)
		}
	})
}

// authorizeKey applies the checks common to bearer- and cookie-
// authenticated requests (never called before the per-key rate limit):
// the key's allowed_ips allowlist, the minimum role for the route, and
// returns the request with the identity in context. On refusal it writes
// the response and returns ok=false.
func (a *auth) authorizeKey(w http.ResponseWriter, r *http.Request, key apikeys.Key) (*http.Request, bool) {
	ip := net.ParseIP(clientIP(r))
	if !authzKeyAllowsIP(a.store, key, ip) {
		if a.audit != nil {
			_ = a.audit.Append(key.Name, "authz-refused:ip-not-allowed", r.Method+" "+r.URL.Path, "client IP rejected by key allowlist")
		}
		writeError(w, http.StatusForbidden, "ip not allowed")
		return r, false
	}
	minRole, needs := roleFor(r.Method, r.URL.Path)
	if needs {
		rr, _ := apikeys.ParseRole(key.Role)
		if rr < minRole {
			a.auditRefused(r, "insufficient role")
			writeError(w, http.StatusForbidden, "insufficient role")
			return r, false
		}
	}
	ctx := context.WithValue(r.Context(), authKeyContextKey{}, key)
	return r.WithContext(ctx), true
}

// Commands under this map require the listed role; everything else is viewer+.
var grpcRoles = map[string]apikeys.Role{
	// Operator-tier (produce, queue, groups write)
	"/firn.v1.FirnService/Produce":      apikeys.RoleOperator,
	"/firn.v1.FirnService/JoinGroup":    apikeys.RoleOperator,
	"/firn.v1.FirnService/Heartbeat":    apikeys.RoleOperator,
	"/firn.v1.FirnService/Pull":         apikeys.RoleOperator,
	"/firn.v1.FirnService/Ack":          apikeys.RoleOperator,
	"/firn.v1.FirnService/CommitOffset": apikeys.RoleOperator,
	// Viewer-tier (read-only)
	"/firn.v1.FirnService/SyncGroup":         apikeys.RoleViewer,
	"/firn.v1.FirnService/LeaveGroup":        apikeys.RoleViewer,
	"/firn.v1.FirnService/Fetch":             apikeys.RoleViewer,
	"/firn.v1.FirnService/FetchOffset":       apikeys.RoleViewer,
	"/firn.v1.FirnService/FetchGroupOffsets": apikeys.RoleViewer,
	"/firn.v1.FirnService/GetMetadata":       apikeys.RoleViewer,
	"/firn.v1.ReplicaService/ReplicaFetch":   apikeys.RoleViewer,
	// Internal-tier (ReplicaPut is operator on the mesh port too)
	"/firn.v1.ReplicaService/ReplicaPut": apikeys.RoleOperator,
	// Federation-tier (only federation keys may call these)
	"/firn.v1.FederationService/FederationFetch":  apikeys.RoleFederation,
	"/firn.v1.FederationService/FederationReport": apikeys.RoleFederation,
}

// MeshUnaryInterceptor is the mesh-listener analogue of GRPCInterceptor,
// scoped to what the mesh actually carries: ReplicaService/QuorumService
// (mTLS is the auth — nothing else is needed and nothing can call them
// anyway), plus FederationService, which additionally requires a
// federation-scoped API key exactly like the public path. Without this,
// any mesh cert (any node, anything with a pinned peer CA) could walk off
// with arbitrary frames — the federation key's entire documented purpose.
func (a *auth) MeshUnaryInterceptor() grpc.UnaryServerInterceptor {
	interceptor := a.GRPCInterceptor()
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if !strings.HasPrefix(info.FullMethod, "/firn.v1.FederationService/") {
			// ReplicaService/QuorumService: the mTLS handshake is the authz.
			return handler(ctx, req)
		}
		return interceptor(ctx, req, info, handler)
	}
}
func (a *auth) GRPCInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		// No auth configured: either explicitly open (dev), or a misconfigured
		// daemon with no key store at all. Fail closed unless -dev/-allow-open-bootstrap.
		if a.allowOpen || (a.store == nil && a.bootstrapToken == "") {
			return handler(ctx, req)
		}
		if a.bootstrap() {
			return nil, status.Error(codes.Unauthenticated, "bootstrap mode: no keys exist; create one via HTTP POST /apikeys with the one-time bootstrap token (see firnd stderr), or restart with -dev")
		}
		md, ok := metadata.FromIncomingContext(ctx)
		var tok string
		if ok {
			if vs := md.Get("authorization"); len(vs) > 0 {
				tok = strings.TrimPrefix(vs[0], "Bearer ")
			}
		}
		if tok == "" {
			return nil, status.Error(codes.Unauthenticated, "missing bearer token")
		}
		if !a.limiter.Allow(tok) {
			return nil, status.Error(codes.ResourceExhausted, "rate limited")
		}
		key, ok := a.store.Verify(tok)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "unknown or revoked key")
		}
		ip := net.ParseIP(gRPCClientIP(ctx))
		if !authzKeyAllowsIP(a.store, key, ip) {
			if a.audit != nil {
				_ = a.audit.Append(key.Name, "authz-refused:ip-not-allowed", info.FullMethod, "client IP rejected by key allowlist")
			}
			return nil, status.Error(codes.PermissionDenied, "ip not allowed")
		}
		min, mapped := grpcRoles[info.FullMethod]
		if !mapped {
			min = apikeys.RoleOperator // default operator for any unmapped FirnService RPC
		}
		r, err := apikeys.ParseRole(key.Role)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "unrecognised role on key")
		}
		// Federation keys are isolated: they only satisfy federation RPCs.
		if r == apikeys.RoleFederation && min != apikeys.RoleFederation {
			return nil, status.Error(codes.PermissionDenied, "federation key cannot call this RPC")
		}
		if r >= min || (min == apikeys.RoleFederation && r == apikeys.RoleFederation) {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.PermissionDenied, "insufficient role")
	}
}

// bootstrapGate handles one-time-bootstrap mode: only POST /apikeys with the
// one-time token passes; the token mints exactly one admin key and is then
// void. Everything else is 401 with an actionable message.
func (a *auth) bootstrapGate(w http.ResponseWriter, r *http.Request, next http.Handler) {
	if r.Method != http.MethodPost || r.URL.Path != "/apikeys" {
		a.auditRefused(r, "bootstrap mode: only POST /apikeys is reachable until first key exists")
		writeError(w, http.StatusUnauthorized, "bootstrap mode: no keys exist yet; POST /apikeys with the one-time bootstrap token (see firnd stderr), or restart with -dev/-allow-open-bootstrap")
		return
	}
	a.bootstrapMu.Lock()
	defer a.bootstrapMu.Unlock()
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == r.Header.Get("Authorization") {
		token = "" // no Bearer prefix
	}
	if a.bootstrapUsed || token == "" || token != a.bootstrapToken {
		a.auditRefused(r, "bootstrap token rejected")
		writeError(w, http.StatusUnauthorized, "invalid bootstrap token")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	// Bootstrap minting is always an admin key; never let role drop it.
	req["role"] = "admin"
	reqBytes, _ := json.Marshal(req)
	r2 := r.Clone(r.Context())
	r2.Body = io.NopCloser(bytes.NewReader(reqBytes))
	r2.ContentLength = int64(len(reqBytes))
	ctx := context.WithValue(r2.Context(), authKeyContextKey{}, apikeys.Key{Name: "bootstrap", Role: "admin"})
	rec := &statusRecorder{ResponseWriter: w, httpCode: http.StatusOK}
	next.ServeHTTP(rec, r2.WithContext(ctx))
	if rec.httpCode == http.StatusCreated {
		a.bootstrapUsed = true
		if a.audit != nil {
			_ = a.audit.Append("bootstrap", "bootstrap-token-consumed", "apikey-created", "first admin key minted")
		}
	}
}

type statusRecorder struct {
	http.ResponseWriter
	httpCode int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.httpCode = code
	w.ResponseWriter.WriteHeader(code)
}

// clientIP extracts the caller's bare IP from the request, stripping any
// port. Falls back to the raw RemoteAddr if parsing fails.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Trust only the first hop; if you're behind a chain of proxies the
		// caller is responsible for set it correctly or you'll allow spurious
		// identities through. Document.
		if i := strings.Index(xff, ","); i > 0 {
			xff = strings.TrimSpace(xff[:i])
		}
		return xff
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// authzIpCheck enforces the per-key allowlist against the caller's IP.
// Returns true if allowed (or no allowlist configured).
func authzKeyAllowsIP(store *apikeys.Store, k apikeys.Key, ip net.IP) bool {
	if len(k.AllowedIPs) == 0 {
		return true
	}
	if ip == nil {
		return false
	}
	for _, cidr := range k.AllowedIPs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// gRPCClientIP extracts a bare IP from peer metadata.
func gRPCClientIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

// whoFromCtx returns a readable identity for audit logs.
func whoFromCtx(ctx context.Context) string {
	k, ok := ctx.Value(authKeyContextKey{}).(apikeys.Key)
	if !ok {
		return "anonymous"
	}
	return k.Name
}

// bootstrap reports whether the broker is in one-time bootstrap mode: no keys,
// no open flag, a printed bootstrap token pending. All requests 401 except the
// bootstrap POST /apikeys itself.
func (a *auth) bootstrap() bool {
	return !a.allowOpen && a.store != nil && !a.store.HasAnyKey() && a.bootstrapToken != "" && !a.bootstrapUsed
}

func isPublic(path string) bool {
	for _, p := range routeTable.public {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func (a *auth) auditRefused(r *http.Request, reason string) {
	if a.audit == nil {
		return
	}
	_ = a.audit.Append(whoFromCtx(r.Context()), "authz-refused", r.Method+" "+r.URL.Path, reason)
}
