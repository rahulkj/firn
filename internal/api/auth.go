package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/rahulkj/slipway/internal/apikeys"
	"github.com/rahulkj/slipway/internal/audit"
	"github.com/rahulkj/slipway/internal/ratelimit"

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
	audit   *audit.Store
}

// NewAuthorizer returns an authenticator wired to these dependencies. All are
// optional: nil store => open (no authentication required).
func NewAuthorizer(store *apikeys.Store, limiter *ratelimit.Bucket, audit *audit.Store) *auth {
	return &auth{store: store, limiter: limiter, audit: audit}
}

// needsAuth reports whether a request must be authenticated.
type routeRole struct {
	roles  map[string]apikeys.Role // pattern -> min role
	public []string                // path prefixes that need no key
}

var routeTable = routeRole{
	public: []string{"/healthz", "/metrics"},
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

// Wrap applies authentication and per-key rate limiting to next. If the
// store is nil, requests pass through with an anonymous viewer identity.
func (a *auth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == r.Header.Get("Authorization") {
			token = "" // no Bearer prefix
		}
		if isPublic(r.URL.Path) || !a.enforced() {
			openCtx := context.WithValue(r.Context(), authKeyContextKey{}, apikeys.Key{Name: "anonymous", Role: "admin"})
			next.ServeHTTP(w, r.WithContext(openCtx))
			return
		}
		if token == "" {
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
		minRole, needs := roleFor(r.Method, r.URL.Path)
		if needs {
			rr, _ := apikeys.ParseRole(key.Role)
			if rr < minRole {
				a.auditRefused(r, "insufficient role")
				writeError(w, http.StatusForbidden, "insufficient role")
				return
			}
		}
		ctx := context.WithValue(r.Context(), authKeyContextKey{}, key)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Commands under this map require the listed role; everything else is viewer+.
var grpcRoles = map[string]apikeys.Role{
	"/slipway.v1.SlipwayService/Produce":      apikeys.RoleOperator,
	"/slipway.v1.SlipwayService/JoinGroup":    apikeys.RoleOperator,
	"/slipway.v1.SlipwayService/Heartbeat":    apikeys.RoleOperator,
	"/slipway.v1.SlipwayService/Pull":         apikeys.RoleOperator,
	"/slipway.v1.SlipwayService/Ack":          apikeys.RoleOperator,
	"/slipway.v1.SlipwayService/CommitOffset": apikeys.RoleOperator,
	"/slipway.v1.ReplicaService/ReplicaPut":   apikeys.RoleOperator,
	"/slipway.v1.ReplicaService/ReplicaFetch": apikeys.RoleViewer,
}

// GRPCInterceptor enforces bearer authentication and rate limiting on every
// unary RPC. ReplicaService and SlipwayService both use it.
func (a *auth) GRPCInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if a.store == nil || !a.store.HasAnyKey() {
			return handler(ctx, req) // no auth configured or not yet enforced
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
		min, mapped := grpcRoles[info.FullMethod]
		if !mapped {
			min = apikeys.RoleViewer
		}
		r, err := apikeys.ParseRole(key.Role)
		if err != nil {
			min = apikeys.RoleViewer
		}
		if r < min {
			return nil, status.Error(codes.PermissionDenied, "insufficient role")
		}
		return handler(ctx, req)
	}
}

// whoFromCtx returns a readable identity for audit logs.
func whoFromCtx(ctx context.Context) string {
	k, ok := ctx.Value(authKeyContextKey{}).(apikeys.Key)
	if !ok {
		return "anonymous"
	}
	return k.Name
}

// enforced reports whether the authenticator requires keys for non-public
// paths. Open mode = no keys configured (single-node dev default).
func (a *auth) enforced() bool { return a.store != nil && a.store.HasAnyKey() }

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
