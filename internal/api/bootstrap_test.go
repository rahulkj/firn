package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/ratelimit"

	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/queue"
)

// newBootstrapFixture builds one node: in-memory broker, httptest admin
// server behind the secure authorizer, temp-dir key store.
type bootstrapFixture struct {
	admin *httptest.Server
	store *apikeys.Store
	auth  *auth
}

func newBootstrapFixture(t *testing.T, opts ...AuthOption) bootstrapFixture {
	t.Helper()
	dir := t.TempDir()
	reg := meta.NewRegistry()
	b := broker.New(reg)
	q, err := queue.NewManager(reg, b, "")
	if err != nil {
		t.Fatalf("queue manager: %v", err)
	}
	store, err := apikeys.OpenDir(dir)
	if err != nil {
		t.Fatalf("apikeys store: %v", err)
	}

	authorizer := NewAuthorizer(store, ratelimit.New(100, 100), nil, opts...)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","version":"test"}`))
	})
	mux.Handle("/metrics", authorizer.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("# stub\n"))
	})))
	mux.Handle("/", authorizer.Wrap(NewAdminHandler(reg, b.Groups(), q, WithAPIKeys(store))))

	admin := httptest.NewServer(mux)
	t.Cleanup(admin.Close)

	return bootstrapFixture{admin: admin, store: store, auth: authorizer}
}

func (f bootstrapFixture) req(t *testing.T, method, path, body, key string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, f.admin.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func (f bootstrapFixture) mintViaBootstrap(t *testing.T, token string) string {
	t.Helper()
	code, resp := f.req(t, "POST", "/apikeys", `{"name":"root","role":"viewer"}`, token)
	if code != http.StatusCreated {
		t.Fatalf("bootstrap mint: want 201, got %d body=%v", code, resp)
	}
	if got := resp["role"]; got != "admin" {
		t.Fatalf("bootstrap key role coerced to admin, got %v", got)
	}
	keyValue, _ := resp["key"].(string)
	if keyValue == "" {
		t.Fatalf("no key in bootstrap response: %v", resp)
	}
	return keyValue
}

func TestSecureByDefaultBootstrapFlow(t *testing.T) {
	token := "bootstrap-token-for-test"
	f := newBootstrapFixture(t, WithBootstrapToken(token))

	// 1. /healthz is the only anonymous endpoint.
	code, _ := f.req(t, "GET", "/healthz", "", "")
	if code != http.StatusOK {
		t.Fatalf("healthz: want 200, got %d", code)
	}

	// 2. Everything else is 401.
	for _, path := range []string{"/metrics", "/topics", "/apikeys", "/groups", "/ui/"} {
		code, _ := f.req(t, "GET", path, "", "")
		if code != http.StatusUnauthorized {
			t.Fatalf("%s without key: want 401, got %d", path, code)
		}
	}
	code, _ = f.req(t, "POST", "/topics", `{"name":"x","partitions":1}`, "")
	if code != http.StatusUnauthorized {
		t.Fatalf("topics POST without key: want 401, got %d", code)
	}

	// 3. Wrong bootstrap token rejected.
	code, _ = f.req(t, "POST", "/apikeys", `{"name":"nope","role":"viewer"}`, "wrong-token")
	if code != http.StatusUnauthorized {
		t.Fatalf("wrong bootstrap token: want 401, got %d", code)
	}

	// 4. Correct token mints exactly one admin key (role coerced).
	key := f.mintViaBootstrap(t, token)
	code, _ = f.req(t, "POST", "/apikeys", `{"name":"second","role":"admin"}`, token)
	if code != http.StatusUnauthorized {
		t.Fatalf("token replay: want 401, got %d", code)
	}

	// 5. Minted key works on everything.
	for _, path := range []string{"/metrics", "/topics"} {
		code, _ := f.req(t, "GET", path, "", key)
		if code != http.StatusOK {
			t.Fatalf("%s with key: want 200, got %d", path, code)
		}
	}
}

func TestSecureByDefaultOpenBootstrap(t *testing.T) {
	f := newBootstrapFixture(t, WithOpenBootstrap(true))
	for _, path := range []string{"/topics", "/metrics"} {
		code, _ := f.req(t, "GET", path, "", "")
		if code != http.StatusOK {
			t.Fatalf("open bootstrap %s: want 200, got %d", path, code)
		}
	}
}

func interceptProduce(authorizer *auth, ctx context.Context) (any, error) {
	return authorizer.GRPCInterceptor()(ctx, &struct{}{},
		&grpc.UnaryServerInfo{FullMethod: "/firn.v1.FirnService/Produce"},
		func(_ context.Context, _ any) (any, error) { return "ok", nil })
}

func incomingWithBearer(tok string) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.New(map[string]string{"authorization": "Bearer " + tok}))
}

func TestSecureByDefaultGGRPCGate(t *testing.T) {
	f := newBootstrapFixture(t, WithBootstrapToken("gg-token"))

	// Without a key: Unauthenticated in bootstrap mode.
	_, err := interceptProduce(f.auth, incomingWithBearer("nope"))
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unauthenticated {
		t.Fatalf("produce w/o key: want Unauthenticated, got %v", err)
	}
	_, err = interceptProduce(f.auth, incomingWithBearer(""))
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unauthenticated {
		t.Fatalf("produce w/o bearer: want Unauthenticated, got %v", err)
	}

	// Mint a key through the real HTTP path; use it on gRPC.
	key := f.mintViaBootstrap(t, "gg-token")
	_, err = interceptProduce(f.auth, incomingWithBearer(key))
	if err != nil {
		t.Fatalf("produce with bootstrap key: %v", err)
	}
}

func newBootstrapFixtureDir(t *testing.T, dir string, opts ...AuthOption) bootstrapFixture {
	t.Helper()
	reg := meta.NewRegistry()
	b := broker.New(reg)
	q, err := queue.NewManager(reg, b, "")
	if err != nil {
		t.Fatalf("queue manager: %v", err)
	}
	store, err := apikeys.OpenDir(dir)
	if err != nil {
		t.Fatalf("apikeys store: %v", err)
	}
	_ = b
	_ = q

	authorizer := NewAuthorizer(store, ratelimit.New(100, 100), nil, opts...)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/metrics", authorizer.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("# stub\n"))
	})))
	mux.Handle("/", authorizer.Wrap(NewAdminHandler(reg, b.Groups(), q, WithAPIKeys(store))))
	admin := httptest.NewServer(mux)
	t.Cleanup(admin.Close)
	return bootstrapFixture{admin: admin, store: store, auth: authorizer}
}

func TestSecureByDefaultRestartEnforced(t *testing.T) {
	// Restart with zero keys (fresh process, fresh store) — daemon prints a
	// bootstrap token and enforces until first key is minted via HTTP.
	dir := t.TempDir()
	f1 := newBootstrapFixtureDir(t, dir, WithBootstrapToken("tok-restart"))
	key := f1.mintViaBootstrap(t, "tok-restart")

	// After a restart the same store already contains a key; the new
	// process serves without offering any bootstrap token and enforces
	// bearer auth immediately.
	f2 := newBootstrapFixtureDir(t, dir) // no opts: simulates a normal restart with keys present
	code, _ := f2.req(t, "GET", "/topics", "", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("after restart without key: want 401, got %d", code)
	}
	code, _ = f2.req(t, "GET", "/topics", "", key)
	if code != http.StatusOK {
		t.Fatalf("after restart with key: want 200, got %d", code)
	}
}
