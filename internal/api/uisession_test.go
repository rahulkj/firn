package api

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/audit"
	"github.com/rahulkj/firn/internal/ratelimit"
)

// uiFixture wires an admin surface with the /ui subtree and the session
// endpoint behind the secure authorizer.
type uiFixture struct {
	admin *httptest.Server
	store *apikeys.Store
	auth  *auth
}

func newUIFixture(t *testing.T, limiter *ratelimit.Bucket, opts ...AuthOption) uiFixture {
	t.Helper()
	dir := t.TempDir()
	store, err := apikeys.OpenDir(dir)
	if err != nil {
		t.Fatalf("apikeys store: %v", err)
	}
	if limiter == nil {
		limiter = ratelimit.New(100, 100)
	}
	authorizer := NewAuthorizer(store, limiter, audit.NewMemStore(0), opts...)
	mux := http.NewServeMux()
	uiStub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ui page: " + r.URL.Path))
	})
	mux.Handle("/ui/", authorizer.Wrap(http.StripPrefix("/ui", uiStub)))
	mux.Handle("/ui/session", authorizer.Wrap(NewUISessionHandler(authorizer)))
	mux.Handle("/topics", authorizer.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"topics": []any{}})
	})))
	admin := httptest.NewServer(mux)
	t.Cleanup(admin.Close)
	return uiFixture{admin: admin, store: store, auth: authorizer}
}

func (f uiFixture) mintKey(t *testing.T, name, role string, allowedIPs ...string) string {
	t.Helper()
	k, err := f.store.Create(name, role, allowedIPs...)
	if err != nil {
		t.Fatalf("create key %s: %v", name, err)
	}
	return k
}

func (f uiFixture) do(t *testing.T, method, path, bearer string, cookie *http.Cookie, xff string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, f.admin.URL+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	resp, err := f.admin.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func (f uiFixture) bodyString(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(raw)
}

func sessionCookie(resp *http.Response) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == uiSessionCookieName {
			return c
		}
	}
	return nil
}

func TestUISessionFlow(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "reader", "viewer")

	// Unauthenticated POST is rejected.
	resp := f.do(t, http.MethodPost, "/ui/session", "", nil, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("POST without creds: want 401, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)

	// Bearer exchange mints a cookie with the expected attributes.
	resp = f.do(t, http.MethodPost, "/ui/session", key, nil, "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST with bearer: want 201, got %d (%s)", resp.StatusCode, f.bodyString(t, resp))
	}
	c := sessionCookie(resp)
	if c == nil {
		t.Fatal("no session cookie in response")
	}
	if !c.HttpOnly || c.Path != "/ui/" || c.SameSite != http.SameSiteStrictMode || c.MaxAge != 12*3600 {
		t.Fatalf("cookie attributes wrong: httpOnly=%v path=%q sameSite=%v maxAge=%d", c.HttpOnly, c.Path, c.SameSite, c.MaxAge)
	}
	if c.Secure {
		t.Fatal("Secure must not be set on a plain-HTTP response")
	}
	var grant map[string]any
	_ = json.Unmarshal([]byte(f.bodyString(t, resp)), &grant)
	if grant["status"] != "session created" {
		t.Fatalf("grant body: %v", grant)
	}

	// Cookie opens /ui/* ...
	resp = f.do(t, http.MethodGet, "/ui/", "", c, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /ui/ with cookie: want 200, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)

	// ... but is not accepted on the admin JSON API.
	resp = f.do(t, http.MethodGet, "/topics", "", c, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /topics with cookie: want 401, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)

	// Bearer still works on both surfaces.
	for _, path := range []string{"/ui/", "/topics"} {
		resp = f.do(t, http.MethodGet, path, key, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s with bearer: want 200, got %d", path, resp.StatusCode)
		}
		_ = f.bodyString(t, resp)
	}

	// Logout revokes the session.
	resp = f.do(t, http.MethodDelete, "/ui/session", "", c, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE /ui/session: want 200, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
	resp = f.do(t, http.MethodGet, "/ui/", "", c, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /ui/ after logout: want 401, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestUISessionRenewalWithCookie(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "reader", "viewer")

	resp := f.do(t, http.MethodPost, "/ui/session", key, nil, "")
	first := sessionCookie(resp)
	_ = f.bodyString(t, resp)

	// POST with the cookie (no bearer) slides the session forward.
	resp = f.do(t, http.MethodPost, "/ui/session", "", first, "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("renewal: want 201, got %d", resp.StatusCode)
	}
	second := sessionCookie(resp)
	_ = f.bodyString(t, resp)
	if second == nil || second.Value == first.Value {
		t.Fatal("renewal must issue a fresh token")
	}
}

func TestUISessionKeyRevokeKillsSession(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "contractor", "viewer")

	resp := f.do(t, http.MethodPost, "/ui/session", key, nil, "")
	c := sessionCookie(resp)
	_ = f.bodyString(t, resp)

	if err := f.store.Revoke("contractor"); err != nil {
		t.Fatalf("revoke key: %v", err)
	}
	resp = f.do(t, http.MethodGet, "/ui/", "", c, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /ui/ with revoked key's session: want 401, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestUISessionExpiry(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "reader", "viewer")

	fake := time.Now()
	f.auth.uiSessions.now = func() time.Time { return fake }

	resp := f.do(t, http.MethodPost, "/ui/session", key, nil, "")
	c := sessionCookie(resp)
	_ = f.bodyString(t, resp)

	fake = fake.Add(13 * time.Hour) // beyond the 12h TTL
	resp = f.do(t, http.MethodGet, "/ui/", "", c, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /ui/ with expired session: want 401, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestUISessionFederationKeyRefused(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "peer", "federation")

	resp := f.do(t, http.MethodPost, "/ui/session", key, nil, "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("federation key POST: want 403, got %d (%s)", resp.StatusCode, f.bodyString(t, resp))
	}
}

func TestUISessionAllowedIPsEnforced(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "office", "viewer", "192.0.2.0/24")

	resp := f.do(t, http.MethodPost, "/ui/session", key, nil, "192.0.2.9")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("mint from allowed IP: want 201, got %d", resp.StatusCode)
	}
	c := sessionCookie(resp)
	_ = f.bodyString(t, resp)

	resp = f.do(t, http.MethodGet, "/ui/", "", c, "192.0.2.10")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET from allowed IP: want 200, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)

	resp = f.do(t, http.MethodGet, "/ui/", "", c, "198.51.100.7")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET from disallowed IP: want 403, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestUISessionRateLimited(t *testing.T) {
	f := newUIFixture(t, ratelimit.New(1, 0)) // one token, no refill
	key := f.mintKey(t, "reader", "viewer")

	resp := f.do(t, http.MethodPost, "/ui/session", key, nil, "")
	c := sessionCookie(resp)
	_ = f.bodyString(t, resp)

	resp = f.do(t, http.MethodGet, "/ui/", "", c, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first cookie GET: want 200, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
	resp = f.do(t, http.MethodGet, "/ui/", "", c, "")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second cookie GET: want 429, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestUISessionOpenModeNeedsNoSession(t *testing.T) {
	f := newUIFixture(t, nil, WithOpenBootstrap(true))
	resp := f.do(t, http.MethodPost, "/ui/session", "", nil, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST in open mode: want 400, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
	// /ui/ is wide open in this mode.
	resp = f.do(t, http.MethodGet, "/ui/", "", nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /ui/ in open mode: want 200, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestUISessionCookieSecureOverTLS(t *testing.T) {
	dir := t.TempDir()
	store, err := apikeys.OpenDir(dir)
	if err != nil {
		t.Fatalf("apikeys store: %v", err)
	}
	authorizer := NewAuthorizer(store, ratelimit.New(100, 100), nil)
	mux := http.NewServeMux()
	mux.Handle("/ui/session", authorizer.Wrap(NewUISessionHandler(authorizer)))
	admin := httptest.NewTLSServer(mux)
	t.Cleanup(admin.Close)

	key, err := store.Create("reader", "viewer")
	if err != nil {
		t.Fatalf("create key: %v", err)
	}

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(http.MethodPost, admin.URL+"/ui/session", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST over TLS: want 201, got %d", resp.StatusCode)
	}
	for _, c := range resp.Cookies() {
		if c.Name == uiSessionCookieName && !c.Secure {
			t.Fatal("Secure must be set on a TLS response")
		}
	}
}

func TestUISessionDeleteWithoutSessionIsIdempotent(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "reader", "viewer")

	// No cookie: DELETE is a no-op 200 (and clears the cookie).
	resp := f.do(t, http.MethodDelete, "/ui/session", key, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE without cookie: want 200, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestUISessionWrongMethodRejected(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "reader", "viewer")
	resp := f.do(t, http.MethodGet, "/ui/session", key, nil, "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /ui/session: want 405, got %d", resp.StatusCode)
	}
	_ = f.bodyString(t, resp)
}

func TestBearerAllowedIPsEnforced(t *testing.T) {
	f := newUIFixture(t, nil)
	key := f.mintKey(t, "office", "viewer", "192.0.2.0/24")

	// Allowed IP: 200.
	resp := f.do(t, http.MethodGet, "/topics", key, nil, "192.0.2.9")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /topics from allowed IP: want 200, got %d", resp.StatusCode)
	}

	// Disallowed IP: 403 with ip-not-allowed refusal.
	resp = f.do(t, http.MethodGet, "/topics", key, nil, "198.51.100.7")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET /topics from disallowed IP: want 403, got %d", resp.StatusCode)
	}
}
