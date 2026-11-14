package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rahulkj/firn/internal/apikeys"
)

// Browser session auth for the read-only admin UI (ADR-013 Slice C,
// plan 005 slice 7). Browsers cannot attach an Authorization header to a
// top-level navigation, so /ui/* additionally accepts an HttpOnly,
// SameSite=Strict cookie minted by POST /ui/session in exchange for a
// bearer key (or an existing valid session — sliding renewal).
//
// Properties:
//   - Sessions are in-memory only; a restart signs every browser out.
//   - Every cookie verification re-checks the backing key against the key
//     store, so revoking a key invalidates its sessions immediately.
//   - Session tokens are stored SHA-256-hashed in the map.
//   - The cookie is scoped Path=/ui/ and is never sent to the admin JSON
//     API. Per-key rate limiting and allowed_ips allowlists apply to
//     cookie auth exactly as to bearer auth.
const (
	uiSessionCookieName = "firn_ui_session"
	uiSessionTTL        = 12 * time.Hour
)

type uiSession struct {
	keyName string
	expires time.Time
}

// uiSessionStore is the process-wide session table. Zero value is ready
// to use via newUISessionStore.
type uiSessionStore struct {
	mu       sync.Mutex
	sessions map[[sha256.Size]byte]uiSession
	now      func() time.Time
}

func newUISessionStore() *uiSessionStore {
	return &uiSessionStore{sessions: map[[sha256.Size]byte]uiSession{}, now: time.Now}
}

// create issues a new session token for keyName. The plaintext token is
// returned exactly once (to the caller's cookie); only its hash is kept.
func (s *uiSessionStore) create(keyName string) (string, time.Time) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is catastrophic and unrecoverable here;
		// an empty token can never verify, matching fail-closed behaviour.
		return "", time.Time{}
	}
	token := base64.RawURLEncoding.EncodeToString(buf)
	exp := s.now().Add(uiSessionTTL)
	s.mu.Lock()
	// GC expired entries so a long-lived process does not accumulate them.
	for k, v := range s.sessions {
		if !s.now().Before(v.expires) {
			delete(s.sessions, k)
		}
	}
	s.sessions[sha256.Sum256([]byte(token))] = uiSession{keyName: keyName, expires: exp}
	s.mu.Unlock()
	return token, exp
}

// verify resolves a session token to the backing key, re-checking that the
// key still exists and is not revoked. Returns ok=false for unknown,
// expired, or orphaned sessions (and prunes them).
func (s *uiSessionStore) verify(store *apikeys.Store, token string) (apikeys.Key, bool) {
	sum := sha256.Sum256([]byte(token))
	s.mu.Lock()
	ent, ok := s.sessions[sum]
	switch {
	case !ok:
		s.mu.Unlock()
		return apikeys.Key{}, false
	case !s.now().Before(ent.expires):
		delete(s.sessions, sum)
		s.mu.Unlock()
		return apikeys.Key{}, false
	}
	s.mu.Unlock()
	if store == nil {
		return apikeys.Key{}, false
	}
	return store.LookupByName(ent.keyName)
}

// revoke deletes the session for token, reporting whether one existed.
func (s *uiSessionStore) revoke(token string) bool {
	sum := sha256.Sum256([]byte(token))
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.sessions[sum]
	delete(s.sessions, sum)
	return ok
}

// isUIPath reports whether path belongs to the UI subtree (where cookie
// auth is accepted). Everything else stays bearer-only.
func isUIPath(path string) bool {
	return path == "/ui" || strings.HasPrefix(path, "/ui/")
}

func uiSessionSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	// Trusted first hop only (same model as clientIP with X-Forwarded-For);
	// firnd sits behind a TLS-terminating proxy in that layout.
	fp := r.Header.Get("X-Forwarded-Proto")
	if i := strings.Index(fp, ","); i >= 0 {
		fp = fp[:i]
	}
	return strings.EqualFold(strings.TrimSpace(fp), "https")
}

func uiSessionCookie(token string, maxAge int, secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     uiSessionCookieName,
		Value:    token,
		Path:     "/ui/",
		MaxAge:   maxAge,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   secure,
	}
}

// NewUISessionHandler returns the /ui/session endpoint: POST mints a
// session cookie for the authenticated caller (bearer, or an existing
// valid cookie as a sliding renewal); DELETE revokes the cookie's session.
// The handler must sit behind auth.Wrap (it reads the verified identity
// from the request context); Wrap accepts bearer or the session cookie.
func NewUISessionHandler(a *auth) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			postUISession(a, w, r)
		case http.MethodDelete:
			deleteUISession(a, w, r)
		default:
			w.Header().Set("Allow", "POST, DELETE")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func postUISession(a *auth, w http.ResponseWriter, r *http.Request) {
	// Open/dev mode: auth is disabled, a session cookie is meaningless.
	if a.allowOpen || a.store == nil {
		writeError(w, http.StatusBadRequest, "authentication disabled; no session needed")
		return
	}
	key, ok := r.Context().Value(authKeyContextKey{}).(apikeys.Key)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	// Federation keys are scoped to FederationService; they must not mint
	// browser sessions (or any admin-surface identity) here.
	if role, err := apikeys.ParseRole(key.Role); err == nil && role == apikeys.RoleFederation {
		if a.audit != nil {
			_ = a.audit.Append(key.Name, "ui-session-granted", "POST /ui/session", "refused: federation-scoped key")
		}
		writeError(w, http.StatusForbidden, "federation keys cannot mint UI sessions")
		return
	}
	token, exp := a.uiSessions.create(key.Name)
	if token == "" {
		writeError(w, http.StatusInternalServerError, "failed to mint session")
		return
	}
	http.SetCookie(w, uiSessionCookie(token, int(uiSessionTTL/time.Second), uiSessionSecure(r)))
	if a.audit != nil {
		_ = a.audit.Append(key.Name, "ui-session-granted", "POST /ui/session", "created; expires unix "+strconv.FormatInt(exp.Unix(), 10))
	}
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "session created", "expires_unix": exp.Unix()})
}

func deleteUISession(a *auth, w http.ResponseWriter, r *http.Request) {
	// Always expire the cookie in the response; revoking a missing session
	// is a no-op (idempotent logout).
	c, err := r.Cookie(uiSessionCookieName)
	if err == nil && c.Value != "" {
		// Name the revoked identity for the audit record when possible.
		who := "anonymous"
		if k, ok := a.uiSessions.verify(a.store, c.Value); ok {
			who = k.Name
		}
		if a.uiSessions.revoke(c.Value) {
			if a.audit != nil {
				_ = a.audit.Append(who, "ui-session-revoked", "DELETE /ui/session", "revoked")
			}
		}
	}
	http.SetCookie(w, uiSessionCookie("", -1, uiSessionSecure(r)))
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "revoked"})
}
