// Package ingest implements Firn's ingress gateways (ADR-016): the HTTP(S)
// ingest endpoint and the MQTT publisher-facing listener. Gateways run as
// separate processes and produce into Firn through the client API only.
package ingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/sdk/firngo"
)

// HTTPConfig is the versioned configuration for the HTTP(S) ingest gateway.
type HTTPConfig struct {
	Firn struct {
		Addr      string `json:"addr"`        // e.g. "localhost:9461"
		APIKeyEnv string `json:"api_key_env"` // env var holding the Firn API key ("" = -dev broker)
		CAEnv     string `json:"ca_env"`      // env var with PEM CA bundle path ("" = system roots / insecure)
		Insecure  bool   `json:"insecure"`    // plaintext Firn dial (dev only)
	} `json:"firn"`
	Listen  string `json:"listen"`   // default :9465
	TLSCert string `json:"tls_cert"` // optional TLS on the ingest listener
	TLSKey  string `json:"tls_key"`
	// Auth: exactly one of BearerEnv or HMACEnv unless AllowAnonymous.
	BearerEnv      string `json:"bearer_env"` // env var with the required bearer token
	HMACEnv        string `json:"hmac_env"`   // env var with the HMAC-SHA256 secret (X-Firn-Signature-256)
	AllowAnonymous bool   `json:"allow_anonymous"`
	MaxBodyBytes   int64  `json:"max_body_bytes"` // default 1 MiB
	RatePerMinute  int    `json:"rate_per_minute"` // per-IP cap, default 120; 0 disables
	DLQTopic       string `json:"dlq_topic"`      // optional dead-letter topic for poison records
	Routes         []HTTPRoute `json:"routes"`
}

// HTTPRoute maps one path to one Firn topic.
type HTTPRoute struct {
	Name string `json:"name"`
	Path string `json:"path"` // exact path, e.g. "/ingest/orders"
	Topic string `json:"topic"`
	// KeyJSONPointer: RFC 6901-style pointer into a JSON object body
	// ("/order_id" or "order_id" both accepted). Empty = round-robin keying.
	KeyJSONPointer string `json:"key_json_pointer"`
	// KeyHeader: alternatively take the record key from this header.
	KeyHeader string `json:"key_header"`
}

// LoadHTTPConfig reads and validates the JSON config at path.
func LoadHTTPConfig(path string) (*HTTPConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ingest: read config: %w", err)
	}
	var cfg HTTPConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("ingest: parse config %s: %w", path, err)
	}
	return cfg.Validate()
}

// Validate checks the config and normalizes defaults.
func (c *HTTPConfig) Validate() (*HTTPConfig, error) {
	if c.Listen == "" {
		c.Listen = ":9465"
	}
	if c.Firn.Addr == "" {
		return nil, errors.New("ingest: firn.addr required")
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return nil, errors.New("ingest: tls_cert and tls_key must be set together")
	}
	if c.BearerEnv == "" && c.HMACEnv == "" && !c.AllowAnonymous {
		return nil, errors.New("ingest: refusing to start without auth: set bearer_env/hmac_env, or allow_anonymous (dev only)")
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.RatePerMinute == 0 {
		c.RatePerMinute = 120
	}
	seen := map[string]bool{}
	for i := range c.Routes {
		r := &c.Routes[i]
		if !integration.ValidName(r.Name) {
			return nil, fmt.Errorf("ingest: route %d: invalid name %q", i, r.Name)
		}
		if r.Path == "" || !strings.HasPrefix(r.Path, "/") {
			return nil, fmt.Errorf("ingest: route %q: path must start with /", r.Name)
		}
		if seen[r.Path] {
			return nil, fmt.Errorf("ingest: duplicate route path %q", r.Path)
		}
		seen[r.Path] = true
		if r.Topic == "" {
			return nil, fmt.Errorf("ingest: route %q: topic required", r.Name)
		}
		if r.KeyJSONPointer != "" && r.KeyHeader != "" {
			return nil, fmt.Errorf("ingest: route %q: key_json_pointer and key_header are mutually exclusive", r.Name)
		}
	}
	if len(c.Routes) == 0 {
		return nil, errors.New("ingest: at least one route required")
	}
	return c, nil
}

// FirnAPIKey resolves the configured Firn key from the environment.
func (c *HTTPConfig) FirnAPIKey() string {
	if c.Firn.APIKeyEnv == "" {
		return ""
	}
	return os.Getenv(c.Firn.APIKeyEnv)
}

// bearerOrHMAC enforces the configured auth. Constant-time comparisons.
type rateLimiter struct {
	mu      sync.Mutex
	windows map[string]*rateWindow
	perMin  int
}

type rateWindow struct {
	start time.Time
	count int
}

func (l *rateLimiter) allow(ip string) bool {
	if l.perMin <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	w, ok := l.windows[ip]
	now := time.Now()
	if !ok || now.Sub(w.start) >= time.Minute {
		l.windows[ip] = &rateWindow{start: now, count: 1}
		return true
	}
	w.count++
	return w.count <= l.perMin
}

// Server serves the HTTP ingest routes.
type Server struct {
	cfg    *HTTPConfig
	client *firngo.Client
	mux    *http.ServeMux
	limit  *rateLimiter
	token  string // resolved bearer (if configured)
	hmac   []byte // resolved HMAC secret (if configured)
	http   *http.Server
}

// NewServer builds the ingest server (does not listen).
func NewServer(cfg *HTTPConfig, client *firngo.Client) (*Server, error) {
	token, hmacKey := "", []byte(nil)
	if cfg.BearerEnv != "" {
		token = os.Getenv(cfg.BearerEnv)
		if token == "" {
			return nil, fmt.Errorf("ingest: %s is empty", cfg.BearerEnv)
		}
	}
	if cfg.HMACEnv != "" {
		hmacKey = []byte(os.Getenv(cfg.HMACEnv))
		if len(hmacKey) == 0 {
			return nil, fmt.Errorf("ingest: %s is empty", cfg.HMACEnv)
		}
	}
	s := &Server{cfg: cfg, client: client, mux: http.NewServeMux(), limit: &rateLimiter{windows: map[string]*rateWindow{}, perMin: cfg.RatePerMinute}, token: token, hmac: hmacKey}
	for _, r := range cfg.Routes {
		route := r
		s.mux.HandleFunc(route.Path, s.handleRoute(route))
	}
	s.http = &http.Server{
		Addr:              cfg.Listen,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
	return s, nil
}

// Handler exposes the root handler (used by tests with httptest).
func (s *Server) Handler() http.Handler { return s.mux }

// ListenAndServe serves until ctx is cancelled or the listener fails.
// If TLS is configured it serves HTTPS; otherwise plaintext (dev/internal).
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("ingest: listen %s: %w", s.cfg.Listen, err)
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutCtx)
	}()
	if s.cfg.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			return fmt.Errorf("ingest: load TLS: %w", err)
		}
		s.http.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		return s.http.ServeTLS(ln, "", "")
	}
	return s.http.Serve(ln)
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request, body []byte) bool {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if ip == "" {
		ip = r.RemoteAddr
	}
	if !s.limit.allow(ip) {
		http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
		return false
	}
	if s.cfg.AllowAnonymous {
		return true
	}
	if s.token != "" {
		got := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+s.token)) != 1 {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return false
		}
		return true
	}
	if s.hmac != nil {
		got := r.Header.Get("X-Firn-Signature-256")
		mac := hmac.New(sha256.New, s.hmac)
		_, _ = mac.Write(body)
		gotMac, err := hex.DecodeString(strings.TrimPrefix(got, "sha256="))
		if err != nil || !hmac.Equal(gotMac, mac.Sum(nil)) {
			http.Error(w, `{"error":"bad signature"}`, http.StatusUnauthorized)
			return false
		}
		return true
	}
	http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	return false
}

func (s *Server) handleRoute(route HTTPRoute) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"body too large or unreadable"}`, http.StatusRequestEntityTooLarge)
			return
		}
		if !s.authenticate(w, r, body) {
			return
		}
		key, kerr := recordKey(route, r, body)
		if kerr != nil {
			s.reject(w, r, body, route, kerr)
			return
		}
		partition, offset, perr := s.client.Produce(r.Context(), route.Topic, key, body)
		if perr == nil {
			writeJSON(w, http.StatusAccepted, map[string]any{"produce": "committed", "partition": partition, "offset": offset})
			return
		}
		// Retry once on transient-looking errors, then DLQ or 503.
		partition, offset, err2 := s.client.Produce(r.Context(), route.Topic, key, body)
		if err2 == nil {
			writeJSON(w, http.StatusAccepted, map[string]any{"produce": "committed", "partition": partition, "offset": offset})
			return
		}
		if s.cfg.DLQTopic != "" {
			payload, _ := json.Marshal(integration.DLQPayload{
				Connector: "http-ingest:" + route.Name,
				Stage:     "produce",
				Error:     perr.Error(),
				Headers:   map[string]string{"path": route.Path},
				ValueBase64: base64.StdEncoding.EncodeToString(body),
			})
			if _, _, derr := s.client.Produce(r.Context(), s.cfg.DLQTopic, key, payload); derr == nil {
				writeJSON(w, http.StatusAccepted, map[string]any{"produce": "dlq", "topic": s.cfg.DLQTopic})
				return
			}
		}
		http.Error(w, `{"error":"firn produce failed; retry the request"}`, http.StatusServiceUnavailable)
	}
}

func (s *Server) reject(w http.ResponseWriter, r *http.Request, body []byte, route HTTPRoute, cause error) {
	if s.cfg.DLQTopic != "" {
		payload, _ := json.Marshal(integration.DLQPayload{
			Connector:   "http-ingest:" + route.Name,
			Stage:       "validate",
			Error:       cause.Error(),
			Headers:     map[string]string{"path": route.Path},
			ValueBase64: base64.StdEncoding.EncodeToString(body),
		})
		if _, _, derr := s.client.Produce(r.Context(), s.cfg.DLQTopic, nil, payload); derr == nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": cause.Error(), "dlq": s.cfg.DLQTopic})
			return
		}
	}
	http.Error(w, fmt.Sprintf(`{"error":%q}`, cause.Error()), http.StatusUnprocessableEntity)
}

func recordKey(route HTTPRoute, r *http.Request, body []byte) ([]byte, error) {
	if route.KeyHeader != "" {
		v := r.Header.Get(route.KeyHeader)
		if v == "" {
			return nil, fmt.Errorf("missing key header %q", route.KeyHeader)
		}
		return []byte(v), nil
	}
	if route.KeyJSONPointer == "" {
		return nil, nil // round-robin
	}
	var obj any
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, fmt.Errorf("body is not JSON: %w", err)
	}
	ptr := route.KeyJSONPointer
	if !strings.HasPrefix(ptr, "/") {
		ptr = "/" + ptr
	}
	cur := obj
	for _, tok := range strings.Split(strings.TrimPrefix(ptr, "/"), "/") {
		tok = strings.ReplaceAll(tok, "~1", "/")
		tok = strings.ReplaceAll(tok, "~0", "~")
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("key pointer %q: not an object at %q", ptr, tok)
		}
		cur, ok = m[tok]
		if !ok {
			return nil, fmt.Errorf("key pointer %q: missing field %q", ptr, tok)
		}
	}
	switch v := cur.(type) {
	case string:
		return []byte(v), nil
	case float64, bool:
		return []byte(fmt.Sprintf("%v", v)), nil
	default:
		return nil, fmt.Errorf("key pointer %q: field is not scalar", ptr)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
