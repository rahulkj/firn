package api

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHardeningMiddlewareAddsHeaders(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, "<html>hello</html>")
	})
	req, err := http.NewRequest("GET", "http://x/ui/", nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingResponseWriter{header: http.Header{}, body: &strings.Builder{}, code: 0}
	HardeningMiddleware(next).ServeHTTP(rec, req)

	if got := rec.header.Get("Content-Security-Policy"); got != "default-src 'self'; base-uri 'self'; frame-ancestors 'none'" {
		t.Fatalf("CSP = %q", got)
	}
	for _, k := range []string{"X-Frame-Options", "X-Content-Type-Options", "Referrer-Policy", "Cache-Control"} {
		if rec.header.Get(k) == "" {
			t.Fatalf("header %s missing", k)
		}
	}
	if got := rec.header.Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("X-Frame-Options = %q", got)
	}
}

func TestHardeningMiddlewareSkipsHealthAndMetrics(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	for _, path := range []string{"/healthz", "/metrics"} {
		rec := &recordingResponseWriter{header: http.Header{}, body: &strings.Builder{}, code: 0}
		req, err := http.NewRequest("GET", "http://x"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		HardeningMiddleware(next).ServeHTTP(rec, req)
		if rec.header.Get("Content-Security-Policy") != "" || rec.header.Get("X-Frame-Options") != "" {
			t.Fatalf("%s unexpectedly hardened", path)
		}
	}
}

type recordingResponseWriter struct {
	header http.Header
	body   *strings.Builder
	code   int
}

func (w *recordingResponseWriter) Header() http.Header        { return w.header }
func (w *recordingResponseWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
func (w *recordingResponseWriter) WriteHeader(code int)        { w.code = code }
