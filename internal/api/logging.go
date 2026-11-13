package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// LoggingMiddleware logs each HTTP request with method, path, status, and
// duration as JSON via slog. Register it on the admin mux in firnd.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		slog.Info("http", "method", r.Method, "path", r.URL.Path, "status", sw.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// LoggingUnaryInterceptor logs each gRPC call as JSON via slog. Hot data-plane
// methods (Produce/Fetch) are logged at debug; everything else at info.
func LoggingUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	start := time.Now()
	resp, err = handler(ctx, req)
	attrs := []any{"method", info.FullMethod, "code", status.Code(err).String(), "duration_ms", time.Since(start).Milliseconds()}
	switch info.FullMethod {
	case "/firn.v1.FirnService/Produce", "/firn.v1.FirnService/Fetch":
		slog.Debug("grpc", attrs...)
	default:
		slog.Info("grpc", attrs...)
	}
	return resp, err
}

// HardeningMiddleware adds conservative security headers to every response.
// Content-Security-Policy is default-src 'self' with no inline scripts or
// styles — the admin UI and JSON APIs serve no inline code. Health/metrics
// are excluded on purpose (telemetry consumers are not browsers).
func HardeningMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isHealthzOrMetricsPath(r.URL.Path) {
			h := w.Header()
			h.Set("Content-Security-Policy", "default-src 'self'; base-uri 'self'; frame-ancestors 'none'")
			h.Set("X-Frame-Options", "DENY")
			h.Set("X-Content-Type-Options", "nosniff")
			h.Set("Referrer-Policy", "no-referrer")
			h.Set("Cache-Control", "no-store")
			h.Set("Cross-Origin-Resource-Policy", "same-origin")
		}
		next.ServeHTTP(w, r)
	})
}

func isHealthzOrMetricsPath(p string) bool {
	switch p {
	case "/healthz", "/metrics":
		return true
	default:
		return false
	}
}
