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
