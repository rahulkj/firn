package egress

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"hash/crc32"
)

// Sink receives batches. Implementations must be safe for sequential use by
// one Runner goroutine. Write must be atomic per batch: either the whole
// batch becomes visible, or Write returns an error and nothing is visible.
type Sink interface {
	Write(ctx context.Context, route string, batch []Delivery) error
	// IsTerminal reports whether err is permanent (route to DLQ, don't retry).
	IsTerminal(err error) bool
	Close() error
}

// TerminalError wraps errors that must dead-letter the batch immediately.
type TerminalError struct{ Err error }

func (e *TerminalError) Error() string { return e.Err.Error() }
func (e *TerminalError) Unwrap() error { return e.Err }

// NewSink builds a sink from configuration.
func NewSink(r Route) (Sink, error) {
	switch r.Sink.Type {
	case "file":
		fi := r.Sink // copy includes dir
		return &FileSink{dir: fi.Dir}, nil
	case "http":
		return newHTTPSink(r)
	default:
		return nil, fmt.Errorf("egress: unknown sink %q", r.Sink.Type)
	}
}

// ---------- File sink ----------

// FileSink writes newline-delimited JSON to a directory. Each batch is split
// by source/partition and lands as one file per partition:
// {route}/{src}-p{partition}-{first}-{last}.jsonl written as "*.tmp",
// fsynced, then renamed; a manifest JSON records
// {route, source, partition, first_offset, last_offset, count, crc32c}. The
// rename is the commit point. Re-delivery of the same offset range produces
// the same filename and is skipped idempotently (existing final file with
// matching manifest is not rewritten), so duplicates from at-least-once
// retries vanish at this sink.
type FileSink struct {
	dir string
}

func (f *FileSink) Write(_ context.Context, route string, batch []Delivery) error {
	if len(batch) == 0 {
		return nil
	}
	// Split by source/partition so each file carries exactly one partition's
	// offset range (deterministic names → idempotent re-delivery).
	groups := map[string][]Delivery{}
	var order []string
	for _, d := range batch {
		src, part := sourceLabel(d)
		key := fmt.Sprintf("%s-p%d", src, part)
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], d)
	}
	for _, key := range order {
		if err := f.writePartition(route, key, groups[key]); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileSink) writePartition(route, key string, group []Delivery) error {
	src, part := sourceLabel(group[0])
	first := group[0].Offset
	last := group[len(group)-1].Offset
	base := filepath.Join(f.dir, route)
	name := fmt.Sprintf("%s-%d-%d.jsonl", key, first, last)
	final := filepath.Join(base, name)
	tmp := final + ".tmp"
	manifest := final + ".manifest.json"
	if st, err := os.Stat(final); err == nil && st.Size() > 0 {
		if _, err := os.Stat(manifest); err == nil {
			return nil // already committed
		}
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		return err
	}
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	w := io.MultiWriter(fh, h)
	md := map[string]any{
		"route":        route,
		"source":       src,
		"partition":    part,
		"first_offset": first,
		"last_offset":  last,
		"count":        len(group),
	}
	for _, d := range group {
		rec := map[string]any{"offset": d.Offset, "ts": time.Now().UnixNano(), "headers": d.Headers}
		if len(d.Key) > 0 {
			rec["key"] = base64.StdEncoding.EncodeToString(d.Key)
		}
		rec["key_encoding"] = "base64"
		// Preserve payload bytes losslessly.
		rec["value"] = base64.StdEncoding.EncodeToString(d.Value)
		rec["value_encoding"] = "base64"
		if d.DeliveryID != "" {
			rec["delivery_id"] = d.DeliveryID
		}
		line, err := json.Marshal(rec)
		if err != nil {
			_ = fh.Close()
			_ = os.Remove(tmp)
			return err
		}
		if _, err := w.Write(line); err != nil {
			_ = fh.Close()
			_ = os.Remove(tmp)
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			_ = fh.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := fh.Sync(); err != nil {
		_ = fh.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := fh.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	md["crc32c"] = fmt.Sprintf("%08x", h.Sum32())
	mbuf, _ := json.MarshalIndent(md, "", "  ")
	if err := os.WriteFile(manifest+".tmp", mbuf, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(manifest+".tmp", manifest); err != nil {
		return err
	}
	return nil
}

func (f *FileSink) IsTerminal(err error) bool {
	var te *TerminalError
	return errors.As(err, &te)
}

func (f *FileSink) Close() error { return nil }

func sourceLabel(d Delivery) (string, int32) {
	if d.DeliveryID != "" {
		return "queue", -1
	}
	return "topic", d.Partition
}

// ---------- HTTP sink ----------

type httpSink struct {
	url     string
	auth    string
	authEnv string
	hc      *http.Client
	timeout time.Duration
}

type httpRequestBody struct {
	Route       string           `json:"route"`
	Source      string           `json:"source"`
	Partition   int32            `json:"partition"`
	FirstOffset int64            `json:"first_offset"`
	LastOffset  int64            `json:"last_offset"`
	Records     []httpRecordJSON `json:"records"`
}

type httpRecordJSON struct {
	KeyB64   string            `json:"key_b64,omitempty"`
	ValueB64 string            `json:"value_b64"`
	Offset   int64             `json:"offset,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
}

func newHTTPSink(r Route) (Sink, error) {
	timeout := time.Duration(r.Sink.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &httpSink{
		url:     r.Sink.URL,
		auth:    r.Sink.Auth,
		authEnv: r.Sink.AuthEnv,
		hc:      &http.Client{Timeout: timeout},
		timeout: timeout,
	}, nil
}

func (s *httpSink) Write(ctx context.Context, route string, batch []Delivery) error {
	if len(batch) == 0 {
		return nil
	}
	src, part := sourceLabel(batch[0])
	body := httpRequestBody{Route: route, Source: src, Partition: part}
	if batch[0].DeliveryID == "" {
		body.FirstOffset = batch[0].Offset
		body.LastOffset = batch[len(batch)-1].Offset
	}
	for _, d := range batch {
		rc := httpRecordJSON{ValueB64: base64.StdEncoding.EncodeToString(d.Value), Offset: d.Offset, Headers: d.Headers}
		if len(d.Key) > 0 {
			rc.KeyB64 = base64.StdEncoding.EncodeToString(d.Key)
		}
		body.Records = append(body.Records, rc)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	parsed, err := url.Parse(s.url)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey(route, batch))
	switch s.auth {
	case "bearer_env":
		req.Header.Set("Authorization", "Bearer "+os.Getenv(s.authEnv))
	case "hmac_env":
		mac := hmac.New(sha256.New, []byte(os.Getenv(s.authEnv)))
		mac.Write(payload)
		req.Header.Set("X-Firn-Signature-256", hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
			return fmt.Errorf("egress: http sink timeout: %w", err) // retryable
		}
		return fmt.Errorf("egress: http sink: %w", err) // network error: retryable
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("egress: http sink status %d (retryable)", resp.StatusCode)
	default:
		return &TerminalError{Err: fmt.Errorf("egress: http sink status %d (terminal)", resp.StatusCode)}
	}
}

func (s *httpSink) IsTerminal(err error) bool {
	var te *TerminalError
	return errors.As(err, &te)
}

func (s *httpSink) Close() error { return nil }

func idempotencyKey(route string, batch []Delivery) string {
	var sb strings.Builder
	sb.WriteString(route)
	sb.WriteString(":")
	src, part := sourceLabel(batch[0])
	sb.WriteString(src)
	sb.WriteString(":")
	fmt.Fprintf(&sb, "%d:%d-%d", part, batch[0].Offset, batch[len(batch)-1].Offset)
	return sb.String()
}

