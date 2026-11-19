package egress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------- Databricks ----------

func TestDatabricksSinkPutsJsonlThenManifest(t *testing.T) {
	var mu sync.Mutex
	puts := map[string][]byte{}
	auths := map[string]string{}
	paths := []string{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		puts[name] = body
		auths[name] = r.Header.Get("Authorization")
		paths = append(paths, name)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	t.Setenv("TEST_DBX_TOKEN", "dbx-token-123")
	batch := []Delivery{
		{Value: []byte(`{"a":1}`), Offset: 5, Partition: 0},
		{Value: []byte(`{"a":2}`), Offset: 6, Partition: 0},
	}
	r, err := newDatabricksSink(Route{Name: "dbx", Sink: SinkConfig{
		Type: "databricks", WorkspaceURL: ts.URL, VolumePath: "/Volumes/cat/schema/vol/out", TokenEnv: "TEST_DBX_TOKEN", // sdg:ignore test fixture: env var name, not a credential
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Write(context.Background(), "dbx", batch); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(puts) != 2 {
		t.Fatalf("expected 2 PUTs, got %v", paths)
	}
	if !strings.HasSuffix(paths[0], ".jsonl") || !strings.HasSuffix(paths[1], ".manifest.json") {
		t.Fatalf("order wrong (jsonl must precede manifest): %v", paths)
	}
	for name, auth := range auths {
		if auth != "Bearer dbx-token-123" {
			t.Fatalf("auth on %s = %q", name, auth)
		}
	}
	var manifest map[string]any
	if err := json.Unmarshal(puts[strings.TrimSuffix(paths[0], ".jsonl")+".jsonl.manifest.json"], &manifest); err != nil {
		t.Fatalf("manifest not JSON: %v", err)
	}
	if int(manifest["count"].(float64)) != 2 || int(manifest["first_offset"].(float64)) != 5 {
		t.Fatalf("manifest wrong: %v", manifest)
	}
	// JSONL decodes and round-trips values losslessly.
	lines := strings.Split(strings.TrimSpace(string(puts[paths[0]])), "\n")
	if len(lines) != 2 {
		t.Fatalf("jsonl lines = %d", len(lines))
	}
	var rec map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &rec)
	if rec["value_encoding"] != "base64" {
		t.Fatalf("not base64: %v", rec)
	}
}

func TestDatabricksSinkRetryableAndTerminal(t *testing.T) {
	for code, wantTerminal := range map[int]bool{http.StatusForbidden: true, http.StatusServiceUnavailable: false} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		r, err := newDatabricksSink(Route{Name: "dbx", Sink: SinkConfig{
			Type: "databricks", WorkspaceURL: ts.URL, VolumePath: "/Volumes/a/b/c", TokenEnv: "TEST_DBX_TOKEN2", // sdg:ignore test fixture: env var name, not a credential
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("TEST_DBX_TOKEN2", "tok")
		batch := []Delivery{{Value: []byte("x"), Offset: 0, Partition: 0}}
		err = r.Write(context.Background(), "dbx", batch)
		var te *TerminalError
		gotTerminal := err != nil && errorsAs(err, &te)
		if gotTerminal != wantTerminal {
			t.Fatalf("code %d: terminal=%v want=%v (err=%v)", code, gotTerminal, wantTerminal, err)
		}
		ts.Close()
	}
}

// ---------- S3 ----------

func newTestS3Sink(endpoint string, forcePath bool) *s3Sink {
	return &s3Sink{
		bucket:         "data",
		prefix:         "enrich",
		region:         "us-east-1",
		endpoint:       endpoint,
		forcePathStyle: forcePath,
		accessKey:      "AKIDEXAMPLE",
		secretKey:      "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", // sdg:ignore AWS documentation example key, not a real credential
		hc:             &http.Client{Timeout: 5 * time.Second},
		now:            func() time.Time { return time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC) },
	}
}

func TestS3SinkSignsAndPuts(t *testing.T) {
	var mu sync.Mutex
	var requests []struct {
		Method, Path, Query, Authorization, ContentSHA256, AmzDate, Body string
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, struct{ Method, Path, Query, Authorization, ContentSHA256, AmzDate, Body string }{
			r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"), r.Header.Get("x-amz-content-sha256"), r.Header.Get("x-amz-date"), string(body),
		})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<ETag>"abc"</ETag>`))
	}))
	defer ts.Close()

	s := newTestS3Sink(ts.URL, true) // path-style against the stub
	sink := Sink(s)
	batch := []Delivery{{Value: []byte("v"), Offset: 0, Partition: 0}, {Value: []byte("w"), Offset: 1, Partition: 0}}
	if err := sink.Write(context.Background(), "archive", batch); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("expected 2 PUTs (jsonl + manifest), got %d", len(requests))
	}
	for _, req := range requests {
		if req.Method != http.MethodPut {
			t.Fatalf("method = %s", req.Method)
		}
		if !strings.HasPrefix(req.Path, "/data/enrich/archive/") {
			t.Fatalf("path = %s (want /data/enrich/archive/...)", req.Path)
		}
		if !strings.HasPrefix(req.Authorization, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261119/us-east-1/s3/aws4_request") {
			t.Fatalf("Authorization = %s", req.Authorization)
		}
		if !strings.Contains(req.Authorization, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") {
			t.Fatalf("SignedHeaders = %s", req.Authorization)
		}
		if !strings.HasPrefix(req.Authorization, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261119/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=") {
			t.Fatalf("Authorization malformed: %s", req.Authorization)
		}
		if req.AmzDate != "20261119T120000Z" {
			t.Fatalf("x-amz-date = %q", req.AmzDate)
		}
		want := sha256.Sum256([]byte(req.Body))
		if req.ContentSHA256 != hex.EncodeToString(want[:]) {
			t.Fatalf("x-amz-content-sha256 mismatch")
		}
	}
	// Deterministic signature for fixed clock + input.
	s2 := newTestS3Sink("", false)
	r1, _ := s2.putObjectRequest(context.Background(), "k", []byte("payload"))
	s2.sign(r1, []byte("payload"), s2.now())
	s3 := newTestS3Sink("", false)
	r2, _ := s3.putObjectRequest(context.Background(), "k", []byte("payload"))
	s3.sign(r2, []byte("payload"), s3.now())
	if r1.Header.Get("Authorization") != r2.Header.Get("Authorization") {
		t.Fatal("signature not deterministic for fixed clock")
	}
	// Virtual-hosted style differs only in addressing.
	if !strings.HasPrefix(r1.URL.String(), "https://data.s3.us-east-1.amazonaws.com/k") {
		t.Fatalf("virtual-hosted URL wrong: %s", r1.URL)
	}
}

func TestS3SinkTerminalVsRetryable(t *testing.T) {
	for code, wantTerminal := range map[int]bool{http.StatusForbidden: true, http.StatusServiceUnavailable: false, http.StatusTooManyRequests: false} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		s := newTestS3Sink(ts.URL, true)
		batch := []Delivery{{Value: []byte("x"), Offset: 0, Partition: 0}}
		err := s.Write(context.Background(), "r", batch)
		var te *TerminalError
		gotTerminal := err != nil && errorsAs(err, &te)
		if gotTerminal != wantTerminal {
			t.Fatalf("code %d: terminal=%v want=%v (err=%v)", code, gotTerminal, wantTerminal, err)
		}
		ts.Close()
	}
}

