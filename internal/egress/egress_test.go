package egress

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/testutil"
	"github.com/rahulkj/firn/sdk/firngo"
)

func TestFileSinkWritesManifestAndSkipsDuplicates(t *testing.T) {
	dir := t.TempDir()
	sink := &FileSink{dir: dir}
	batch := []Delivery{
		{Key: []byte("k1"), Value: []byte("v1"), Offset: 0, Partition: 0},
		{Key: []byte("k2"), Value: []byte("v2"), Offset: 1, Partition: 0},
	}
	if err := sink.Write(context.Background(), "route1", batch); err != nil {
		t.Fatal(err)
	}
	// Write the exact same batch again: must be a no-op.
	if err := sink.Write(context.Background(), "route1", batch); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "route1")
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	var jsonl, manifest int
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".jsonl"):
			jsonl++
		case strings.HasSuffix(e.Name(), ".manifest.json"):
			manifest++
		}
	}
	if jsonl != 1 || manifest != 1 {
		t.Fatalf("files: %d jsonl %d manifest, want 1+1", jsonl, manifest)
	}
	_ = entries
	// find the manifest with the same basename
	files, _ := os.ReadDir(base)
	var manifestName string
	for _, e := range files {
		if strings.HasSuffix(e.Name(), ".manifest.json") {
			manifestName = filepath.Join(base, e.Name())
		}
	}
	var md map[string]any
	mb, err := os.ReadFile(manifestName)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(mb, &md); err != nil {
		t.Fatal(err)
	}
	if int(md["count"].(float64)) != 2 {
		t.Fatalf("manifest count = %v", md["count"])
	}
}

func TestHTTPSinkPostsWithIdempotencyKey(t *testing.T) {
	var mu sync.Mutex
	var gotAuth, gotIdem string
	var payloads []httpRequestBody
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotAuth = r.Header.Get("Authorization")
		gotIdem = r.Header.Get("Idempotency-Key")
		var b httpRequestBody
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &b)
		payloads = append(payloads, b)
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

	t.Setenv("TEST_EGRESS_BEARER", "tok")
	r := Route{Name: "hook", Sources: []RouteSource{{Topic: "events"}}, Sink: SinkConfig{Type: "http", URL: ts.URL, Auth: "bearer_env", AuthEnv: "TEST_EGRESS_BEARER", TimeoutMs: 5000, InsecureHTTP: true}}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	batch := []Delivery{
		{Value: []byte("a"), Offset: 10, Partition: 1},
		{Value: []byte("b"), Offset: 11, Partition: 1},
	}
	if err := sink.Write(context.Background(), "hook", batch); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotAuth != "Bearer tok" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotIdem != "hook:topic:1:10-11" {
		t.Fatalf("Idempotency-Key = %q", gotIdem)
	}
	if len(payloads) != 1 || len(payloads[0].Records) != 2 {
		t.Fatalf("payloads = %+v", payloads)
	}
	if got := payloads[0].Records[0].ValueB64; got != base64.StdEncoding.EncodeToString([]byte("a")) {
		t.Fatalf("value encoding mismatch: %q", got)
	}
}

func TestHTTPSinkTerminalAndRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "bad") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()
	for path, wantTerminal := range map[string]bool{"/good": false, "/bad": true} {
		r := Route{Name: "r", Sink: SinkConfig{Type: "http", URL: ts.URL + path, TimeoutMs: 2000, InsecureHTTP: true}}
		sink, err := NewSink(r)
		if err != nil {
			t.Fatal(err)
		}
		batch := []Delivery{{Value: []byte("x"), Offset: 0, Partition: 0}}
		err = sink.Write(context.Background(), "r", batch)
		var te *TerminalError
		if got := err != nil && asTerminal(err, &te); got != wantTerminal {
			t.Fatalf("path %s: terminal=%v want=%v (err=%v)", path, got, wantTerminal, err)
		}
	}
}

func asTerminal(err error, te **TerminalError) bool {
	for err != nil {
		if e, ok := err.(*TerminalError); ok {
			*te = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestTopicRouteForwardsAndResumes(t *testing.T) {
	baddr := testutil.StartBroker(t, map[string]int{"iot": 2})
	dir := t.TempDir()
	client, err := firngo.DialInsecure(context.Background(), baddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	for i := 0; i < 7; i++ {
		if _, _, err := client.Produce(context.Background(), "iot", []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	r := Route{
		Name:      "archive",
		Sources:   []RouteSource{{Topic: "iot"}},
		Group:     "egress-archive-test",
		AckPolicy: "after_sink_confirm",
		DLQ:       "egress-dlq-test",
	}
	r.Sink = SinkConfig{Type: "file", Dir: dir}
	r.Batch.MaxRecords = 500
	r.Batch.FlushIntervalMs = 200
	r.Retry.Budget = 1
	r.Retry.BackoffMs = 10

	src, err := NewTopicSource(context.Background(), client, "iot", nil, r.Group)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Route: r, Src: src, Sink: sink, Client: client, Logger: t.Logf}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = runner.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		matches, _ := filepath.Glob(filepath.Join(dir, "archive", "*.jsonl"))
		total := 0
		for _, m := range matches {
			b, _ := os.ReadFile(m)
			total += strings.Count(string(b), "\n")
		}
		if total == 7 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("forwarded %d records, want 7", total)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Wait for the runner to confirm positions (at-least-once: commit may
	// lag the file write), then stop it.
	deadline = time.Now().Add(5 * time.Second)
	meta, err := client.TopicMetadata(context.Background(), "iot")
	if err != nil {
		t.Fatal(err)
	}
	for {
		sum := int64(0)
		allFound := true
		for p := int32(0); p < meta.Partitions; p++ {
			off, found, err := client.FetchOffset(context.Background(), r.Group, "iot", p)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				allFound = false
				break
			}
			sum += off
		}
		if allFound && sum == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("offsets not committed within deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestQueueRouteForwardsAndAcks(t *testing.T) {
	baddr := testutil.StartBrokerWithModes(t, map[string]testutil.TopicSpec{
		"jobs": {Partitions: 1, Mode: "queue"},
	})
	dir := t.TempDir()
	client, err := firngo.DialInsecure(context.Background(), baddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	for i := 0; i < 3; i++ {
		if _, _, err := client.Produce(context.Background(), "jobs", nil, []byte(fmt.Sprintf("job-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	r := Route{
		Name:      "jobs-archive",
		Sources:   []RouteSource{{Queue: "jobs"}},
		AckPolicy: "after_sink_confirm",
	}
	r.Sink = SinkConfig{Type: "file", Dir: dir}
	r.Batch.MaxRecords = 500
	r.Retry.Budget = 1
	r.Retry.BackoffMs = 10
	src := NewQueueSource(client, "jobs", 100, 30000)
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Route: r, Src: src, Sink: sink, Client: client, Logger: t.Logf}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		matches, _ := filepath.Glob(filepath.Join(dir, "jobs-archive", "*.jsonl"))
		total := 0
		for _, m := range matches {
			b, _ := os.ReadFile(m)
			total += strings.Count(string(b), "\n")
		}
		if total == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forwarded %d records, want 3", total)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Deliveries must be acked (queue drained).
	time.Sleep(100 * time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()
	ds, err := client.Pull(ctx2, "jobs", 10, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 0 {
		t.Fatalf("queue not drained: %d deliveries visible", len(ds))
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := &Config{}
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("empty config must fail")
	}
	cfg = &Config{Routes: []Route{{Name: "x", Sources: []RouteSource{{Topic: "t"}}, Sink: SinkConfig{Type: "file", Dir: t.TempDir()}}}}
	cfg.Firn.Addr = "localhost:9461"
	if _, err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// Unknown ack policy rejected.
	cfg.Routes[0].AckPolicy = "on_write"
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("on_write must be rejected")
	}
}
