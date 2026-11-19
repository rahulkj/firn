package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rahulkj/firn/internal/testutil"
	"github.com/rahulkj/firn/sdk/firngo"
)

func TestFileCheckpointStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "cp.json")
	s, err := NewFileCheckpointStore(p)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Load(ctx, "conn"); err != nil || got != "" {
		t.Fatalf("empty load: %q %v", got, err)
	}
	if err := s.Store(ctx, "conn", "42"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Load(ctx, "conn"); got != "42" {
		t.Fatalf("load: %q", got)
	}
	// Crash-safety: values persist across reopen.
	s2, err := NewFileCheckpointStore(p)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := s2.Load(ctx, "conn"); got != "42" {
		t.Fatalf("reopen load: %q", got)
	}
}

func TestValidName(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"ok.name-1_2", true}, {"", false}, {"has space", false}, {"slash/name", false}, {"null\x00x", false},
	} {
		if got := ValidName(tc.in); got != tc.want {
			t.Errorf("ValidName(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestRunnerProducesAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "outbox.ndjson")
	if err := os.WriteFile(srcPath, []byte("line-one\nline-two\npartial-no-newline"), 0o600); err != nil {
		t.Fatal(err)
	}
	addr := testutil.StartBroker(t, map[string]int{"inbox": 2, "inbox-dlq": 1})
	ctx := context.Background()
	client, err := firngo.DialInsecure(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	conn, err := NewTailFileConnector("outbox", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	cp, err := NewFileCheckpointStore(filepath.Join(dir, "cp.json"))
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{Connector: conn, Client: client, Topic: "inbox", DLQ: "inbox-dlq"}
	if err := r.RunUntilEmpty(ctx, cp); err != nil {
		t.Fatal(err)
	}
	pos, err := cp.Load(ctx, "outbox")
	if err != nil {
		t.Fatal(err)
	}
	if pos == "" {
		t.Fatal("checkpoint empty after drain")
	}
	// Complete the partial line; the connector must pick it up on the next drain.
	f, err := os.OpenFile(srcPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("\nline-three\n"); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := r.RunUntilEmpty(ctx, cp); err != nil {
		t.Fatal(err)
	}
	// Verify broker content: 3 records across 2 partitions.
	total := 0
	for p := int32(0); p < 2; p++ {
		recs, err := client.Fetch(ctx, "inbox", p, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		total += len(recs)
	}
	if total != 4 {
		t.Fatalf("broker holds %d records, want 4 (line-one, line-two, completed partial, line-three)", total)
	}
	// Restart simulation: fresh runner on same checkpoint must not replay.
	conn2, err := NewTailFileConnector("outbox", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn2.Close() }()
	r2 := &Runner{Connector: conn2, Client: client, Topic: "inbox", DLQ: "inbox-dlq"}
	if err := r2.RunUntilEmpty(ctx, cp); err != nil {
		t.Fatal(err)
	}
	total = 0
	for p := int32(0); p < 2; p++ {
		recs, err := client.Fetch(ctx, "inbox", p, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		total += len(recs)
	}
	if total != 4 {
		t.Fatalf("after restart broker holds %d records, want 4 (no replay)", total)
	}
}

func TestTailFileTruncateReReads(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(srcPath, []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	conn, err := NewTailFileConnector("app", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := conn.FetchBatch(ctx, "", 10)
	if err != nil || len(recs) != 2 {
		t.Fatalf("initial: %v %v", recs, err)
	}
	last := recs[len(recs)-1].Position
	if err := os.WriteFile(srcPath, []byte("c\n"), 0o600); err != nil {
		t.Fatal(err) // truncate + rewrite shorter content
	}
	recs, err = conn.FetchBatch(ctx, last, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || string(recs[0].Value) != "c" {
		t.Fatalf("after truncate: %+v", recs)
	}
}

func TestDLQPayloadShape(t *testing.T) {
	payload := DLQPayload{Connector: "c", Stage: "produce", Position: "9", Error: "boom", Headers: map[string]string{"k": "v"}, ValueBase64: "QQ=="}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	want := `"connector":"c"`
	if !strings.Contains(string(b), want) {
		t.Fatalf("payload missing %q: %s", want, b)
	}
}

func ExampleRunner() {
	fmt.Println("firn-ingest -firn localhost:9461 -source-file outbox.ndjson -topic inbox")
	// Output: firn-ingest -firn localhost:9461 -source-file outbox.ndjson -topic inbox
}
