package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/egress"
	"github.com/rahulkj/firn/internal/testutil"
	"github.com/rahulkj/firn/sdk/firngo"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}


func baseConfig() *Config {
	cfg := &Config{Name: "test", Outputs: map[string]OutputDef{"out": {Type: "firn_topic", Topic: "out"}}}
	cfg.Input.Topic = "in"
	cfg.Input.Group = "test-group"
	cfg.DLQ = "test-dlq"
	return cfg
}

func TestProjectKeepDropRename(t *testing.T) {
	cfg := baseConfig()
	cfg.Stages = []Stage{
		{Type: "project", Keep: []string{"user.id", "user.name", "event"}, Rename: map[string]string{"user.name": "customer"}, Drop: []string{"event"}},
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	in := mustJSON(t, map[string]any{"user": map[string]any{"id": "u1", "name": "Ada", "pii": "secret"}, "event": "click", "extra": "x"})
	outs, err := cfg.Process(Record{Value: in}, NewStats(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 1 {
		t.Fatalf("outs=%d", len(outs))
	}
	got := map[string]any{}
	_ = json.Unmarshal(outs[0].Value, &got)
	if got["customer"] != "Ada" || got["user"].(map[string]any)["id"] != "u1" {
		t.Fatalf("unexpected doc: %v", got)
	}
	if _, ok := got["event"]; ok {
		t.Fatalf("event should be dropped: %v", got)
	}
	if _, ok := got["extra"]; ok {
		t.Fatalf("extra should be removed by keep: %v", got)
	}
}

func TestMapSetDefaultCast(t *testing.T) {
	cfg := baseConfig()
	cfg.Stages = []Stage{
		{Type: "map",
			Set:     map[string]any{"pipeline": "v1"},
			Default: map[string]any{"region": "eu"},
			Cast:    map[string]string{"total": "number", "active": "bool", "qty": "string"},
		},
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	in := mustJSON(t, map[string]any{"total": "42.5", "active": "true", "qty": 17})
	outs, err := cfg.Process(Record{Value: in}, NewStats(), "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	_ = json.Unmarshal(outs[0].Value, &got)
	if got["total"].(float64) != 42.5 || got["active"].(bool) != true || got["qty"].(string) != "17" {
		t.Fatalf("cast results wrong: %v", got)
	}
	if got["region"] != "eu" || got["pipeline"] != "v1" {
		t.Fatalf("set/default wrong: %v", got)
	}
}

func TestFilterDropsAndCounts(t *testing.T) {
	cfg := baseConfig()
	cfg.Stages = []Stage{
		{Type: "filter", Field: "kind", Op: "eq", Value: "keep"},
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	stats := NewStats()
	outs, err := cfg.Process(Record{Value: mustJSON(t, map[string]any{"kind": "drop"})}, stats, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 0 || stats.Filtered != 1 {
		t.Fatalf("outs=%v filtered=%d", outs, stats.Filtered)
	}
}

func TestRouteFanout(t *testing.T) {
	cfg := &Config{Name: "fanout",
		Outputs: map[string]OutputDef{
			"hot":  {Type: "firn_topic", Topic: "hot"},
			"cold": {Type: "firn_queue", Queue: "cold"},
		}}
	cfg.Input.Topic = "in"
	cfg.DLQ = "dlq"
	cfg.Stages = []Stage{
		{Type: "route", Rules: []RouteRule{
			{When: Predicate{Field: "sev", Op: "in", Value: []any{"crit", "high"}}, Output: "hot"},
		}, DefaultRoute: "cold"},
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	stats := NewStats()
	outs, err := cfg.Process(Record{Value: mustJSON(t, map[string]any{"sev": "crit"})}, stats, "")
	if err != nil || len(outs) != 1 || outs[0].Output != "hot" {
		t.Fatalf("crit route: %v %v", outs, err)
	}
	outs, err = cfg.Process(Record{Value: mustJSON(t, map[string]any{"sev": "low"})}, stats, "")
	if err != nil || len(outs) != 1 || outs[0].Output != "cold" {
		t.Fatalf("low route: %v %v", outs, err)
	}
}

func TestValidationRouteLast(t *testing.T) {
	cfg := baseConfig()
	cfg.Outputs = map[string]OutputDef{"a": {Type: "firn_topic", Topic: "a"}, "b": {Type: "firn_topic", Topic: "b"}}
	cfg.Stages = []Stage{
		{Type: "route", Rules: []RouteRule{{When: Predicate{Field: "x", Op: "exists"}, Output: "a"}}},
		{Type: "project", Keep: []string{"x"}},
	}
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("route not in last position must be rejected")
	}
	cfg.Stages = cfg.Stages[:1]
	if _, err := cfg.Validate(); err != nil {
		t.Fatalf("valid route config rejected: %v", err)
	}
}

func TestMultipleOutputsRequireRoute(t *testing.T) {
	cfg := baseConfig()
	cfg.Outputs = map[string]OutputDef{"a": {Type: "firn_topic", Topic: "a"}, "b": {Type: "firn_topic", Topic: "b"}}
	cfg.Stages = []Stage{{Type: "map", Set: map[string]any{"x": "y"}}}
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("multiple outputs without route must be rejected")
	}
}

func TestEnrichJoinsFromJSONL(t *testing.T) {
	ref := filepath.Join(t.TempDir(), "ref.jsonl")
	if err := os.WriteFile(ref, []byte("{\"id\":\"u1\",\"plan\":\"pro\",\"tier\":7}\n{\"id\":\"u2\",\"plan\":\"free\",\"tier\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig()
	cfg.Stages = []Stage{
		{Type: "enrich", Enrich: &EnrichSpec{KeyPath: "user_id", SourceFile: ref, MatchField: "id", Fields: map[string]string{"plan": "plan", "tier": "tier"}}},
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	outs, err := cfg.Process(Record{Value: mustJSON(t, map[string]any{"user_id": "u1"})}, NewStats(), "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	_ = json.Unmarshal(outs[0].Value, &got)
	if got["plan"] != "pro" || got["tier"].(float64) != 7 {
		t.Fatalf("enrich failed: %v", got)
	}
	// Missing key → passthrough unchanged.
	outs, err = cfg.Process(Record{Value: mustJSON(t, map[string]any{"other": 1})}, NewStats(), "")
	if err != nil {
		t.Fatal(err)
	}
	if string(outs[0].Value) == "" {
		t.Fatal("passthrough produced empty output")
	}
}

func TestAIStageMockProvider(t *testing.T) {
	defer func() { MockHandler = nil }()
	MockHandler = func(prompt string) (map[string]any, error) {
		if !strings.Contains(prompt, "sanitize this ticket") {
			return nil, fmt.Errorf("unexpected prompt")
		}
		return map[string]any{"category": "bug", "priority": "high"}, nil
	}
	cfg := baseConfig()
	cfg.Stages = []Stage{
		{Type: "ai", AI: &AISpec{
			Provider: "mock",
			Prompt:   "sanitize this ticket: {{payload}}",
			OutputContract: map[string]string{"category": "string", "priority": "string"},
			OnInvalid:      "dlq",
			MaxPerMinute:   1000,
			DailyBudget:    100,
		}},
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	stats := NewStats()
	outs, err := cfg.Process(Record{Value: mustJSON(t, map[string]any{"text": "my screen is blue"})}, stats, "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	_ = json.Unmarshal(outs[0].Value, &got)
	if got["category"] != "bug" || got["priority"] != "high" {
		t.Fatalf("ai merge failed: %v", got)
	}
	if stats.AICalls != 1 {
		t.Fatalf("AICalls = %d", stats.AICalls)
	}
	// Invalid contract output → error (runner DLQs it).
	MockHandler = func(prompt string) (map[string]any, error) {
		return map[string]any{"category": 123}, nil
	}
	stats = NewStats()
	_, err = cfg.Process(Record{Value: mustJSON(t, map[string]any{"text": "x"})}, stats, "")
	if err == nil {
		t.Fatal("invalid contract output must error")
	}
	if stats.AIInvalid != 1 {
		t.Fatalf("AIInvalid = %d", stats.AIInvalid)
	}
}

func TestEndToEndProjectPipeline(t *testing.T) {
	baddr := testutil.StartBroker(t, map[string]int{"raw": 2, "clean": 2, "pipeline-dlq": 1})
	client, err := firngo.DialInsecure(context.Background(), baddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	inputs := []map[string]any{
		{"user_id": "u1", "email": "a@example.com", "amount": 100, "secret": "drop-me"},
		{"user_id": "u2", "email": "b@example.com", "amount": 250, "secret": "drop-me"},
		{"user_id": "u3", "email": "c@example.com", "amount": 75, "secret": "drop-me"},
	}
	for _, in := range inputs {
		if _, _, err := client.Produce(context.Background(), "raw", nil, mustJSON(t, in)); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &Config{Name: "demo", DLQ: "pipeline-dlq",
		Outputs: map[string]OutputDef{"out": {Type: "firn_topic", Topic: "clean"}}}
	cfg.Input.Topic = "raw"
	cfg.Input.Group = "e2e-test-group"
	cfg.Stages = []Stage{
		{Type: "project", Keep: []string{"user_id", "email", "amount"}},
		{Type: "map", Set: map[string]any{"pipeline_source": "demo"}},
	}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	src, err := egress.NewTopicSource(context.Background(), client, "raw", nil, cfg.Input.Group)
	if err != nil {
		t.Fatal(err)
	}
	stats := NewStats()
	runner := &Runner{Config: cfg, Src: src, Client: client, Stats: stats, Logger: t.Logf}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		total := 0
		for p := int32(0); p < 2; p++ {
			recs, err := client.Fetch(context.Background(), "clean", p, 0, 10)
			if err == nil {
				total += len(recs)
			}
		}
		if total == len(inputs) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forwarded %d outputs, want %d", total, len(inputs))
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Check field projection happened.
	for p := int32(0); p < 2; p++ {
		recs, _ := client.Fetch(context.Background(), "clean", p, 0, 10)
		for _, r := range recs {
			var d map[string]any
			_ = json.Unmarshal(r.Value, &d)
			if _, ok := d["secret"]; ok {
				t.Fatalf("secret field leaked into output: %v", d)
			}
			if d["pipeline_source"] != "demo" {
				t.Fatalf("map stage missing: %v", d)
			}
		}
	}
}

func TestResolvedInputs(t *testing.T) {
	cfg := &Config{Name: "m"}
	cfg.Inputs = []InputDef{{Topic: "a"}, {Queue: "q"}}
	cfg.Outputs = map[string]OutputDef{"out": {Type: "firn_topic", Topic: "out"}}
	cfg.DLQ = "dlq"
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	got := cfg.ResolvedInputs()
	if len(got) != 2 {
		t.Fatalf("inputs=%d", len(got))
	}
	if got[0].Group != "firn-pipeline-m-0" || got[1].Group != "firn-pipeline-m-1" {
		t.Fatalf("default groups wrong: %+v", got)
	}
	// Single-input mode keeps the legacy field working.
	cfg = &Config{Name: "s"}
	cfg.Input.Topic = "in"
	cfg.Outputs = map[string]OutputDef{"out": {Type: "firn_topic", Topic: "out"}}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	got = cfg.ResolvedInputs()
	if len(got) != 1 || got[0].Group != "firn-pipeline-s" {
		t.Fatalf("single input wrong: %+v", got)
	}
}

func TestProcessInjectsSource(t *testing.T) {
	cfg := baseConfig()
	cfg.Stages = []Stage{{Type: "project", Keep: []string{"_source", "x"}}}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	outs, err := cfg.Process(Record{Value: mustJSON(t, map[string]any{"x": 1}), Source: "topic:raw-a"}, NewStats(), "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	_ = json.Unmarshal(outs[0].Value, &got)
	if got["_source"] != "topic:raw-a" {
		t.Fatalf("_source not injected: %v", got)
	}
}
