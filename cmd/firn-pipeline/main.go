// firn-pipeline runs declarative data pipelines against Firn topics/queues
// (ADR-018, docs/plans/009-data-pipelines.md): field selection, mapping,
// enrichment, routing, and — opt-in — AI-assisted stages. It runs outside
// firnd and talks to Firn only through the client API. Pipelines are
// at-least-once; input offsets advance only after outputs are durable.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rahulkj/firn/internal/egress"
	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/internal/pipeline"

	"github.com/rahulkj/firn/sdk/firngo"
)

func main() {
	cfgPath := flag.String("config", "", "path to pipeline config (pipeline.json)")
	validate := flag.Bool("validate", false, "validate config and exit")
	dryRun := flag.Bool("dry-run", false, "run stages over -sample records and print outputs; produces nothing")
	sample := flag.String("sample", "", "JSONL sample file for -dry-run and -suggest-map")
	revision := flag.String("revision", "", "revision tag stamped into logs (default: config hash)")
	statsEvery := flag.Duration("stats-interval", 10*time.Second, "log pipeline stats at this interval (0 disables)")

	firnAddr := flag.String("firn", "localhost:9461", "firnd client gRPC address")
	firnKeyEnv := flag.String("firn-key-env", "FIRN_API_KEY", "env var holding the Firn API key")
	firnCAEnv := flag.String("firn-ca-env", "", "env var holding the path to a PEM CA bundle for the Firn dial")
	firnInsecure := flag.Bool("firn-insecure", false, "plaintext Firn dial (dev only)")
	metricsAddr := flag.String("metrics-addr", "", "if set, serve Prometheus metrics on this address")

	// suggest-map mode (design-time assistant, ADR-018 slice 5)
	suggestMap := flag.Bool("suggest-map", false, "read -sample JSONL, call the configured AI provider with -goal, print a proposed project/map fragment (human approves before use)")
	goal := flag.String("goal", "", "free-text description of the desired mapping (suggest-map)")
	provider := flag.String("provider", "openai_compatible", "AI provider for suggest-map: openai_compatible|mock")
	endpoint := flag.String("endpoint", "", "AI provider base URL (suggest-map)")
	model := flag.String("model", "", "model name (suggest-map)")
	lenientEndpoint := flag.Bool("insecure-http", false, "allow http:// provider endpoints (dev only)")
	aiKeyEnv := flag.String("ai-key-env", "FIRN_AI_KEY", "env var holding the AI provider API key (suggest-map)")

	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("firn-pipeline ")

	if *cfgPath == "" && !*suggestMap {
		fmt.Fprintln(os.Stderr, "usage: firn-pipeline -config pipeline.json [-validate|-dry-run -sample in.jsonl]\n       firn-pipeline -suggest-map -sample in.jsonl -goal \"...\" [-provider openai_compatible -endpoint URL -model M -ai-key-env K]")
		os.Exit(2)
	}

	if *suggestMap {
		if *goal == "" || *sample == "" {
			log.Fatal("-goal and -sample are required with -suggest-map")
		}
		b, err := os.ReadFile(*sample)
		if err != nil {
			log.Fatal(err)
		}
		first := ""
		for _, line := range strings.Split(string(b), "\n") {
			if strings.TrimSpace(line) != "" {
				first = strings.TrimSpace(line)
				break
			}
		}
		if first == "" {
			log.Fatal("sample file has no records")
		}
		ctxT, cancelT := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancelT()
		proposed, err := pipeline.SuggestMapping(ctxT, &pipeline.AISpec{
			Provider: *provider, Endpoint: *endpoint, Model: *model, APIKeyEnv: *aiKeyEnv,
			InsecureHTTP: *lenientEndpoint,
			TimeoutMs:    40000, OnInvalid: "fail", MaxPerMinute: 60, DailyBudget: 100,
		}, []byte(first), *goal)
		if err != nil {
			log.Fatal(err)
		}
		pretty, err := json.MarshalIndent(proposed, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Proposed pipeline fragment (review before use):\n%s\n", pretty)
		return
	}

	cfg, err := pipeline.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	rev := *revision
	if rev == "" {
		b, _ := os.ReadFile(*cfgPath)
		sum := sha256.Sum256(b)
		rev = hex.EncodeToString(sum[:])[:12]
	}
	if *validate {
		fmt.Printf("pipeline %q OK: %d stage(s), %d output(s), revision %s\n", cfg.Name, len(cfg.Stages), len(cfg.Outputs), rev)
		for i, s := range cfg.Stages {
			fmt.Printf("  stage %d: %s\n", i, s.Type)
		}
		return
	}
	if *dryRun {
		if *sample == "" {
			log.Fatal("-sample is required with -dry-run")
		}
		b, err := os.ReadFile(*sample)
		if err != nil {
			log.Fatal(err)
		}
		stats := pipeline.NewStats()
		n := 0
		for _, line := range strings.Split(string(b), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			n++
			outs, err := cfg.Process(pipeline.Record{Value: []byte(line), Source: "sample"}, stats, rev)
			if err != nil {
				fmt.Printf("record %d ERROR: %v\n", n, err)
				continue
			}
			for _, o := range outs {
				var pretty json.RawMessage = o.Value
				fmt.Printf("record %d -> output %q: %s\n", n, o.Output, pretty)
			}
		}
		fmt.Printf("dry-run complete: in=%d filtered=%d dlq=%d\n", stats.In, stats.Filtered, stats.DLQ)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fc := integration.FirnConn{Addr: *firnAddr, APIKey: os.Getenv(*firnKeyEnv), Insecure: *firnInsecure}
	if *firnCAEnv != "" {
		fc.CAFile = os.Getenv(*firnCAEnv)
	}
	client, err := integration.DialFirn(ctx, fc)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	metrics := integration.NewMetrics("pipeline")
	if *metricsAddr != "" {
		go func() {
			log.Printf("metrics listening on %s/metrics", *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, metrics.Handler()); err != nil {
				log.Printf("metrics listener failed: %v", err)
			}
		}()
	}

	inputs := cfg.ResolvedInputs()
	if len(inputs) <= 1 {
		src, err := buildInputSource(ctx, client, inputs[0])
		if err != nil {
			log.Fatal(err)
		}
		stats := pipeline.NewStats()
		runner := &pipeline.Runner{Config: cfg, Src: src, Client: client, Revision: rev, Stats: stats, Metrics: metrics, Logger: log.Printf}
		stopStats := make(chan struct{})
		defer close(stopStats)
		if *statsEvery > 0 {
			go statsLoop(ctx, stopStats, cfg.Name, stats, *statsEvery)
		}
		log.Printf("pipeline %q running (revision %s, input %s)", cfg.Name, rev, src.Name())
		if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
		return
	}

	// Multi-input: one runner per input; every input's records flow through
	// the same stage graph. Ordering across inputs is undefined. Each input
	// injects "_source" into the record doc (documented); correlation is via
	// enrich stages, never via cross-input ordering.
	var wg sync.WaitGroup
	for i, in := range inputs {
		in := in
		i := i
		src, err := buildInputSource(ctx, client, in)
		if err != nil {
			log.Fatalf("input %d (%s): %v", i, in.Topic+in.Queue, err)
		}
		stats := pipeline.NewStats()
		wg.Add(1)
		go func() {
			defer wg.Done()
			runner := &pipeline.Runner{Config: cfg, Src: src, Client: client, Revision: rev, Stats: stats, Metrics: metrics, Logger: log.Printf}
			log.Printf("pipeline %q input %d running (%s)", cfg.Name, i, src.Name())
			if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("input %d stopped with error: %v", i, err)
			}
		}()
	}
	wg.Wait()
}

func buildInputSource(ctx context.Context, client *firngo.Client, in pipeline.InputDef) (egress.Source, error) {
	if in.Topic != "" {
		return egress.NewTopicSource(ctx, client, in.Topic, in.Partitions, in.Group)
	}
	return egress.NewQueueSource(client, in.Queue, 100, 30000), nil
}

func statsLoop(ctx context.Context, stop <-chan struct{}, name string, stats *pipeline.Stats, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			log.Printf("stats(%s): in=%d filtered=%d unrouted=%d dlq=%d ai_calls=%d ai_invalid=%d circuit=%d", name, stats.In, stats.Filtered, stats.Unrouted, stats.DLQ, stats.AICalls, stats.AIInvalid, stats.AICircuitOpen)
		}
	}
}
