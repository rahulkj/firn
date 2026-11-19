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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rahulkj/firn/internal/egress"
	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/internal/pipeline"

)

func main() {
	cfgPath := flag.String("config", "", "path to pipeline config (pipeline.json)")
	validate := flag.Bool("validate", false, "validate config and exit")
	dryRun := flag.Bool("dry-run", false, "run stages over -sample records and print outputs; produces nothing")
	sample := flag.String("sample", "", "JSONL sample file for -dry-run")
	revision := flag.String("revision", "", "revision tag stamped into logs (default: config hash)")
	statsEvery := flag.Duration("stats-interval", 10*time.Second, "log pipeline stats at this interval (0 disables)")

	firnAddr := flag.String("firn", "localhost:9461", "firnd client gRPC address")
	firnKeyEnv := flag.String("firn-key-env", "FIRN_API_KEY", "env var holding the Firn API key")
	firnCAEnv := flag.String("firn-ca-env", "", "env var holding the path to a PEM CA bundle for the Firn dial")
	firnInsecure := flag.Bool("firn-insecure", false, "plaintext Firn dial (dev only)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("firn-pipeline ")

	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "usage: firn-pipeline -config pipeline.json [-validate|-dry-run -sample in.jsonl]")
		os.Exit(2)
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

	var src egress.Source
	if cfg.Input.Topic != "" {
		src, err = egress.NewTopicSource(ctx, client, cfg.Input.Topic, cfg.Input.Partitions, cfg.Input.Group)
	} else {
		src = egress.NewQueueSource(client, cfg.Input.Queue, 100, 30000)
	}
	if err != nil {
		log.Fatal(err)
	}
	stats := pipeline.NewStats()
	runner := &pipeline.Runner{Config: cfg, Src: src, Client: client, Revision: rev, Stats: stats, Logger: log.Printf}

	stopStats := make(chan struct{})
	if *statsEvery > 0 {
		go func() {
			t := time.NewTicker(*statsEvery)
			defer t.Stop()
			for {
				select {
				case <-stopStats:
					return
				case <-t.C:
					log.Printf("stats: in=%d filtered=%d unrouted=%d dlq=%d ai_calls=%d ai_invalid=%d circuit=%d", stats.In, stats.Filtered, stats.Unrouted, stats.DLQ, stats.AICalls, stats.AIInvalid, stats.AICircuitOpen)
				}
			}
		}()
	}
	defer close(stopStats)

	log.Printf("pipeline %q running (revision %s, input %s)", cfg.Name, rev, srcName(cfg))
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func srcName(cfg *pipeline.Config) string {
	if cfg.Input.Topic != "" {
		return "topic:" + cfg.Input.Topic
	}
	return "queue:" + cfg.Input.Queue
}
