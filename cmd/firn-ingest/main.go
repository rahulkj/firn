// firn-ingest is the entry point for Firn's ingress integrations
// (ADR-016, docs/plans/007-ingress-integrations.md). It serves the
// HTTP(S) and MQTT ingest gateways and runs reference source connectors.
// It is a separate process from firnd and talks to Firn only through the
// client API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rahulkj/firn/internal/ingest"
	"github.com/rahulkj/firn/internal/integration"
)

func main() {
	httpCfg := flag.String("http", "", "path to HTTP(S) ingest config (serve and block)")
	mqttCfg := flag.String("mqtt", "", "path to MQTT ingest config (serve and block)")
	validate := flag.String("validate", "", "validate a config file and exit (http|mqtt by extension of content)")

	tailSource := flag.String("source-file", "", "run a tail-file source connector: path to an append-only file (NDJSON)")
	tailName := flag.String("connector-name", "tail-source", "checkpoint identity for the tail connector")
	tailTopic := flag.String("topic", "", "Firn topic for tail-file records")

	firnAddr := flag.String("firn", "localhost:9461", "firnd client gRPC address")
	firnKeyEnv := flag.String("firn-key-env", "FIRN_API_KEY", "env var holding the Firn API key (ignored when nothing produces to Firn)")
	firnCAEnv := flag.String("firn-ca-env", "", "env var holding the path to a PEM CA bundle for the Firn dial (empty = system roots)")
	firnInsecure := flag.Bool("firn-insecure", false, "plaintext Firn dial (dev only; never for production)")
	dlqTopic := flag.String("dlq", "", "dead-letter topic for the tail connector (optional)")
	checkpoint := flag.String("checkpoint", "", "checkpoint file path for the tail connector (required)")
	metricsAddr := flag.String("metrics-addr", "", "if set, serve Prometheus metrics on this address (e.g. 127.0.0.1:9466)")

	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("firn-ingest ")

	if *validate != "" {
		mustValidate(*validate)
		return
	}

	modes := 0
	for _, set := range []bool{*httpCfg != "", *mqttCfg != "", *tailSource != ""} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		fmt.Fprintln(os.Stderr, "exactly one of -http, -mqtt, or -source-file is required\n\nExamples:\n  firn-ingest -http http-ingest.json\n  firn-ingest -mqtt mqtt-ingest.json\n  firn-ingest -source-file outbox.ndjson -topic inbox -checkpoint ./state/cp.json -firn localhost:9461")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fc := integration.FirnConn{Addr: *firnAddr, APIKey: os.Getenv(*firnKeyEnv), Insecure: *firnInsecure}
	if *firnCAEnv != "" {
		fc.CAFile = os.Getenv(*firnCAEnv)
	}

	switch {
	case *httpCfg != "":
		cfg, err := ingest.LoadHTTPConfig(*httpCfg)
		if err != nil {
			log.Fatal(err)
		}
		// Config's firn section wins over flags when set.
		if cfg.Firn.Addr != "" {
			fc.Addr = cfg.Firn.Addr
		}
		if cfg.Firn.APIKeyEnv != "" {
			fc.APIKey = os.Getenv(cfg.Firn.APIKeyEnv)
		}
		if cfg.Firn.CAEnv != "" {
			fc.CAFile = os.Getenv(cfg.Firn.CAEnv)
		}
		fc.Insecure = cfg.Firn.Insecure
		client, err := integration.DialFirn(ctx, fc)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		srv, err := ingest.NewServer(cfg, client)
		if err != nil {
			log.Fatal(err)
		}
		if *metricsAddr != "" {
			go serveMetrics(*metricsAddr, srv.Metrics.Handler(), log.Printf)
		}
		log.Printf("http ingest listening on %s (%d routes, dlq=%q)", cfg.Listen, len(cfg.Routes), cfg.DLQTopic)
		if err := srv.ListenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}

	case *mqttCfg != "":
		cfg, err := ingest.LoadMQTTConfig(*mqttCfg)
		if err != nil {
			log.Fatal(err)
		}
		if cfg.Firn.Addr != "" {
			fc.Addr = cfg.Firn.Addr
		}
		if cfg.Firn.APIKeyEnv != "" {
			fc.APIKey = os.Getenv(cfg.Firn.APIKeyEnv)
		}
		if cfg.Firn.CAEnv != "" {
			fc.CAFile = os.Getenv(cfg.Firn.CAEnv)
		}
		fc.Insecure = cfg.Firn.Insecure
		client, err := integration.DialFirn(ctx, fc)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		srv, err := ingest.NewMQTTServer(cfg, client)
		if err != nil {
			log.Fatal(err)
		}
		if *metricsAddr != "" {
			go serveMetrics(*metricsAddr, srv.Metrics.Handler(), log.Printf)
		}
		log.Printf("mqtt ingest listening on %s (%d routes, %d users, dlq=%q)", cfg.Listen, len(cfg.Routes), len(cfg.Users), cfg.DLQTopic)
		if err := srv.ListenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}

	case *tailSource != "":
		if *tailTopic == "" || *checkpoint == "" {
			log.Fatal("-topic and -checkpoint are required with -source-file")
		}
		client, err := integration.DialFirn(ctx, fc)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		conn, err := integration.NewTailFileConnector(*tailName, *tailSource)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		cp, err := integration.NewFileCheckpointStore(*checkpoint)
		if err != nil {
			log.Fatal(err)
		}
		r := &integration.Runner{Connector: conn, Client: client, Topic: *tailTopic, DLQ: *dlqTopic, Logger: log.Printf}
		if *metricsAddr != "" {
			tailMetrics := integration.NewMetrics("ingest")
			go serveMetrics(*metricsAddr, tailMetrics.Handler(), log.Printf)
		}
		log.Printf("tail connector %q: %s -> topic %q (checkpoint %s)", *tailName, *tailSource, *tailTopic, *checkpoint)
		if err := r.Run(ctx, cp); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
	}
}

func mustValidate(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	// Sniff: HTTP configs have "routes" with "path"; MQTT routes have "pattern".
	var probe struct {
		Routes []map[string]json.RawMessage `json:"routes"`
	}
	if err := json.Unmarshal(b, &probe); err != nil {
		log.Fatalf("invalid JSON: %v", err)
	}
	isMQTT := false
	for _, r := range probe.Routes {
		if _, ok := r["pattern"]; ok {
			isMQTT = true
		}
	}
	if isMQTT {
		cfg, err := ingest.LoadMQTTConfig(path)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("mqtt config OK: %d routes, listen %s\n", len(cfg.Routes), cfg.Listen)
		return
	}
	cfg, err := ingest.LoadHTTPConfig(path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("http config OK: %d routes, listen %s\n", len(cfg.Routes), cfg.Listen)
}

func serveMetrics(addr string, h http.Handler, logf func(string, ...any)) {
	logf("metrics listening on %s/metrics", addr)
	if err := http.ListenAndServe(addr, h); err != nil {
		logf("metrics listener failed: %v", err)
	}
}
