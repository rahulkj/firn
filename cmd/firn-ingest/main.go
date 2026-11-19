// firn-ingest is the entry point for Firn's ingress integrations
// (ADR-016, docs/plans/007-ingress-integrations.md). It serves the
// HTTP(S) and MQTT ingest gateways and runs source connectors (tail-file,
// Kafka, Postgres CDC, MongoDB change streams, DynamoDB Streams). Every
// connector talks to Firn through the client API only.
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
	sourceFile := flag.String("source-file", "", "run a tail-file source connector: path to an append-only file (NDJSON)")
	sourceKafka := flag.String("source-kafka", "", "path to a Kafka source config (JSON)")
	sourcePostgres := flag.String("source-postgres", "", "path to a Postgres CDC (logical replication) source config (JSON)")
	sourceMongo := flag.String("source-mongo", "", "path to a MongoDB change-stream source config (JSON)")
	sourceDynamoDB := flag.String("source-dynamodb", "", "path to a DynamoDB Streams source config (JSON)")
	validate := flag.String("validate", "", "validate a config file and exit (http|mqtt by content)")

	connectorName := flag.String("connector-name", "", "checkpoint identity for file/kafka/postgres/mongo/dynamodb sources (default: derived from type)")
	topic := flag.String("topic", "", "Firn topic for source connector records")
	firnAddr := flag.String("firn", "localhost:9461", "firnd client gRPC address")
	firnKeyEnv := flag.String("firn-key-env", "FIRN_API_KEY", "env var holding the Firn API key (ignored when nothing produces to Firn)")
	firnCAEnv := flag.String("firn-ca-env", "", "env var holding the path to a PEM CA bundle for the Firn dial (empty = system roots)")
	firnInsecure := flag.Bool("firn-insecure", false, "plaintext Firn dial (dev only; never for production)")
	dlqTopic := flag.String("dlq", "", "dead-letter topic for source connectors (optional)")
	checkpoint := flag.String("checkpoint", "", "checkpoint file path for source connectors (required)")
	metricsAddr := flag.String("metrics-addr", "", "if set, serve Prometheus metrics on this address (e.g. 127.0.0.1:9466)")

	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("firn-ingest ")

	if *validate != "" {
		mustValidate(*validate)
		return
	}

	sourceModes := []struct {
		set  bool
		kind string
	}{
		{*sourceFile != "", "source-file"},
		{*sourceKafka != "", "source-kafka"},
		{*sourcePostgres != "", "source-postgres"},
		{*sourceMongo != "", "source-mongo"},
		{*sourceDynamoDB != "", "source-dynamodb"},
	}
	sources := 0
	for _, s := range sourceModes {
		if s.set {
			sources++
		}
	}
	modes := sources
	if *httpCfg != "" {
		modes++
	}
	if *mqttCfg != "" {
		modes++
	}
	if modes != 1 {
		fmt.Fprintln(os.Stderr, `usage: exactly one mode is required

  firn-ingest -http http-ingest.json
  firn-ingest -mqtt mqtt-ingest.json
  firn-ingest -source-file outbox.ndjson -topic inbox -checkpoint ./state/cp.json [-connector-name name] [-dlq topic] [-metrics-addr addr]
  firn-ingest -source-kafka kafka.json      -topic inbox -checkpoint ./state/cp.json [-dlq topic]
  firn-ingest -source-postgres pg.json     -topic inbox -checkpoint ./state/cp.json [-dlq topic]
  firn-ingest -source-mongo mongo.json     -topic inbox -checkpoint ./state/cp.json [-dlq topic]
  firn-ingest -source-dynamodb ddb.json    -topic inbox -checkpoint ./state/cp.json [-dlq topic]

Source configs (JSON):
  kafka:     {"brokers":["b:9092"],"topic":"src","start":"earliest","connector_name":"kafka-in"}
  postgres:  {"dsn_env":"PG_CDC_DSN","slot":"firn_cdc","connector_name":"pg-in"}
  mongo:     {"uri_env":"MONGO_URI","database":"app","collection":"events","full_document":"updateLookup","connector_name":"mongo-in"}
  dynamodb:  {"stream_arn":"arn:aws:dynamodb:...","region":"us-east-1","endpoint":"","connector_name":"ddb-in"}`)
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

	default: // source connector modes
		cfg := readSourceConfig(*sourceFile, *sourceKafka, *sourcePostgres, *sourceMongo, *sourceDynamoDB)
		if *topic == "" {
			log.Fatal("-topic is required for source connectors")
		}
		if *checkpoint == "" {
			log.Fatal("-checkpoint is required for source connectors")
		}
		conn, err := buildSourceConnector(cfg)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		client, err := integration.DialFirn(ctx, fc)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		cp, err := integration.NewFileCheckpointStore(*checkpoint)
		if err != nil {
			log.Fatal(err)
		}
		r := &integration.Runner{Connector: conn, Client: client, Topic: *topic, DLQ: *dlqTopic, Logger: log.Printf}
		if *metricsAddr != "" {
			metrics := integration.NewMetrics("ingest")
			r.Metrics = metrics
			go serveMetrics(*metricsAddr, metrics.Handler(), log.Printf)
		}
		log.Printf("source connector %q -> topic %q (checkpoint %s)", conn.Name(), *topic, *checkpoint)
		if *connectorName != "" {
			log.Printf("note: -connector-name ignored (name taken from config: %q)", conn.Name())
		}
		if err := r.Run(ctx, cp); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
	}
}

// sourceConfig is the union of all source connector config files.
type sourceConfig struct {
	Type string `json:"-"`
	// shared
	ConnectorName string `json:"connector_name"`
	// kafka
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
	Start   string   `json:"start"`
	// postgres
	DSNEnv string `json:"dsn_env"`
	Slot   string `json:"slot"`
	// mongo
	URIEnv       string `json:"uri_env"`
	Database     string `json:"database"`
	Collection   string `json:"collection"`
	FullDocument string `json:"full_document"`
	// dynamodb
	StreamArn string `json:"stream_arn"`
	Region    string `json:"region"`
	Endpoint  string `json:"endpoint"`

	rawPath string
}

func readSourceConfig(file, kafka, postgres, mongo, dynamodb string) sourceConfig {
	var c sourceConfig
	var path string
	switch {
	case file != "":
		c.Type = "file"
		path = file
	case kafka != "":
		c.Type = "kafka"
		path = kafka
	case postgres != "":
		c.Type = "postgres"
		path = postgres
	case mongo != "":
		c.Type = "mongo"
		path = mongo
	case dynamodb != "":
		c.Type = "dynamodb"
		path = dynamodb
	default:
		log.Fatal("no source config selected")
	}
	c.rawPath = path
	if c.Type == "file" {
		// file mode: config IS the path to the NDJSON file; no JSON parsing.
		// The consumer wraps it with the flag flow instead; keep it simple:
		// for -source-file the "config" is not JSON, so handle inline.
		return c
	}
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	if err := json.Unmarshal(b, &c); err != nil {
		log.Fatalf("parse %s: %v", path, err)
	}
	return c
}

func buildSourceConnector(cfg sourceConfig) (integration.SourceConnector, error) {
	name := cfg.ConnectorName
	if name == "" {
		switch cfg.Type {
		case "kafka":
			name = "kafka-source"
		case "postgres":
			name = "postgres-cdc"
		case "mongo":
			name = "mongo-changes"
		case "dynamodb":
			name = "dynamodb-streams"
		case "file":
			name = "tail-source"
		}
	}
	switch cfg.Type {
	case "file":
		return integration.NewTailFileConnector(name, cfg.rawPath)
	case "kafka":
		return integration.NewKafkaSourceConnector(name, cfg.Brokers, cfg.Topic, cfg.Start)
	case "postgres":
		if cfg.DSNEnv == "" {
			return nil, fmt.Errorf("postgres source: dsn_env is required")
		}
		return integration.NewPostgresCDCConnector(name, cfg.DSNEnv, cfg.Slot)
	case "mongo":
		return integration.NewMongoChangeStreamConnector(name, cfg.URIEnv, cfg.Database, cfg.Collection, cfg.FullDocument)
	case "dynamodb":
		return integration.NewDynamoDBStreamsConnector(name, cfg.StreamArn, cfg.Region, cfg.Endpoint)
	default:
		return nil, fmt.Errorf("unknown source type %q", cfg.Type)
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
