// Package egress implements Firn's egress integrations (ADR-017,
// docs/plans/008-egress-integrations.md): forwarding selected topics/queues
// to external systems. Forwarders are separate processes consuming Firn
// through the client API; they never touch storage internals. Delivery is
// at-least-once; offsets/acks advance only after the sink confirms.
package egress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rahulkj/firn/internal/integration"
)

// Delivery is one record consumed from a Firn source.
type Delivery struct {
	Key        []byte
	Value      []byte
	Headers    map[string]string
	Offset     int64  // -1 for queue sources
	DeliveryID string // set for queue sources only
	Partition  int32  // -1 for queue sources
}

// Source is a batchable pull source bound to a route (topic partitions or a queue).
type Source interface {
	// Next blocks until at least one record is available or ctx is done.
	// Returned deliveries must be confirmed via Confirm after sink success.
	Next(ctx context.Context) ([]Delivery, error)
	// Confirm advances the source position (group commit / queue ack).
	Confirm(ctx context.Context, batch []Delivery) error
	Name() string
}

// Route is one forwarding rule (versioned JSON config).
type Route struct {
	Name string `json:"name"`
	// Sources: topic (partitions optional; default all, from metadata) and/or queue.
	Sources []RouteSource `json:"sources"`
	Group   string        `json:"group"` // consumer group for topic sources (default firn-egress-<name>)
	// AckPolicy is always "after_sink_confirm" in this implementation; any
	// other value is rejected at validation (see plan 008 slice 1 note:
	// "on_write" was folded away as redundant and confusing).
	AckPolicy string `json:"ack_policy"`
	Batch     struct {
		MaxRecords      int `json:"max_records"`       // default 500
		FlushIntervalMs int `json:"flush_interval_ms"` // default 1000
	} `json:"batch"`
	Sink      SinkConfig    `json:"sink"`
	Retry     RetryConfig   `json:"retry"`
	DLQ       string        `json:"dlq"` // dead-letter topic (optional; strongly recommended)
}

// RouteSource selects a Firn topic (with optional partitions) or one queue.
type RouteSource struct {
	Topic      string  `json:"topic"`
	Queue      string  `json:"queue"`
	Partitions []int32 `json:"partitions"`
}

// SinkConfig is a tagged union; Type selects the sink implementation.
type SinkConfig struct {
	Type string `json:"type"` // file | http

	// file sink
	Dir string `json:"dir"` // output directory (created 0700)

	// http sink
	URL             string `json:"url"`
	Auth            string `json:"auth"`             // none | bearer_env | hmac_env
	AuthEnv         string `json:"auth_env"`         // env var name for bearer token / HMAC secret
	TimeoutMs       int    `json:"timeout_ms"`       // default 10000
	InsecureHTTP    bool   `json:"insecure_http"`    // allow http:// (dev only)
	MaxBatchBytes   int    `json:"max_batch_bytes"`  // informational; not enforced

	// databricks sink (Files API over Unity Catalog Volumes)
	WorkspaceURL string `json:"workspace_url"` // https://dbc-<id>.cloud.databricks.com
	VolumePath   string `json:"volume_path"`   // /Volumes/<catalog>/<schema>/<volume>/<dir>
	TokenEnv     string `json:"token_env"`     // env var holding the Databricks bearer token

	// s3 sink (AWS S3 and S3-compatible endpoints, e.g. MinIO)
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix"`            // optional key prefix
	Region          string `json:"region"`            // falls back to AWS_REGION / AWS_DEFAULT_REGION
	ForcePathStyle  bool   `json:"force_path_style"`  // MinIO-style addressing
	Endpoint        string `json:"endpoint"`          // optional S3-compatible endpoint URL (e.g. https://play.min.io)

	// kafka sink
	Brokers []string    `json:"brokers"` // e.g. ["kafka-1:9092"]
	Topic   string      `json:"topic"`   // destination topic
	CAEnv   string      `json:"ca_env"`  // optional env var holding a PEM CA bundle path for the broker
	SASL    *KafkaSASL  `json:"sasl"`    // optional; omit for plaintext/PLAIN or SASL per broker config

	// sql sink (any RDBMS via database/sql)
	Driver     string   `json:"driver"`      // pgx|mysql|sqlserver|oracle|sqlite
	Table      string   `json:"table"`       // validated identifier, optionally schema-qualified
	DSNEnv     string   `json:"dsn_env"`     // env var holding the DSN
	KeyColumns []string `json:"key_columns"` // columns forming the uniqueness constraint
	Columns    []string `json:"columns"`     // optional allow-list of columns to write (default all)
	AppendOnly bool     `json:"append_only"` // plain INSERT, ignore conflicts (default false = upsert)

	// http-family sinks (stdlib)
	Index       string `json:"index"`        // elasticsearch index or clickhouse table
	Pipeline    string `json:"pipeline"`     // elasticsearch ingest pipeline (optional)
	Database    string `json:"database"`     // clickhouse database (default default)
	CHUserEnv   string `json:"user_env"`     // clickhouse: env var holding the user (default "default")
	CHPasswordEnv string `json:"password_env"` // clickhouse: env var holding the password

	// rabbitmq sink
	AMQPURLEnv string `json:"amqp_url_env"` // env var holding the AMQP URL
	AMQPQueue  string `json:"amqp_queue"`   // destination queue

	// nats sink
	NatsURLEnv    string `json:"nats_url_env"`    // env var holding the NATS URL
	NatsSubject   string `json:"nats_subject"`    // destination subject
	NatsStream    string `json:"nats_stream"`     // optional JetStream stream name (PULL consumers use it)

	// redis sink
	RedisURLEnv string `json:"redis_url_env"` // env var holding the Redis connection URL
	RedisStream string `json:"redis_stream"`  // destination stream key

	// IBM MQ sink (mqweb REST API, stdlib)
	MQWebURL     string `json:"mqweb_url"`      // e.g. https://mq:9443/ibmmq/rest/v3
	MQWebQueueMgr string `json:"mqweb_queue_mgr"`
	MQWebQueue   string `json:"mqweb_queue"`
	MQWebUser    string `json:"mqweb_user"`
	MQWebPassEnv string `json:"mqweb_pass_env"` // env var holding the mqweb password
	MQWebCAEnv   string `json:"mqweb_ca_env"`   // optional env var with PEM CA bundle (empty = system roots)

	// IBM MQ source uses the same fields on ingest side (mqtt/etc. use their own readers).
}

// KafkaSASL configures optional SASL for the kafka sink.
type KafkaSASL struct {
	Mechanism   string `json:"mechanism"`     // plain | scram-sha-256 | scram-sha-512
	Username    string `json:"username"`
	PasswordEnv string `json:"password_env"`  // env var holding the password
}

// RetryConfig bounds sink retry behavior.
type RetryConfig struct {
	Budget    int `json:"budget"`     // retries per batch before DLQ/park; default 5
	BackoffMs int `json:"backoff_ms"` // base backoff, doubled per attempt; default 100
}

// Config is the top-level egress config file: a list of routes.
type Config struct {
	Firn struct {
		Addr      string `json:"addr"`
		APIKeyEnv string `json:"api_key_env"`
		CAEnv     string `json:"ca_env"`
		Insecure  bool   `json:"insecure"`
	} `json:"firn"`
	Routes []Route `json:"routes"`
}

// LoadConfig reads and validates the JSON config at path.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("egress: read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("egress: parse config %s: %w", path, err)
	}
	return cfg.Validate()
}

// Validate checks the config and normalizes defaults.
func (c *Config) Validate() (*Config, error) {
	if c.Firn.Addr == "" {
		return nil, errors.New("egress: firn.addr required")
	}
	if len(c.Routes) == 0 {
		return nil, errors.New("egress: at least one route required")
	}
	names := map[string]bool{}
	for i := range c.Routes {
		r := &c.Routes[i]
		if !integration.ValidName(r.Name) {
			return nil, fmt.Errorf("egress: routes[%d]: invalid name %q", i, r.Name)
		}
		if names[r.Name] {
			return nil, fmt.Errorf("egress: duplicate route %q", r.Name)
		}
		names[r.Name] = true
		if len(r.Sources) == 0 {
			return nil, fmt.Errorf("egress: route %q: at least one source required", r.Name)
		}
		// v1: exactly one source per route. Multi-topic fan-in is a
		// documented follow-up (ordering + ack semantics need care).
		if len(r.Sources) > 1 {
			return nil, fmt.Errorf("egress: route %q: multiple sources per route are not supported yet (one topic or one queue)", r.Name)
		}
		src := r.Sources[0]
		switch {
		case src.Topic != "" && src.Queue == "":
		case src.Queue != "" && src.Topic == "":
		default:
			return nil, fmt.Errorf("egress: route %q: exactly one of topic or queue required", r.Name)
		}
		if r.Group == "" {
			r.Group = "firn-egress-" + r.Name
		}
		if r.AckPolicy == "" {
			r.AckPolicy = "after_sink_confirm"
		}
		if r.AckPolicy != "after_sink_confirm" {
			return nil, fmt.Errorf("egress: route %q: ack_policy must be after_sink_confirm (got %q)", r.Name, r.AckPolicy)
		}
		if r.Batch.MaxRecords <= 0 {
			r.Batch.MaxRecords = 500
		}
		if r.Batch.MaxRecords > 500 {
			return nil, fmt.Errorf("egress: route %q: batch.max_records capped at 500", r.Name)
		}
		if r.Batch.FlushIntervalMs <= 0 {
			r.Batch.FlushIntervalMs = 1000
		}
		if r.Retry.Budget <= 0 {
			r.Retry.Budget = 5
		}
		if r.Retry.BackoffMs <= 0 {
			r.Retry.BackoffMs = 100
		}
		switch r.Sink.Type {
		case "file":
			if r.Sink.Dir == "" {
				return nil, fmt.Errorf("egress: route %q: sink.dir required for file sink", r.Name)
			}
		case "http":
			if r.Sink.URL == "" {
				return nil, fmt.Errorf("egress: route %q: sink.url required for http sink", r.Name)
			}
			if !r.Sink.InsecureHTTP && !urlHasScheme(r.Sink.URL, "https") {
				return nil, fmt.Errorf("egress: route %q: http sink requires https (or insecure_http: true for dev)", r.Name)
			}
			switch r.Sink.Auth {
			case "", "none":
			case "bearer_env", "hmac_env":
				if r.Sink.AuthEnv == "" {
					return nil, fmt.Errorf("egress: route %q: sink.auth_env required for auth %q", r.Name, r.Sink.Auth)
				}
				if os.Getenv(r.Sink.AuthEnv) == "" {
					return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.AuthEnv)
				}
			default:
				return nil, fmt.Errorf("egress: route %q: unknown auth %q (none|bearer_env|hmac_env)", r.Name, r.Sink.Auth)
			}
			if r.Sink.TimeoutMs > 0 && r.Sink.TimeoutMs < 100 {
				return nil, fmt.Errorf("egress: route %q: sink.timeout_ms too small (min 100)", r.Name)
			}
		case "databricks":
			if r.Sink.WorkspaceURL == "" || r.Sink.VolumePath == "" || r.Sink.TokenEnv == "" {
				return nil, fmt.Errorf("egress: route %q: databricks sink requires workspace_url, volume_path and token_env", r.Name)
			}
			if !urlHasScheme(r.Sink.WorkspaceURL, "https") && !r.Sink.InsecureHTTP {
				return nil, fmt.Errorf("egress: route %q: databricks workspace_url must be https (or insecure_http: true for dev)", r.Name)
			}
			if !strings.HasPrefix(r.Sink.VolumePath, "/Volumes/") {
				return nil, fmt.Errorf("egress: route %q: volume_path must start with /Volumes/", r.Name)
			}
			if os.Getenv(r.Sink.TokenEnv) == "" {
				return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.TokenEnv)
			}
		case "s3":
			if r.Sink.Bucket == "" {
				return nil, fmt.Errorf("egress: route %q: s3 sink requires bucket", r.Name)
			}
			region := r.Sink.Region
			if region == "" {
				region = os.Getenv("AWS_REGION")
			}
			if region == "" {
				region = os.Getenv("AWS_DEFAULT_REGION")
			}
			if region == "" {
				return nil, fmt.Errorf("egress: route %q: s3 region required (sink.region or AWS_REGION)", r.Name)
			}
			r.Sink.Region = region
			if r.Sink.Endpoint != "" && !urlHasScheme(r.Sink.Endpoint, "https") && !r.Sink.InsecureHTTP {
				return nil, fmt.Errorf("egress: route %q: s3 endpoint must be https (or insecure_http: true for local/MinIO)", r.Name)
			}
			for _, v := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
				if os.Getenv(v) == "" {
					return nil, fmt.Errorf("egress: route %q: env %s is required for s3 sink", r.Name, v)
				}
			}
		case "kafka":
			if len(r.Sink.Brokers) == 0 || r.Sink.Topic == "" {
				return nil, fmt.Errorf("egress: route %q: kafka sink requires brokers and topic", r.Name)
			}
			if r.Sink.SASL != nil {
				switch r.Sink.SASL.Mechanism {
				case "plain", "scram-sha-256", "scram-sha-512":
				default:
					return nil, fmt.Errorf("egress: route %q: sasl mechanism must be plain|scram-sha-256|scram-sha-512", r.Name)
				}
				if r.Sink.SASL.Username == "" || r.Sink.SASL.PasswordEnv == "" {
					return nil, fmt.Errorf("egress: route %q: kafka sasl requires username and password_env", r.Name)
				}
				if os.Getenv(r.Sink.SASL.PasswordEnv) == "" {
					return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.SASL.PasswordEnv)
				}
			}
		case "sql":
			if r.Sink.Driver == "" || r.Sink.DSNEnv == "" || r.Sink.Table == "" {
				return nil, fmt.Errorf("egress: route %q: sql sink requires driver, dsn_env and table", r.Name)
			}
		case "rabbitmq":
			if r.Sink.AMQPURLEnv == "" || r.Sink.AMQPQueue == "" {
				return nil, fmt.Errorf("egress: route %q: rabbitmq sink requires amqp_url_env and amqp_queue", r.Name)
			}
			if os.Getenv(r.Sink.AMQPURLEnv) == "" {
				return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.AMQPURLEnv)
			}
		case "nats":
			if r.Sink.NatsURLEnv == "" || r.Sink.NatsSubject == "" {
				return nil, fmt.Errorf("egress: route %q: nats sink requires nats_url_env and nats_subject", r.Name)
			}
			if os.Getenv(r.Sink.NatsURLEnv) == "" {
				return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.NatsURLEnv)
			}
		case "redis-streams":
			if r.Sink.RedisURLEnv == "" || r.Sink.RedisStream == "" {
				return nil, fmt.Errorf("egress: route %q: redis-streams sink requires redis_url_env and redis_stream", r.Name)
			}
			if os.Getenv(r.Sink.RedisURLEnv) == "" {
				return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.RedisURLEnv)
			}
		case "ibmmq":
			if r.Sink.MQWebURL == "" || r.Sink.MQWebQueueMgr == "" || r.Sink.MQWebQueue == "" || r.Sink.MQWebUser == "" || r.Sink.MQWebPassEnv == "" {
				return nil, fmt.Errorf("egress: route %q: ibmmq sink requires mqweb_url, mqweb_queue_mgr, mqweb_queue, mqweb_user and mqweb_pass_env", r.Name)
			}
			if !strings.HasPrefix(r.Sink.MQWebURL, "https://") {
				return nil, fmt.Errorf("egress: route %q: mqweb_url must be https", r.Name)
			}
			if os.Getenv(r.Sink.MQWebPassEnv) == "" {
				return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.MQWebPassEnv)
			}
		case "elasticsearch":
			if r.Sink.URL == "" || r.Sink.Index == "" {
				return nil, fmt.Errorf("egress: route %q: elasticsearch sink requires url and index", r.Name)
			}
			if os.Getenv(r.Sink.AuthEnv) == "" && r.Sink.AuthEnv != "" {
				return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.AuthEnv)
			}
			if r.Sink.Auth != "none" && r.Sink.Auth != "" && r.Sink.Auth != "bearer_env" {
				return nil, fmt.Errorf("egress: route %q: elasticsearch auth must be bearer_env|none (got %q)", r.Name, r.Sink.Auth)
			}
		case "clickhouse":
			if r.Sink.URL == "" || r.Sink.Index == "" {
				return nil, fmt.Errorf("egress: route %q: clickhouse sink requires url and table (use index for table name)", r.Name)
			}
		default:
			return nil, fmt.Errorf("egress: route %q: unknown sink type %q (file|http|databricks|s3|kafka|sql|rabbitmq|nats|redis-streams|ibmmq|elasticsearch|clickhouse)", r.Name, r.Sink.Type)
		}
	}
	return c, nil
}

func urlHasScheme(u, scheme string) bool {
	p := scheme + "://"
	return len(u) > len(p) && u[:len(p)] == p
}
