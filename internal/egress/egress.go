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
	MaxBatchBytes   int    `json:"max_batch_bytes"`  // truncate/split? not used — enforced at 1 MiB hard cap
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
		default:
			return nil, fmt.Errorf("egress: route %q: unknown sink type %q (file|http)", r.Name, r.Sink.Type)
		}
	}
	return c, nil
}

func urlHasScheme(u, scheme string) bool {
	p := scheme + "://"
	return len(u) > len(p) && u[:len(p)] == p
}
