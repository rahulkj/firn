// Package pipeline implements Firn's data-pipeline integrations
// (ADR-018, docs/plans/009-data-pipelines.md): declarative stages that
// select/reshape/route records between Firn sources and outputs. Pipelines
// run outside firnd and talk to Firn only through the client API.
// Transformation is deterministic unless an ai_* stage is configured and
// explicitly enabled; records that cannot be processed per policy go to a
// configured dead-letter topic — never silently dropped.
package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Record is the normalized envelope the stages operate on.
type Record struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
	Source  string // "topic:iot" or "queue:jobs"
	Partition int32
	Offset    int64  // -1 for queue sources
	DeliveryID string // set for queue outputs/sources
}

// OutputDef names a destination for routed records.
type OutputDef struct {
	Type  string `json:"type"`  // firn_topic | firn_queue
	Topic string `json:"topic"` // firn_topic
	Queue string `json:"queue"` // firn_queue
}

// Stage is one declarative step. Type selects the implementation; the
// remaining fields are type-specific.
type Stage struct {
	Type string `json:"type"` // filter | project | map | route | enrich | ai

	// filter / route predicates use Field/Op/Value.
	Field string `json:"field"`
	Op    string `json:"op"` // eq, ne, in, exists, contains, gt, ge, lt, le
	Value any    `json:"value"`

	// project
	Keep   []string          `json:"keep"`
	Drop   []string          `json:"drop"`
	Rename map[string]string `json:"rename"` // from -> to
	// map
	Set     map[string]any    `json:"set"`
	Default map[string]any    `json:"default"`
	Cast    map[string]string `json:"cast"` // field -> string|number|bool

	// route
	Rules        []RouteRule `json:"rules"`
	DefaultRoute string      `json:"default_output"` // empty = record to DLQ/drop-counted

	// enrich
	Enrich *EnrichSpec `json:"enrich"`

	// ai
	AI *AISpec `json:"ai"`

	// error policy for the whole stage: dlq (default) | fail | passthrough_raw
	OnError string `json:"on_error"`
}

// RouteRule sends matching records to one named output.
type RouteRule struct {
	When   Predicate `json:"when"`
	Output string    `json:"output"`
}

// Predicate is the shared filter expression used by filter and route stages.
type Predicate struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value"`
}

// EnrichSpec looks up one field value in a local read-only reference source.
type EnrichSpec struct {
	KeyPath   string            `json:"key_path"`    // JSON path of the lookup key in the record
	SourceFile string           `json:"source_file"` // JSONL reference file (read-only)
	MatchField string           `json:"match_field"` // reference field whose scalar value equals the lookup key
	OnMiss     string           `json:"on_miss"`     // passthrough (default) | dlq
	Fields     map[string]string `json:"fields"`      // ref field -> output field
	CacheRecs  int              `json:"cache_records"` // max reference rows held in memory (default 100000)
}

// AISpec configures one AI-assisted stage (opt-in, ADR-018 §3).
type AISpec struct {
	Provider   string `json:"provider"`    // mock | openai_compatible
	Endpoint   string `json:"endpoint"`    // openai_compatible only; must be https unless InsecureHTTP
	Model      string `json:"model"`
	APIKeyEnv  string `json:"api_key_env"` // env var for the provider key
	Prompt     string `json:"prompt"`      // prompt template; {{payload}} is substituted with the record JSON
	InsecureHTTP bool  `json:"insecure_http"`
	// OutputContract requires these fields of the JSON response: name -> type (string|number|boolean|object|array).
	OutputContract map[string]string `json:"output_contract"`
	OnInvalid      string             `json:"on_invalid"` // retry (default, max 2) | dlq | fail
	MaxPerMinute   int                `json:"max_calls_per_minute"` // default 60
	DailyBudget    int                `json:"daily_call_budget"`    // default 10000; circuit-breaks the stage when exhausted
	TimeoutMs      int                `json:"timeout_ms"`           // default 15000
}

// InputDef names one input source. Multi-input pipelines fan records from
// every input into the same stage graph; ordering across inputs is undefined
// (documented). Each input runs under its own consumer group.
type InputDef struct {
	Topic      string  `json:"topic"`
	Queue      string  `json:"queue"`
	Partitions []int32 `json:"partitions"`
	Group      string  `json:"group"` // default firn-pipeline-<name>[-<i>]
}

// Config is the versioned pipeline definition (pipeline.json).
type Config struct {
	Name string `json:"name"`
	// Single input: use "input". Multiple inputs: use "inputs" (a list of
	// InputDef). Multi-input pipelines inject a "_source" field into each
	// record's doc before stages run ("topic:name" or "queue:name").
	Input  InputDef   `json:"input"`
	Inputs []InputDef `json:"inputs"`
	Stages  []Stage            `json:"stages"`
	Outputs map[string]OutputDef `json:"outputs"`
	DLQ     string             `json:"dlq"` // dead-letter topic (optional; strongly recommended)
	// OnError default for stages without explicit on_error: dlq (default) | fail
	OnError string `json:"on_error"`
}

// LoadConfig reads and validates the JSON config at path.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pipeline: read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("pipeline: parse %s: %w", path, err)
	}
	return cfg.Validate()
}

// Validate checks the config and normalizes defaults.
func (c *Config) Validate() (*Config, error) {
	if c.Name == "" {
		return nil, errors.New("pipeline: name required")
	}
	inputs := c.ResolvedInputs()
	if len(inputs) == 0 {
		return nil, errors.New("pipeline: at least one input is required (input or inputs)")
	}
	for i, in := range inputs {
		t, q := in.Topic, in.Queue
		if (t == "") == (q == "") {
			return nil, fmt.Errorf("pipeline: inputs[%d]: exactly one of topic or queue required", i)
		}
	}
	if len(c.Outputs) == 0 {
		return nil, errors.New("pipeline: at least one output required")
	}
	for name, out := range c.Outputs {
		if err := validateName(name); err != nil {
			return nil, fmt.Errorf("pipeline: output %q: %w", name, err)
		}
		switch out.Type {
		case "firn_topic":
			if out.Topic == "" {
				return nil, fmt.Errorf("pipeline: output %q: topic required", name)
			}
		case "firn_queue":
			if out.Queue == "" {
				return nil, fmt.Errorf("pipeline: output %q: queue required", name)
			}
		default:
			return nil, fmt.Errorf("pipeline: output %q: unknown type %q (firn_topic|firn_queue)", name, out.Type)
		}
	}
	if c.OnError == "" {
		c.OnError = "dlq"
	}
	if c.OnError != "dlq" && c.OnError != "fail" {
		return nil, fmt.Errorf("pipeline: on_error must be dlq|fail (got %q)", c.OnError)
	}
	routeIdx := -1
	for i := range c.Stages {
		if c.Stages[i].Type == "route" {
			if routeIdx >= 0 {
				return nil, errors.New("pipeline: at most one route stage allowed")
			}
			routeIdx = i
		}
	}
	if routeIdx >= 0 && routeIdx != len(c.Stages)-1 {
		return nil, errors.New("pipeline: route stage must be the last stage")
	}
	if routeIdx < 0 && len(c.Outputs) > 1 {
		return nil, errors.New("pipeline: multiple outputs require a route stage")
	}
	for i := range c.Stages {
		if err := c.Stages[i].Validate(c); err != nil {
			return nil, fmt.Errorf("pipeline: stages[%d] (%s): %w", i, c.Stages[i].Type, err)
		}
	}
	return c, nil
}

// ResolvedInputs returns the effective input list, normalizing defaults
// (group names). Exactly one of c.Input or c.Inputs must be set.
func (c *Config) ResolvedInputs() []InputDef {
	if c.Input.Topic != "" || c.Input.Queue != "" {
		out := c.Input
		if out.Topic != "" && out.Group == "" {
			out.Group = "firn-pipeline-" + c.Name
		}
		return []InputDef{out}
	}
	out := make([]InputDef, len(c.Inputs))
	for i, in := range c.Inputs {
		if in.Group == "" {
			in.Group = fmt.Sprintf("firn-pipeline-%s-%d", c.Name, i)
		}
		out[i] = in
	}
	return out
}

// Validate checks one stage against the whole config (outputs exist, types known).
func (s *Stage) Validate(c *Config) error {
	switch s.Type {
	case "filter":
		return validatePredicate(Predicate{Field: s.Field, Op: s.Op, Value: s.Value})
	case "project":
		if len(s.Keep) == 0 && len(s.Drop) == 0 && len(s.Rename) == 0 {
			return errors.New("at least one of keep, drop, rename required")
		}
		for from, to := range s.Rename {
			if from == "" || to == "" {
				return errors.New("rename keys and values must be non-empty")
			}
		}
		return nil
	case "map":
		if len(s.Set) == 0 && len(s.Default) == 0 && len(s.Cast) == 0 {
			return errors.New("at least one of set, default, cast required")
		}
		for f, to := range s.Cast {
			switch to {
			case "string", "number", "bool":
			default:
				return fmt.Errorf("cast %q: target must be string|number|bool", f)
			}
		}
		return nil
	case "route":
		if len(s.Rules) == 0 {
			return errors.New("at least one rule required")
		}
		for _, rule := range s.Rules {
			if err := validatePredicate(rule.When); err != nil {
				return err
			}
			if _, ok := c.Outputs[rule.Output]; !ok {
				return fmt.Errorf("rule output %q is not defined in outputs", rule.Output)
			}
		}
		if s.DefaultRoute != "" {
			if _, ok := c.Outputs[s.DefaultRoute]; !ok {
				return fmt.Errorf("default_output %q is not defined in outputs", s.DefaultRoute)
			}
		}
		return nil
	case "enrich":
		if s.Enrich == nil {
			return errors.New("enrich spec required")
		}
		if s.Enrich.KeyPath == "" || s.Enrich.SourceFile == "" || s.Enrich.MatchField == "" {
			return errors.New("enrich.key_path, enrich.source_file and enrich.match_field required")
		}
		switch s.Enrich.OnMiss {
		case "", "passthrough", "dlq":
		default:
			return fmt.Errorf("enrich.on_miss must be passthrough|dlq (got %q)", s.Enrich.OnMiss)
		}
		if len(s.Enrich.Fields) == 0 {
			return errors.New("enrich.fields must map at least one reference field to an output field")
		}
		return nil
	case "ai":
		if s.AI == nil {
			return errors.New("ai spec required")
		}
		switch s.AI.Provider {
		case "mock":
		case "openai_compatible":
			if s.AI.Endpoint == "" || s.AI.Model == "" || s.AI.APIKeyEnv == "" {
				return errors.New("ai: endpoint, model and api_key_env required for openai_compatible")
			}
			if !strings.HasPrefix(s.AI.Endpoint, "https://") && !s.AI.InsecureHTTP {
				return errors.New("ai: endpoint must be https (or set insecure_http for dev)")
			}
		default:
			return fmt.Errorf("ai.provider must be mock|openai_compatible (got %q)", s.AI.Provider)
		}
		if len(s.AI.OutputContract) == 0 {
			return errors.New("ai.output_contract must declare at least one required field")
		}
		for f, t := range s.AI.OutputContract {
			switch t {
			case "string", "number", "boolean", "object", "array":
			default:
				return fmt.Errorf("ai.output_contract %q: type must be string|number|boolean|object|array", f)
			}
		}
		switch s.AI.OnInvalid {
		case "", "retry", "dlq", "fail":
		default:
			return fmt.Errorf("ai.on_invalid must be retry|dlq|fail (got %q)", s.AI.OnInvalid)
		}
		if s.AI.MaxPerMinute < 0 || s.AI.DailyBudget < 0 {
			return errors.New("ai.max_calls_per_minute and daily_call_budget must be >= 0")
		}
		return nil
	default:
		return fmt.Errorf("unknown stage type %q", s.Type)
	}
}

func validatePredicate(p Predicate) error {
	if p.Field == "" {
		return errors.New("field required")
	}
	switch p.Op {
	case "eq", "ne", "in", "exists", "contains", "gt", "ge", "lt", "le":
	default:
		return fmt.Errorf("op must be eq|ne|in|exists|contains|gt|ge|lt|le (got %q)", p.Op)
	}
	return nil
}

func validateName(s string) error {
	if s == "" || len(s) > 64 {
		return errors.New("name empty or too long")
	}
	for _, r := range s {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.'
		if !ok {
			return fmt.Errorf("invalid character %q", r)
		}
	}
	return nil
}
