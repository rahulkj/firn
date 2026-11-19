package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// SuggestMapping is the design-time mapping assistant (ADR-018 slice 5):
// given one sample record and a free-text goal, it returns a proposed
// pipeline fragment (a JSON object with "project" and/or "map" stages in
// pipeline config shape) that a human reviews and pastes — it never mutates
// deployed configs. Runs strictly at design time via the same provider
// abstraction and guardrails as the runtime ai stage.
func SuggestMapping(ctx context.Context, spec *AISpec, sample []byte, goal string) (map[string]any, error) {
	if spec == nil {
		return nil, errors.New("ai: spec required")
	}
	if !json.Valid(sample) {
		return nil, errors.New("ai: sample record is not valid JSON")
	}
	prompt := fmt.Sprintf(`You are a data-pipeline config generator. Given ONE sample JSON record and a goal, reply with ONLY a JSON object shaped like:

{"project": {"keep": ["field.path"], "drop": ["field.path"], "rename": {"from.path": "to.path"}}, "map": {"set": {"field": "literal"}, "default": {"field": "literal"}}}

Rules: dot-separated paths; "keep"/"drop" optional; output ONLY the JSON object, no prose, no markdown fences. Use exactly the fields present in the sample. Follow the goal strictly.

Goal: %s
Sample record: %s`, goal, string(sample))
	out, err := callAIProvider(ctx, spec, prompt)
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"project", "map"} {
		v, ok := out[key]
		if !ok {
			continue
		}
		if _, ok := v.(map[string]any); !ok {
			return nil, fmt.Errorf("ai: %q in proposed mapping must be an object", key)
		}
	}
	if _, ok := out["project"]; !ok {
		if _, ok := out["map"]; !ok {
			return nil, errors.New("ai: proposed mapping has neither project nor map")
		}
	}
	return out, nil
}
