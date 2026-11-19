package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// OutputRecord is the result of pushing one input record through the stages.
type OutputRecord struct {
	Output  string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// ProcessError marks a record-level failure. The runner routes it to the
// pipeline's dead-letter topic (or aborts, per on_error=fail).
type ProcessError struct {
	Stage string
	Err   error
}

func (e *ProcessError) Error() string { return fmt.Sprintf("stage %s: %v", e.Stage, e.Err) }
func (e *ProcessError) Unwrap() error  { return e.Err }

// Stats counts pipeline activity.
type Stats struct {
	In            int64
	Out           map[string]int64
	Filtered      int64
	Unrouted      int64
	DLQ           int64
	AICalls       int64
	AIInvalid     int64
	AICircuitOpen int64
}

func NewStats() *Stats { return &Stats{Out: map[string]int64{}} }

// Process runs one record through the stages. Stage semantics:
//
//   - filter:  records not matching the predicate are dropped (counted).
//   - project: keep/drop/rename fields (dot-separated paths).
//   - map:     set/default/cast fields.
//   - enrich:  look up a field in a local read-only JSONL reference source.
//   - ai:      provider-backed stage; output validated against the contract.
//   - route:   splits records to named outputs; must be the last stage.
//
// Without a route stage, the pipeline requires exactly one output.
func (c *Config) Process(rec Record, stats *Stats, rev string) ([]OutputRecord, error) {
	stats.In++
	doc, ok := parseObject(rec.Value)
	if !ok {
		return nil, &ProcessError{Stage: "parse", Err: errors.New("record value is not a JSON object")}
	}
	if rec.Source != "" {
		if _, exists := doc["_source"]; !exists {
			doc["_source"] = rec.Source
		}
	}

	mk := func(out string, d map[string]any) (OutputRecord, error) {
		v, err := json.Marshal(d)
		if err != nil {
			return OutputRecord{}, err
		}
		stats.Out[out]++
		return OutputRecord{Output: out, Key: rec.Key, Value: v, Headers: rec.Headers}, nil
	}

	for si, st := range c.Stages {
		stageName := fmt.Sprintf("stages[%d]:%s", si, st.Type)
		fail := func(err error) ([]OutputRecord, error) {
			if st.OnError == "fail" || (st.OnError == "" && c.OnError == "fail") {
				return nil, &ProcessError{Stage: stageName, Err: err}
			}
			return nil, &ProcessError{Stage: stageName, Err: err}
		}
		var err error
		switch st.Type {
		case "filter":
			var keep bool
			keep, err = evalPredicate(doc, Predicate{Field: st.Field, Op: st.Op, Value: st.Value})
			if err == nil && !keep {
				stats.Filtered++
				return nil, nil // dropped, not an error
			}
		case "project":
			doc, err = applyProject(doc, st)
		case "map":
			doc, err = applyMap(doc, st)
		case "enrich":
			doc, err = applyEnrich(doc, st.Enrich)
		case "ai":
			doc, err = applyAI(doc, st.AI, stats, rev)
		case "route":
			matched := ""
			for _, rule := range st.Rules {
				var ok bool
				ok, err = evalPredicate(doc, rule.When)
				if err != nil {
					return fail(err)
				}
				if ok {
					matched = rule.Output
					break
				}
			}
			if matched == "" {
				matched = st.DefaultRoute
			}
			if matched == "" {
				stats.Unrouted++
				return nil, nil
			}
			orec, mkErr := mk(matched, doc)
			if mkErr != nil {
				return fail(mkErr)
			}
			return []OutputRecord{orec}, nil
		default:
			err = fmt.Errorf("unknown stage type %q", st.Type)
		}
		if err != nil {
			return fail(err)
		}
	}
	// No route stage: emit to the single output (validated at config load).
	for name := range c.Outputs {
		orec, err := mk(name, doc)
		if err != nil {
			return nil, &ProcessError{Stage: "marshal", Err: err}
		}
		return []OutputRecord{orec}, nil
	}
	return nil, nil
}

func parseObject(b []byte) (map[string]any, bool) {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, false
	}
	m, ok := v.(map[string]any)
	return m, ok
}

func evalPredicate(doc map[string]any, p Predicate) (bool, error) {
	v, found := getPath(doc, p.Field)
	switch p.Op {
	case "exists":
		return found == truthy(p.Value, true), nil
	}
	if !found {
		return false, nil
	}
	switch p.Op {
	case "eq":
		return jsonEqual(v, p.Value), nil
	case "ne":
		return !jsonEqual(v, p.Value), nil
	case "in":
		arr, ok := p.Value.([]any)
		if !ok {
			return false, errors.New("in: value must be a JSON array")
		}
		for _, item := range arr {
			if jsonEqual(v, item) {
				return true, nil
			}
		}
		return false, nil
	case "contains":
		s, ok := v.(string)
		if !ok {
			return false, errors.New("contains: field must be a string")
		}
		sub, ok := p.Value.(string)
		if !ok {
			return false, errors.New("contains: value must be a string")
		}
		return strings.Contains(s, sub), nil
	case "gt", "ge", "lt", "le":
		nf, ok := toNumber(v)
		if !ok {
			return false, errors.New("comparison: field must be a number")
		}
		nfv, ok := toNumber(p.Value)
		if !ok {
			return false, errors.New("comparison: value must be a number")
		}
		switch p.Op {
		case "gt":
			return nf > nfv, nil
		case "ge":
			return nf >= nfv, nil
		case "lt":
			return nf < nfv, nil
		case "le":
			return nf <= nfv, nil
		}
	}
	return false, fmt.Errorf("unknown op %q", p.Op)
}

func truthy(v any, def bool) bool {
	if v == nil {
		return def
	}
	b, ok := v.(bool)
	if !ok {
		return def
	}
	return b
}

func jsonEqual(a, b any) bool {
	af, aok := toNumber(a)
	bf, bok := toNumber(b)
	if aok && bok {
		return af == bf
	}
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	default:
		ab, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		return string(ab) == string(bb)
	}
}

func toNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

func getPath(doc map[string]any, path string) (any, bool) {
	var cur any = doc
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func setPath(doc map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	cur := doc
	for i, part := range parts {
		if i == len(parts)-1 {
			cur[part] = value
			return
		}
		next, ok := cur[part].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[part] = next
		}
		cur = next
	}
}

func deletePath(doc map[string]any, path string) {
	parts := strings.Split(path, ".")
	cur := doc
	for i, part := range parts {
		if i == len(parts)-1 {
			delete(cur, part)
			return
		}
		next, ok := cur[part].(map[string]any)
		if !ok {
			return
		}
		cur = next
	}
}

func applyProject(doc map[string]any, st Stage) (map[string]any, error) {
	if len(st.Keep) > 0 {
		out := map[string]any{}
		for _, path := range st.Keep {
			if v, ok := getPath(doc, path); ok {
				setPath(out, path, v)
			}
		}
		doc = out
	}
	for _, path := range st.Drop {
		deletePath(doc, path)
	}
	for from, to := range st.Rename {
		if v, ok := getPath(doc, from); ok {
			deletePath(doc, from)
			setPath(doc, to, v)
		}
	}
	return doc, nil
}

func applyMap(doc map[string]any, st Stage) (map[string]any, error) {
	for path, v := range st.Set {
		setPath(doc, path, v)
	}
	for path, v := range st.Default {
		if _, ok := getPath(doc, path); !ok {
			setPath(doc, path, v)
		}
	}
	for path, to := range st.Cast {
		v, ok := getPath(doc, path)
		if !ok {
			continue
		}
		var nv any
		switch to {
		case "string":
			switch x := v.(type) {
			case string:
				nv = x
			case bool:
				nv = fmt.Sprintf("%t", x)
			default:
				if f, ok := toNumber(v); ok {
					nv = fmt.Sprintf("%g", f)
				} else {
					b, _ := json.Marshal(v)
					nv = string(b)
				}
			}
		case "number":
			if f, ok := toNumber(v); ok {
				nv = f
			} else if s, ok := v.(string); ok {
				var f float64
				if _, err := fmt.Sscanf(strings.TrimSpace(s), "%g", &f); err != nil {
					return nil, fmt.Errorf("cast %q to number: %q", path, s)
				}
				nv = f
			} else {
				return nil, fmt.Errorf("cast %q to number: unsupported type", path)
			}
		case "bool":
			switch x := v.(type) {
			case bool:
				nv = x
			case string:
				switch strings.ToLower(strings.TrimSpace(x)) {
				case "true", "1", "yes":
					nv = true
				case "false", "0", "no":
					nv = false
				default:
					return nil, fmt.Errorf("cast %q to bool: %q", path, x)
				}
			default:
				return nil, fmt.Errorf("cast %q to bool: unsupported type", path)
			}
		}
		setPath(doc, path, nv)
	}
	return doc, nil
}
