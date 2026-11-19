package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// errAICircuitOpen is returned when the daily AI budget is exhausted; the
// runner dead-letters records per the stage policy until the budget resets
// (midnight, local) or is raised.
var errAICircuitOpen = errors.New("ai: daily call budget exhausted (circuit open)")

// MockHandler is test-only: when set, the mock provider returns whatever it
// produces for the given prompt. Production configs never see it.
var MockHandler func(prompt string) (map[string]any, error)

type aiLimiter struct {
	mu        sync.Mutex
	window    []time.Time
	dayCount  int
	dayStart  time.Time
	maxPerMin int
	budget    int
}

var aiLimiters sync.Map // key *AISpec -> *aiLimiter

func limiterFor(spec *AISpec) *aiLimiter {
	v, _ := aiLimiters.LoadOrStore(spec, &aiLimiter{
		maxPerMin: aiMax(spec.MaxPerMinute, 60),
		budget:    spec.DailyBudget,
	})
	return v.(*aiLimiter)
}

func aiMax(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// acquire blocks (bounded) until a call slot is available or the budget is
// known-exhausted. Returns errAICircuitOpen when the daily budget is spent.
func (l *aiLimiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		now := time.Now()
		if l.dayStart.IsZero() || now.Sub(l.dayStart) >= 24*time.Hour {
			l.dayStart = now
			l.dayCount = 0
		}
		if l.budget > 0 && l.dayCount >= l.budget {
			l.mu.Unlock()
			return errAICircuitOpen
		}
		cutoff := now.Add(-time.Minute)
		kept := l.window[:0]
		for _, t := range l.window {
			if t.After(cutoff) {
				kept = append(kept, t)
			}
		}
		l.window = kept
		if len(l.window) < l.maxPerMin {
			l.window = append(l.window, now)
			l.dayCount++
			l.mu.Unlock()
			return nil
		}
		// Rate limited this minute: wait until the oldest call leaves the window (bounded).
		oldest := l.window[0]
		wait := time.Minute - now.Sub(oldest) + time.Millisecond
		l.mu.Unlock()
		if wait > 2*time.Second {
			wait = 2 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// applyAI performs one AI stage: rate limit, call the provider, validate the
// contract, merge validated fields into the record.
func applyAI(doc map[string]any, spec *AISpec, stats *Stats, rev string) (map[string]any, error) {
	if spec == nil {
		return nil, errors.New("ai: spec required")
	}
	lim := limiterFor(spec)
	if err := lim.acquire(context.Background()); err != nil {
		stats.AICircuitOpen++
		return nil, err
	}
	stats.AICalls++
	payload, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	prompt := strings.ReplaceAll(spec.Prompt, "{{payload}}", string(payload))
	timeout := time.Duration(spec.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	out, err := callAIProvider(ctx, spec, prompt)
	if err == nil {
		err = validateAIContract(out, spec.OutputContract)
	}
	if err != nil {
		switch spec.OnInvalid {
		case "fail", "dlq", "":
			// retry mode also lands here after the in-provider attempts
			stats.AIInvalid++
			return nil, fmt.Errorf("ai: %w", err)
		}
	}
	for k, v := range out {
		doc[k] = v // contract-validated fields merge into the record
	}
	_ = rev
	return doc, nil
}

func callAIProvider(ctx context.Context, spec *AISpec, prompt string) (map[string]any, error) {
	switch spec.Provider {
	case "mock":
		if MockHandler == nil {
			return nil, errors.New("ai: mock provider selected but no mock handler registered")
		}
		return MockHandler(prompt)
	case "openai_compatible":
		key := os.Getenv(spec.APIKeyEnv)
		if key == "" {
			return nil, fmt.Errorf("ai: env %s empty", spec.APIKeyEnv)
		}
		body := map[string]any{
			"model":    spec.Model,
			"messages": []map[string]string{{"role": "user", "content": prompt}},
			"temperature": 0,
			"response_format": map[string]string{"type": "json_object"},
		}
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		maxAttempts := 1
		if spec.OnInvalid == "" || spec.OnInvalid == "retry" {
			maxAttempts = 3
		}
		var lastErr error
		for i := 0; i < maxAttempts; i++ {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(spec.Endpoint, "/")+"/chat/completions", bytes.NewReader(payload))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+key)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				lastErr = err
				continue
			}
			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			_ = resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				lastErr = fmt.Errorf("ai: provider status %d: %.200s", resp.StatusCode, respBody)
				continue
			}
			var chat struct {
				Choices []struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				} `json:"choices"`
			}
			if err := json.Unmarshal(respBody, &chat); err != nil {
				lastErr = fmt.Errorf("ai: decode provider response: %w", err)
				continue
			}
			if len(chat.Choices) == 0 {
				lastErr = errors.New("ai: provider returned no choices")
				continue
			}
			var obj map[string]any
			if err := json.Unmarshal([]byte(chat.Choices[0].Message.Content), &obj); err != nil {
				lastErr = fmt.Errorf("ai: provider content is not a JSON object: %w", err)
				continue
			}
			return obj, nil
		}
		return nil, lastErr
	default:
		return nil, fmt.Errorf("ai: unknown provider %q", spec.Provider)
	}
}

func validateAIContract(doc map[string]any, contract map[string]string) error {
	for field, want := range contract {
		v, ok := doc[field]
		if !ok {
			return fmt.Errorf("ai: response missing required field %q", field)
		}
		switch want {
		case "string":
			if _, ok := v.(string); !ok {
				return fmt.Errorf("ai: field %q must be a string", field)
			}
		case "number":
			if _, ok := toNumber(v); !ok {
				return fmt.Errorf("ai: field %q must be a number", field)
			}
		case "boolean":
			if _, ok := v.(bool); !ok {
				return fmt.Errorf("ai: field %q must be a boolean", field)
			}
		case "object":
			if _, ok := v.(map[string]any); !ok {
				return fmt.Errorf("ai: field %q must be an object", field)
			}
		case "array":
			if _, ok := v.([]any); !ok {
				return fmt.Errorf("ai: field %q must be an array", field)
			}
		}
	}
	return nil
}
