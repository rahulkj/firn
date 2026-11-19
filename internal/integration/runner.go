package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rahulkj/firn/sdk/firngo"
)

// AckableSource is implemented by sources whose messages must be confirmed
// AFTER Firn accepts the batch (AMQP manual ack, JetStream AckExplicit,
// XACK). Ack is called by the runner before the checkpoint advances.
type AckableSource interface {
	Ack(ctx context.Context, records []Record) error
}

// Runner drains a SourceConnector into one Firn topic using the client API.
// Semantics (ADR-016): at-least-once. Checkpoints advance only after the
// produce to Firn is acknowledged; a crash between produce and checkpoint
// replays the window (duplicates possible, loss impossible).
type Runner struct {
	Connector SourceConnector
	Client    *firngo.Client
	Topic     string
	DLQ       string // optional dead-letter topic (empty = park on failure)
	BatchSize int    // max records per ProduceBatch (default 500)
	IdleDelay time.Duration
	Metrics   *Metrics // nil = no-op
	Logger    func(format string, args ...any)
}

// RunUntilEmpty drains the source until a FetchBatch returns no records.
// Intended for tests and one-shot bridges; long-running services should use Run.
func (r *Runner) RunUntilEmpty(ctx context.Context, checkpoints CheckpointStore) error {
	for {
		n, err := r.step(ctx, checkpoints)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// Run polls the source forever until ctx is cancelled.
func (r *Runner) Run(ctx context.Context, checkpoints CheckpointStore) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := r.step(ctx, checkpoints)
		if err != nil {
			r.logf("connector %s: %v (backing off)", r.Connector.Name(), err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		if n == 0 {
			delay := r.IdleDelay
			if delay <= 0 {
				delay = 200 * time.Millisecond
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
}

func (r *Runner) logf(format string, args ...any) {
	if r.Logger != nil {
		r.Logger(format, args...)
	}
}

func (r *Runner) step(ctx context.Context, checkpoints CheckpointStore) (int, error) {
	pos, err := checkpoints.Load(ctx, r.Connector.Name())
	if err != nil {
		return 0, err
	}
	max := r.BatchSize
	if max <= 0 || max > 500 {
		max = 500
	}
	fetchStart := time.Now()
	recs, err := r.Connector.FetchBatch(ctx, pos, max)
	if r.Metrics != nil {
		r.Metrics.OpSeconds.WithLabelValues("ingest", "fetch").Observe(time.Since(fetchStart).Seconds())
	}
	if err != nil {
		return 0, fmt.Errorf("fetch: %w", err)
	}
	if len(recs) == 0 {
		return 0, nil
	}
	if r.Metrics != nil {
		r.Metrics.RecordsIn.WithLabelValues("ingest", r.Connector.Name(), r.Topic, "ok").Add(float64(len(recs)))
	}
	if err := r.produce(ctx, recs); err != nil {
		return 0, err
	}
	if a, ok := r.Connector.(AckableSource); ok {
		if err := a.Ack(ctx, recs); err != nil {
			return 0, fmt.Errorf("ack: %w", err)
		}
	}
	last := recs[len(recs)-1].Position
	if last != "" {
		if err := checkpoints.Store(ctx, r.Connector.Name(), last); err != nil {
			return 0, fmt.Errorf("checkpoint: %w", err)
		}
	}
	return len(recs), nil
}

func (r *Runner) produce(ctx context.Context, recs []Record) error {
	kv := make([]firngo.KeyValue, 0, len(recs))
	for _, rec := range recs {
		kv = append(kv, firngo.KeyValue{Key: rec.Key, Value: rec.Value})
	}
	if _, err := r.Client.ProduceBatch(ctx, r.Topic, kv); err == nil {
		if r.Metrics != nil {
			r.Metrics.RecordsProduced.WithLabelValues("ingest", r.Connector.Name(), r.Topic, "ok").Add(float64(len(recs)))
		}
		return nil
	} else {
		r.logf("batch produce failed (%v); falling back to per-record produce to isolate poison records", err)
	}
	// Fallback: isolate poison records per-record; transient per-record
	// failures are retried here a bounded number of times.
	for i, rec := range recs {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			if _, _, err := r.Client.Produce(ctx, r.Topic, rec.Key, rec.Value); err == nil {
				lastErr = nil
				break
			} else {
				lastErr = err
				time.Sleep(50 * time.Millisecond)
			}
		}
		if lastErr != nil {
			if dlqErr := r.deadLetter(ctx, rec, "produce", lastErr); dlqErr != nil {
				return fmt.Errorf("record %d: produce failed (%v) and DLQ failed: %w", i, lastErr, dlqErr)
			}
			if r.Metrics != nil {
				r.Metrics.DLQTotal.WithLabelValues("ingest", r.Connector.Name(), "produce").Add(1)
			}
			r.logf("record %d dead-lettered: %v", i, lastErr)
		}
	}
	return nil
}

func (r *Runner) deadLetter(ctx context.Context, rec Record, stage string, cause error) error {
	if r.DLQ == "" {
		return fmt.Errorf("no DLQ configured; cannot dead-letter: %w", cause)
	}
	payload, err := json.Marshal(DLQPayload{
		Connector:   r.Connector.Name(),
		Stage:       stage,
		Position:    rec.Position,
		Error:       cause.Error(),
		Headers:     rec.Headers,
		ValueBase64: base64.StdEncoding.EncodeToString(rec.Value),
	})
	if err != nil {
		return err
	}
	_, _, err = r.Client.Produce(ctx, r.DLQ, []byte(r.Connector.Name()), payload)
	return err
}
