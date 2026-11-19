package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rahulkj/firn/internal/egress"
	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/sdk/firngo"
)

// Runner executes one pipeline: pull inputs, run the stage graph, produce
// outputs, then confirm the input position — only after outputs are durable
// (at-least-once end-to-end; duplicates possible across restarts).
type Runner struct {
	Config   *Config
	Src      egress.Source // built from Config.Input (topic or queue)
	Client   *firngo.Client
	Revision string
	Stats    *Stats
	Metrics  *integration.Metrics
	Logger   func(format string, args ...any)
}

// Run drains forever until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		batch, err := r.Src.Next(ctx)
		if err != nil {
			r.logf("pipeline %s: source error: %v (backing off)", r.Config.Name, err)
			if !sleepCtx(ctx, 500*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if len(batch) == 0 {
			continue
		}
		if r.Metrics != nil {
			r.Metrics.RecordsIn.WithLabelValues("pipeline", r.Config.Name, r.Src.Name(), "ok").Add(float64(len(batch)))
		}
		if err := r.processBatch(ctx, batch); err != nil {
			// Outputs not durable (or DLQ failed): do not confirm the input;
			// the unconfirmed batch is re-served.
			r.logf("pipeline %s: batch error: %v (will retry)", r.Config.Name, err)
			if !sleepCtx(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
}

// processBatch processes records and produces outputs, then confirms input
// offsets. Records failing stages go to the DLQ (counted) when configured;
// with on_error=fail the whole batch aborts.
func (r *Runner) processBatch(ctx context.Context, batch []egress.Delivery) error {
	revision := r.Revision
	type pendingOutput struct {
		name   string
		key    []byte
		value  []byte
		headers map[string]string
	}
	outputs := []pendingOutput{}
	for _, d := range batch {
		rec := Record{Key: d.Key, Value: d.Value, Source: r.Src.Name(), Partition: d.Partition, Offset: d.Offset, DeliveryID: d.DeliveryID}
		outRecords, err := r.Config.Process(rec, r.Stats, revision)
		if err != nil {
			var pe *ProcessError
			if asProcessError(err, &pe) && r.Config.OnError == "fail" {
				return err // abort; input not confirmed
			}
			// Dead-letter the offending record; continue the rest of the batch.
			if dlqErr := r.deadLetter(ctx, d, err); dlqErr != nil {
				return fmt.Errorf("process failed (%v) and DLQ failed: %w", err, dlqErr)
			}
			r.Stats.DLQ++
			if r.Metrics != nil {
				r.Metrics.DLQTotal.WithLabelValues("pipeline", r.Config.Name, "process").Add(1)
			}
			continue
		}
		for _, out := range outRecords {
			outputs = append(outputs, pendingOutput{name: out.Output, key: out.Key, value: out.Value, headers: out.Headers})
		}
	}
	// Produce each output (batched per output name, ≤500 records each).
	byOutput := map[string][]pendingOutput{}
	for _, o := range outputs {
		byOutput[o.name] = append(byOutput[o.name], o)
	}
	for name, recs := range byOutput {
		def := r.Config.Outputs[name]
		for start := 0; start < len(recs); start += 500 {
			end := start + 500
			if end > len(recs) {
				end = len(recs)
			}
			switch def.Type {
			case "firn_topic":
				kv := make([]firngo.KeyValue, 0, end-start)
				for _, o := range recs[start:end] {
					kv = append(kv, firngo.KeyValue{Key: o.key, Value: o.value})
				}
				produceStart := time.Now()
				if _, err := r.Client.ProduceBatch(ctx, def.Topic, kv); err != nil {
					return fmt.Errorf("produce output %q (%s): %w", name, def.Topic, err)
				}
				if r.Metrics != nil {
					r.Metrics.OpSeconds.WithLabelValues("pipeline", "produce").Observe(time.Since(produceStart).Seconds())
					r.Metrics.RecordsProduced.WithLabelValues("pipeline", r.Config.Name, name, "ok").Add(float64(end - start))
				}
			case "firn_queue":
				for _, o := range recs[start:end] {
					if _, _, err := r.Client.Produce(ctx, def.Queue, o.key, o.value); err != nil {
						return fmt.Errorf("produce output %q (queue %s): %w", name, def.Queue, err)
					}
					if r.Metrics != nil {
						r.Metrics.RecordsProduced.WithLabelValues("pipeline", r.Config.Name, name, "ok").Add(1)
					}
				}
			}
		}
	}
	// All outputs durable: advance input position.
	return r.Src.Confirm(ctx, batch)
}

func (r *Runner) deadLetter(ctx context.Context, d egress.Delivery, cause error) error {
	if r.Config.DLQ == "" {
		return fmt.Errorf("no dlq configured (on_error=dlq default)")
	}
	payload, err := json.Marshal(integration.DLQPayload{
		Connector: "pipeline:" + r.Config.Name,
		Stage:     "process",
		Position:  fmt.Sprintf("%s/%d/%d", sourceLabel(d), d.Partition, d.Offset),
		Error:     cause.Error(),
		ValueBase64: base64.StdEncoding.EncodeToString(d.Value),
	})
	if err != nil {
		return err
	}
	_, _, err = r.Client.Produce(ctx, r.Config.DLQ, d.Key, payload)
	return err
}

func sourceLabel(d egress.Delivery) string {
	if d.DeliveryID != "" {
		return "queue"
	}
	return "topic"
}

func asProcessError(err error, target **ProcessError) bool {
	for err != nil {
		if pe, ok := err.(*ProcessError); ok {
			*target = pe
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func (r *Runner) logf(format string, args ...any) {
	if r.Logger != nil {
		r.Logger(format, args...)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
