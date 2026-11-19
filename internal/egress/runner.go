package egress

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/sdk/firngo"
)

// Runner forwards one route: pull → sink (retry w/ budget) → confirm.
// At-least-once: on crash between sink write and confirm, the batch is
// re-sent; sinks must tolerate or dedupe (file sink is idempotent by
// construction; HTTP sink carries Idempotency-Key).
type Runner struct {
	Route   Route
	Src     Source
	Sink    Sink
	Client  *firngo.Client // for DLQ produce
	Metrics *integration.Metrics
	Logger  func(format string, args ...any)
}

type lagReporter interface {
	Lag(ctx context.Context, scanCap int) (map[int32]int64, error)
	Topic() string
}

// Run drains forever until ctx is cancelled or a fatal error occurs.
func (r *Runner) Run(ctx context.Context) error {
	lagDone := make(chan struct{})
	defer close(lagDone)
	if r.Metrics != nil {
		go r.lagLoop(ctx, lagDone)
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		batch, err := r.Src.Next(ctx)
		if err != nil {
			r.logf("route %s: source error: %v (backing off)", r.Route.Name, err)
			if !sleepCtx(ctx, 500*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if len(batch) == 0 {
			continue
		}
		if r.Metrics != nil {
			r.Metrics.RecordsIn.WithLabelValues("egress", r.Route.Name, r.Src.Name(), "ok").Add(float64(len(batch)))
		}
		if err := r.deliver(ctx, batch); err != nil {
			// Deliver failed and no DLQ absorbed it: back off and retry
			// the whole batch (source position was not advanced).
			r.logf("route %s: deliver error: %v (backing off)", r.Route.Name, err)
			if !sleepCtx(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
}

func (r *Runner) deliver(ctx context.Context, batch []Delivery) error {
	var lastErr error
	for attempt := 0; attempt <= r.Route.Retry.Budget; attempt++ {
		writeStart := time.Now()
		err := r.Sink.Write(ctx, r.Route.Name, batch)
		if r.Metrics != nil {
			r.Metrics.OpSeconds.WithLabelValues("egress", "write").Observe(time.Since(writeStart).Seconds())
		}
		if err == nil {
			if r.Metrics != nil {
				r.Metrics.RecordsProduced.WithLabelValues("egress", r.Route.Name, r.Route.Sink.Type, "ok").Add(float64(len(batch)))
			}
			return r.Src.Confirm(ctx, batch)
		} else if r.Sink.IsTerminal(err) {
			lastErr = err
			break
		} else {
			lastErr = err
		}
		backoff := time.Duration(r.Route.Retry.BackoffMs) * time.Millisecond << attempt
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
	}
	// Exhausted or terminal: dead-letter the batch, then confirm.
	if r.Metrics != nil {
		r.Metrics.DLQTotal.WithLabelValues("egress", r.Route.Name, "sink").Add(float64(len(batch)))
	}
	if dlqErr := r.deadLetter(ctx, batch, lastErr); dlqErr != nil {
		return fmt.Errorf("route %s: sink failed (%v) and DLQ failed: %w", r.Route.Name, lastErr, dlqErr)
	}
	r.logf("route %s: batch dead-lettered after retries: %v", r.Route.Name, lastErr)
	return r.Src.Confirm(ctx, batch)
}

func (r *Runner) deadLetter(ctx context.Context, batch []Delivery, cause error) error {
	if r.Route.DLQ == "" {
		return errors.New("no dlq configured")
	}
	first := batch[0]
	payload, err := json.Marshal(integration.DLQPayload{
		Connector: "egress:" + r.Route.Name,
		Stage:     "sink",
		Position:  fmt.Sprintf("%s/%d/%d", firstHeadersSource(batch), first.Partition, lastOffset(batch)),
		Error:     cause.Error(),
		ValueBase64: base64.StdEncoding.EncodeToString(batch[len(batch)-1].Value),
		Headers:   map[string]string{"records": fmt.Sprintf("%d", len(batch))},
	})
	if err != nil {
		return err
	}
	_, _, err = r.Client.Produce(ctx, r.Route.DLQ, []byte(r.Route.Name), payload)
	return err
}

func firstHeadersSource(batch []Delivery) string {
	if batch[0].DeliveryID != "" {
		return "queue"
	}
	return "topic"
}

func lastOffset(batch []Delivery) int64 {
	return batch[len(batch)-1].Offset
}

func (r *Runner) logf(format string, args ...any) {
	if r.Logger != nil {
		r.Logger(format, args...)
	}
}

// lagLoop refreshes the firn_egress_lag_records gauge every 15 seconds.
func (r *Runner) lagLoop(ctx context.Context, done <-chan struct{}) {
	lag, ok := r.Src.(lagReporter)
	if !ok {
		return // queue sources: no scan-based lag
	}
	topic := lag.Topic()
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			m, err := lag.Lag(ctx, 10000)
			if err != nil {
				continue
			}
			for p, n := range m {
				r.Metrics.LagRecords.WithLabelValues(r.Route.Name, topic, fmt.Sprint(p)).Set(float64(n))
			}
		}
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

