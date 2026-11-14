// Package metrics exposes firn's Prometheus metrics (ADR-006):
//
//	firn_produce_total{topic}        counter
//	firn_fetch_total{topic}          counter
//	firn_fsync_seconds               histogram (FileLog fsync)
//	firn_group_rebalances_total      counter (groups coordinator)
//	firn_queue_depth                 gauge (pending + lease-expired messages)
//	firn_federation_cursor_lag_seconds{peer,topic,partition}  gauge (ADR-010)
//
// The package registers the internal/log FsyncObserver hook and reads the
// groups rebalance counter at scrape time. No dependencies escape this
// package into storage code.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rahulkj/firn/internal/groups"
	"github.com/rahulkj/firn/internal/log"
	"github.com/rahulkj/firn/internal/queue"
)

// Metrics holds the registered collectors. Create once at process start.
type Metrics struct {
	ProduceTotal               *prometheus.CounterVec
	FetchTotal                 *prometheus.CounterVec
	FsyncSeconds               prometheus.Histogram
	GroupRebalances            prometheus.CounterFunc
	QueueDepth                 prometheus.GaugeFunc
	FederationCursorLagSeconds *prometheus.GaugeVec
	FlushWindowRecords         prometheus.Histogram
	FlushWindowBytes           prometheus.Histogram
	FlushWindowWaitSeconds     prometheus.Histogram
	FlushWindowFailuresTotal   prometheus.Counter

	registry *prometheus.Registry
}

// New registers all collectors on a fresh registry and wires the FileLog
// fsync observer to the histogram. Do not call New twice in one process.
func New(coord *groups.Coordinator, qm *queue.Manager) *Metrics {
	m := &Metrics{
		ProduceTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_produce_total",
			Help: "Total produced messages, by topic.",
		}, []string{"topic"}),
		FetchTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_fetch_total",
			Help: "Total fetch responses, by topic.",
		}, []string{"topic"}),
		FsyncSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "firn_fsync_seconds",
			Help:    "Duration of FileLog fsyncs (append path, segment roll, flush).",
			Buckets: prometheus.DefBuckets,
		}),
		registry: prometheus.NewRegistry(),
	}
	m.GroupRebalances = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "firn_group_rebalances_total",
		Help: "Total group rebalances since process start.",
	}, func() float64 {
		if coord == nil {
			return 0
		}
		return float64(coord.Rebalances())
	})
	m.QueueDepth = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "firn_queue_depth",
		Help: "Unacked queue messages (pending + lease-expired redeliveries), across queues.",
	}, func() float64 {
		if qm == nil {
			return 0
		}
		var depth int
		for _, s := range qm.Stats() {
			depth += s.Pending + s.Redeliveries
		}
		return float64(depth)
	})
	m.FederationCursorLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "firn_federation_cursor_lag_seconds",
		Help: "Seconds since the sink was last fully caught up with this source partition; 0 = caught up now. Rises while a peer is unreachable. Inter-cluster passive replication (ADR-010).",
	}, []string{"peer", "topic", "partition"})

	// ADR-014 observability: group-commit window health.
	m.FlushWindowRecords = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "firn_flush_window_records",
		Help:    "Records per flush window (group commit). 0-row windows are expected under light load.",
		Buckets: []float64{1, 8, 32, 128, 512, 2048, 8192},
	})
	m.FlushWindowBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "firn_flush_window_bytes",
		Help:    "Bytes written per flush window.",
		Buckets: prometheus.ExponentialBuckets(1<<10, 4, 8),
	})
	m.FlushWindowWaitSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "firn_flush_window_wait_seconds",
		Help:    "Age of the oldest pending frame when the window flushed (includes timer wait + fsync).",
		Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1},
	})
	m.FlushWindowFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "firn_flush_failures_total",
		Help: "Flush-window fsync failures (whole batch rolled back, produce errors returned).",
	})

	// Wire the FileLog fsync observer (ADR-006) and the flush-window observer (ADR-014).
	log.FsyncObserver = func(d time.Duration) { m.FsyncSeconds.Observe(d.Seconds()) }
	log.FlushWindowObserver = func(records int, bytes int64, wait time.Duration, outcome error) {
		m.FlushWindowRecords.Observe(float64(records))
		m.FlushWindowBytes.Observe(float64(bytes))
		m.FlushWindowWaitSeconds.Observe(wait.Seconds())
		if outcome != nil {
			m.FlushWindowFailuresTotal.Inc()
		}
	}

	m.registry.MustRegister(m.ProduceTotal, m.FetchTotal, m.FsyncSeconds, m.GroupRebalances, m.QueueDepth, m.FederationCursorLagSeconds, m.FlushWindowRecords, m.FlushWindowBytes, m.FlushWindowWaitSeconds, m.FlushWindowFailuresTotal)
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
