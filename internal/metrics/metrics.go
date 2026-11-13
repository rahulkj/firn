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

	// Wire the FileLog fsync observer (ADR-006).
	log.FsyncObserver = func(d time.Duration) { m.FsyncSeconds.Observe(d.Seconds()) }

	m.registry.MustRegister(m.ProduceTotal, m.FetchTotal, m.FsyncSeconds, m.GroupRebalances, m.QueueDepth, m.FederationCursorLagSeconds)
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
