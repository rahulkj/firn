// Package metrics exposes eventing's Prometheus metrics (ADR-006):
//
//	eventing_produce_total{topic}        counter
//	eventing_fetch_total{topic}          counter
//	eventing_fsync_seconds               histogram (FileLog fsync)
//	eventing_group_rebalances_total      counter (groups coordinator)
//	eventing_queue_depth                 gauge (pending + lease-expired messages)
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

	"github.com/rahulkj/eventing/internal/groups"
	"github.com/rahulkj/eventing/internal/log"
	"github.com/rahulkj/eventing/internal/queue"
)

// Metrics holds the registered collectors. Create once at process start.
type Metrics struct {
	ProduceTotal    *prometheus.CounterVec
	FetchTotal      *prometheus.CounterVec
	FsyncSeconds    prometheus.Histogram
	GroupRebalances prometheus.CounterFunc
	QueueDepth      prometheus.GaugeFunc

	registry *prometheus.Registry
}

// New registers all collectors on a fresh registry and wires the FileLog
// fsync observer to the histogram. Do not call New twice in one process.
func New(coord *groups.Coordinator, qm *queue.Manager) *Metrics {
	m := &Metrics{
		ProduceTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventing_produce_total",
			Help: "Total produced messages, by topic.",
		}, []string{"topic"}),
		FetchTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "eventing_fetch_total",
			Help: "Total fetch responses, by topic.",
		}, []string{"topic"}),
		FsyncSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "eventing_fsync_seconds",
			Help:    "Duration of FileLog fsyncs (append path, segment roll, flush).",
			Buckets: prometheus.DefBuckets,
		}),
		registry: prometheus.NewRegistry(),
	}
	m.GroupRebalances = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "eventing_group_rebalances_total",
		Help: "Total group rebalances since process start.",
	}, func() float64 {
		if coord == nil {
			return 0
		}
		return float64(coord.Rebalances())
	})
	m.QueueDepth = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "eventing_queue_depth",
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

	// Wire the FileLog fsync observer (ADR-006).
	log.FsyncObserver = func(d time.Duration) { m.FsyncSeconds.Observe(d.Seconds()) }

	m.registry.MustRegister(m.ProduceTotal, m.FetchTotal, m.FsyncSeconds, m.GroupRebalances, m.QueueDepth)
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
