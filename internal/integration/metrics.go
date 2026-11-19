package integration

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the shared Prometheus collectors for integration binaries
// (firn-ingest, firn-egress, firn-pipeline). Create once at startup with
// NewMetrics(component) where component is "ingest", "egress", or "pipeline".
type Metrics struct {
	RecordsIn       *prometheus.CounterVec // component, route/connector, source(topic/queue), result(ok|dlq|error)
	RecordsProduced *prometheus.CounterVec // component, route, output(topic/queue/sink type), result
	DLQTotal        *prometheus.CounterVec // component, route, stage
	OpSeconds       *prometheus.HistogramVec // component, op (produce|fetch|write|deliver|pull|stage:<name>)
	LagRecords      *prometheus.GaugeVec   // egress only: committed-to-HWM backlog estimate per route/topic/partition
	ConnectionsActive *prometheus.GaugeVec // ingest only: open MQTT connections

	registry *prometheus.Registry
}

// NewMetrics registers the collectors on a fresh registry.
func NewMetrics(component string) *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry()}
	m.RecordsIn = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firn_integration_records_in_total",
		Help: "Records consumed from sources, by result.",
	}, []string{"component", "route", "source", "result"})
	m.RecordsProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firn_integration_records_produced_total",
		Help: "Records produced to destinations, by result.",
	}, []string{"component", "route", "output", "result"})
	m.DLQTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firn_integration_dlq_total",
		Help: "Records dead-lettered, per stage.",
	}, []string{"component", "route", "stage"})
	m.OpSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "firn_integration_op_seconds",
		Help:    "Latency of integration operations.",
		Buckets: prometheus.DefBuckets,
	}, []string{"component", "op"})
	m.LagRecords = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "firn_egress_lag_records",
		Help: "Backlog estimate: committed group offset vs scanned HWM for the route/partition.",
	}, []string{"route", "topic", "partition"})
	m.ConnectionsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "firn_integration_connections_active",
		Help: "Open connections (ingress servers).",
	}, []string{"component", "listener"})
	m.registry.MustRegister(m.RecordsIn, m.RecordsProduced, m.DLQTotal, m.OpSeconds, m.LagRecords, m.ConnectionsActive)
	return m
}

// Handler exposes /metrics on a private listener.
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	return mux
}
