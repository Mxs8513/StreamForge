// Package metrics holds the Prometheus collectors shared across processes and a
// helper to expose them. The Grafana dashboard (deploy/grafana) and the
// benchmark harness (bench/) read these.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsConsumed counts events pulled from Kafka (throughput numerator).
	EventsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamforge_events_consumed_total",
		Help: "Total events consumed from Kafka.",
	})
	// RecordsEmitted counts output rows written/staged by the sink.
	RecordsEmitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamforge_records_emitted_total",
		Help: "Total window aggregate rows emitted by the sink.",
	})
	// EventLatency is the end-to-end latency from an event's event_time to when
	// it is folded into state — the pipeline's tail-latency signal.
	EventLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "streamforge_event_latency_seconds",
		Help: "End-to-end event_time -> aggregation latency.",
		// 1ms .. ~16s, doubling — covers low-load and backlog regimes.
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})
	// ShuffleBytes counts bytes sent over the keyBy shuffle.
	ShuffleBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamforge_shuffle_bytes_total",
		Help: "Total bytes sent over the gRPC shuffle.",
	})
	// CheckpointDuration is the wall time of a full checkpoint round (coordinator).
	CheckpointDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "streamforge_checkpoint_duration_seconds",
		Help:    "Duration of a completed checkpoint round (prepare+commit).",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})
	// LastCompletedCheckpoint is the highest committed checkpoint id (coordinator).
	LastCompletedCheckpoint = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamforge_last_completed_checkpoint",
		Help: "Highest committed checkpoint id.",
	})
)

// Serve exposes /metrics on addr in a background goroutine.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() { _ = http.ListenAndServe(addr, mux) }()
}
