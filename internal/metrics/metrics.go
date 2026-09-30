package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	defaultMetrics *Metrics
	defaultMu      sync.Mutex
)

// Metrics encapsulates all Prometheus metric collectors for Raftra.
type Metrics struct {
	CurrentTerm          prometheus.Gauge
	NodeRole             prometheus.Gauge
	LeaderElectionsTotal prometheus.Counter
	CommitIndex          prometheus.Gauge
	LastApplied          prometheus.Gauge
	LogEntriesTotal      prometheus.Gauge
	ReplicationLatency   prometheus.Histogram
	AppendEntriesTotal   prometheus.Counter
	RequestVoteTotal     prometheus.Counter
	KVRequestsTotal      *prometheus.CounterVec
	KVRequestDuration    *prometheus.HistogramVec
	KVStoreSize          prometheus.Gauge
	reg                  prometheus.Registerer
}
