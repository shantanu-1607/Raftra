package metrics

import (
	"sync"
	"time"

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

	reg prometheus.Registerer
}

// Default returns the process-wide default Metrics instance registered with prometheus.DefaultRegisterer.
func Default() *Metrics {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultMetrics == nil {
		defaultMetrics = newMetricsWithRegisterer(prometheus.DefaultRegisterer)
	}
	return defaultMetrics
}

// NewMetrics creates or returns a Metrics instance.
// If reg is nil or prometheus.DefaultRegisterer, the default singleton is returned.
// Otherwise, fresh collectors are registered with the provided Registerer.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil || reg == prometheus.DefaultRegisterer {
		return Default()
	}
	return newMetricsWithRegisterer(reg)
}

func newMetricsWithRegisterer(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		reg: reg,

		CurrentTerm: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "raft_current_term",
			Help: "Current Raft term of this node.",
		}),

		NodeRole: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "raft_node_role",
			Help: "Current Raft role of this node: 0=Follower, 1=Candidate, 2=Leader.",
		}),

		LeaderElectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raft_leader_elections_total",
			Help: "Total number of Raft leader elections initiated by this node.",
		}),

		CommitIndex: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "raft_commit_index",
			Help: "Current Raft log commit index.",
		}),

		LastApplied: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "raft_last_applied",
			Help: "Highest Raft log index applied to the state machine.",
		}),

		LogEntriesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "raft_log_entries_total",
			Help: "Total number of log entries currently stored in the Raft log.",
		}),

		ReplicationLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "raft_replication_latency_seconds",
			Help:    "Latency in seconds for a leader proposal to replicate to a majority quorum.",
			Buckets: []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.000, 2.500, 5.000},
		}),

		AppendEntriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raft_append_entries_total",
			Help: "Total number of AppendEntries RPCs sent by this node.",
		}),

		RequestVoteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raft_request_vote_total",
			Help: "Total number of RequestVote RPCs sent by this node.",
		}),

		KVRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_requests_total",
			Help: "Total number of Key-Value requests processed, partitioned by operation type.",
		}, []string{"type"}),

		KVRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kv_request_duration_seconds",
			Help:    "Latency distribution of Key-Value requests in seconds, partitioned by operation type.",
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.000},
		}, []string{"type"}),

		KVStoreSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kv_store_size",
			Help: "Number of active keys currently in the replicated in-memory KV store.",
		}),
	}

	reg.MustRegister(
		m.CurrentTerm,
		m.NodeRole,
		m.LeaderElectionsTotal,
		m.CommitIndex,
		m.LastApplied,
		m.LogEntriesTotal,
		m.ReplicationLatency,
		m.AppendEntriesTotal,
		m.RequestVoteTotal,
		m.KVRequestsTotal,
		m.KVRequestDuration,
		m.KVStoreSize,
	)

	return m
}

// SetCurrentTerm updates the current Raft term gauge.
func (m *Metrics) SetCurrentTerm(term uint64) {
	if m == nil {
		return
	}
	m.CurrentTerm.Set(float64(term))
}

// SetNodeRole updates the node role gauge (0=Follower, 1=Candidate, 2=Leader).
func (m *Metrics) SetNodeRole(role int) {
	if m == nil {
		return
	}
	m.NodeRole.Set(float64(role))
}

// IncLeaderElections increments the total leader elections counter.
func (m *Metrics) IncLeaderElections() {
	if m == nil {
		return
	}
	m.LeaderElectionsTotal.Inc()
}

// SetCommitIndex updates the commit index gauge.
func (m *Metrics) SetCommitIndex(idx uint64) {
	if m == nil {
		return
	}
	m.CommitIndex.Set(float64(idx))
}

// SetLastApplied updates the last applied index gauge.
func (m *Metrics) SetLastApplied(idx uint64) {
	if m == nil {
		return
	}
	m.LastApplied.Set(float64(idx))
}

// SetLogEntriesTotal updates the total log entries gauge.
func (m *Metrics) SetLogEntriesTotal(count int) {
	if m == nil {
		return
	}
	m.LogEntriesTotal.Set(float64(count))
}

// ObserveReplicationLatency records the replication latency of a proposal.
func (m *Metrics) ObserveReplicationLatency(d time.Duration) {
	if m == nil {
		return
	}
	m.ReplicationLatency.Observe(d.Seconds())
}

// IncAppendEntriesTotal increments the AppendEntries RPC count.
func (m *Metrics) IncAppendEntriesTotal() {
	if m == nil {
		return
	}
	m.AppendEntriesTotal.Inc()
}

// IncRequestVoteTotal increments the RequestVote RPC count.
func (m *Metrics) IncRequestVoteTotal() {
	if m == nil {
		return
	}
	m.RequestVoteTotal.Inc()
}

// IncKVRequests increments the KV request counter for a given operation type ("put", "get", "delete").
func (m *Metrics) IncKVRequests(opType string) {
	if m == nil {
		return
	}
	m.KVRequestsTotal.WithLabelValues(opType).Inc()
}

// ObserveKVRequestDuration records the request processing duration for a given operation type.
func (m *Metrics) ObserveKVRequestDuration(opType string, d time.Duration) {
	if m == nil {
		return
	}
	m.KVRequestDuration.WithLabelValues(opType).Observe(d.Seconds())
}

// SetKVStoreSize updates the number of keys in the KV store gauge.
func (m *Metrics) SetKVStoreSize(size int) {
	if m == nil {
		return
	}
	m.KVStoreSize.Set(float64(size))
}
