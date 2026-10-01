package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_CustomRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	// 1. Exercise all metric setters and observers
	m.SetCurrentTerm(5)
	m.SetNodeRole(2)
	m.IncLeaderElections()
	m.SetCommitIndex(10)
	m.SetLastApplied(10)
	m.SetLogEntriesTotal(12)
	m.ObserveReplicationLatency(15 * time.Millisecond)
	m.IncAppendEntriesTotal()
	m.IncRequestVoteTotal()
	m.IncKVRequests("put")
	m.ObserveKVRequestDuration("put", 5*time.Millisecond)
	m.SetKVStoreSize(42)

	// 2. Scrape/gather metrics from the test registry
	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	expectedMetrics := map[string]bool{
		"raft_current_term":                false,
		"raft_node_role":                   false,
		"raft_leader_elections_total":      false,
		"raft_commit_index":                false,
		"raft_last_applied":                false,
		"raft_log_entries_total":           false,
		"raft_replication_latency_seconds": false,
		"raft_append_entries_total":        false,
		"raft_request_vote_total":          false,
		"kv_requests_total":                false,
		"kv_request_duration_seconds":      false,
		"kv_store_size":                    false,
	}

	for _, mf := range metricFamilies {
		if _, ok := expectedMetrics[mf.GetName()]; ok {
			expectedMetrics[mf.GetName()] = true
		}
	}

	for name, found := range expectedMetrics {
		if !found {
			t.Errorf("expected metric %s was not found in gathered metrics", name)
		}
	}
}

func TestMetrics_NilSafety(t *testing.T) {
	var m *Metrics
	// Verify calling methods on a nil pointer does not panic
	m.SetCurrentTerm(1)
	m.SetNodeRole(1)
	m.IncLeaderElections()
	m.SetCommitIndex(1)
	m.SetLastApplied(1)
	m.SetLogEntriesTotal(1)
	m.ObserveReplicationLatency(time.Second)
	m.IncAppendEntriesTotal()
	m.IncRequestVoteTotal()
	m.IncKVRequests("get")
	m.ObserveKVRequestDuration("get", time.Second)
	m.SetKVStoreSize(1)
}
