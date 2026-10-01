package benchmark

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/test/chaos"
)

// BenchmarkSetOperation measures the end-to-end throughput and latency of
// quorum-replicated writes through the Raft consensus engine and bbolt persistence.
func BenchmarkSetOperation(b *testing.B) {
	cluster := chaos.NewTestCluster(b, 3)
	cluster.Start()
	defer cluster.Stop()

	// Wait for leader election before starting benchmark timer
	cluster.WaitForLeader(3 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)
		val := fmt.Sprintf("val-%d", i)
		if _, err := cluster.Propose(key, val); err != nil {
			b.Fatalf("Propose failed at op %d: %v", i, err)
		}
	}
}

// BenchmarkGetOperation measures in-memory read throughput from the replicated
// state machine on the cluster leader.
func BenchmarkGetOperation(b *testing.B) {
	cluster := chaos.NewTestCluster(b, 3)
	cluster.Start()
	defer cluster.Stop()

	leader := cluster.WaitForLeader(3 * time.Second)

	// Pre-populate key before timing
	const targetKey = "bench-read-target"
	const targetVal = "bench-read-value"
	if _, err := cluster.Propose(targetKey, targetVal); err != nil {
		b.Fatalf("failed to seed target key: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		val, found := cluster.GetKV(leader.ID(), targetKey)
		if !found || val != targetVal {
			b.Fatalf("GetKV failed: found=%v, val=%s", found, val)
		}
	}
}

// BenchmarkMixedWorkload simulates a realistic production workload consisting of
// 80% in-memory reads and 20% consensus-replicated writes across concurrent clients.
func BenchmarkMixedWorkload(b *testing.B) {
	cluster := chaos.NewTestCluster(b, 3)
	cluster.Start()
	defer cluster.Stop()

	leader := cluster.WaitForLeader(3 * time.Second)

	// Pre-populate initial keys
	const keyCount = 100
	for i := 0; i < keyCount; i++ {
		_, _ = cluster.Propose(fmt.Sprintf("seed-%d", i), fmt.Sprintf("val-%d", i))
	}

	var opCount int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&opCount, 1)
			if idx%5 == 0 {
				// 20% Writes: full Raft quorum replication
				k := fmt.Sprintf("mixed-w-%d", idx)
				v := fmt.Sprintf("data-%d", idx)
				_, _ = cluster.Propose(k, v)
			} else {
				// 80% Reads: fast in-memory KV lookup
				k := fmt.Sprintf("seed-%d", idx%keyCount)
				_, _ = cluster.GetKV(leader.ID(), k)
			}
		}
	})
}

// BenchmarkReplicationLatency measures the exact duration from proposal to
// quorum acknowledgment and state machine commit across all nodes.
func BenchmarkReplicationLatency(b *testing.B) {
	cluster := chaos.NewTestCluster(b, 3)
	cluster.Start()
	defer cluster.Stop()

	cluster.WaitForLeader(3 * time.Second)

	var totalLatency time.Duration

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("lat-%d", i)
		val := fmt.Sprintf("lat-val-%d", i)

		start := time.Now()
		if _, err := cluster.Propose(key, val); err != nil {
			b.Fatalf("Propose failed: %v", err)
		}
		totalLatency += time.Since(start)
	}

	if b.N > 0 {
		avgMicroseconds := float64(totalLatency.Microseconds()) / float64(b.N)
		b.ReportMetric(avgMicroseconds, "us/quorum-commit")
	}
}
