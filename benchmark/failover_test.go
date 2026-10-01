package benchmark

import (
	"fmt"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/test/chaos"
)

// TestFailoverTimeMeasurement measures the time it takes for the cluster to detect
// leader failure, elect a new leader, and successfully commit a new write.
// Per Phase 7B specification, it executes 10 consecutive trials and reports
// min, max, and average failover duration (target: < 2.0 seconds).
func TestFailoverTimeMeasurement(t *testing.T) {
	const totalRuns = 10
	var durations []time.Duration
	var totalDuration time.Duration

	t.Logf("\n=======================================================")
	t.Logf("   Raftra Failover Time Benchmark (%d Runs)", totalRuns)
	t.Logf("=======================================================")

	for run := 1; run <= totalRuns; run++ {
		cluster := chaos.NewTestCluster(t, 3)
		cluster.Start()

		// 1. Wait for initial leader
		oldLeader := cluster.WaitForLeader(3 * time.Second)
		if oldLeader == nil {
			cluster.Stop()
			t.Fatalf("Run %d: Failed to elect initial leader", run)
		}

		// 2. Warm up with an initial write
		if _, err := cluster.Propose("init-key", "init-val"); err != nil {
			cluster.Stop()
			t.Fatalf("Run %d: Initial write failed: %v", run, err)
		}

		// 3. Record crash time T1 and immediately kill leader
		t1 := time.Now()
		cluster.Crash(oldLeader.ID())

		// 4. Attempt to write to the cluster until the new leader accepts and commits
		failoverKey := fmt.Sprintf("failover-key-%d", run)
		failoverVal := fmt.Sprintf("failover-val-%d", run)

		_, err := cluster.Propose(failoverKey, failoverVal)
		t2 := time.Now()

		if err != nil {
			cluster.Stop()
			t.Fatalf("Run %d: Cluster failed to recover and accept write: %v", run, err)
		}

		// 5. Failover time = T2 - T1
		failoverDuration := t2.Sub(t1)
		durations = append(durations, failoverDuration)
		totalDuration += failoverDuration

		t.Logf("  Run %2d: Crash leader [%s] → New leader committed write in %v",
			run, oldLeader.ID(), failoverDuration.Round(time.Millisecond))

		cluster.Stop()
	}

	// Calculate statistics
	minDuration := durations[0]
	maxDuration := durations[0]
	for _, d := range durations {
		if d < minDuration {
			minDuration = d
		}
		if d > maxDuration {
			maxDuration = d
		}
	}
	avgDuration := totalDuration / totalRuns

	t.Logf("\n-------------------------------------------------------")
	t.Logf("  Failover Results Summary:")
	t.Logf("    Trials:   %d", totalRuns)
	t.Logf("    Min:      %v", minDuration.Round(time.Millisecond))
	t.Logf("    Max:      %v", maxDuration.Round(time.Millisecond))
	t.Logf("    Average:  %v", avgDuration.Round(time.Millisecond))
	t.Logf("=======================================================\n")

	// Phase 7 verification requirement: Failover time < 2 seconds consistently
	if avgDuration > 2*time.Second {
		t.Fatalf("Average failover time %v exceeded 2.0s requirement", avgDuration)
	}
}
