package chaos

import (
	"testing"
	"time"
)

// Scenario 1: Leader Crash & Failover
// 1. Start a 3-node cluster and wait for leader election.
// 2. Commit a write ("key1" = "value1").
// 3. KILL the leader suddenly (simulate power loss).
// 4. Verify a new leader is elected by the surviving 2 nodes in < 2s.
// 5. Commit a new write ("key2" = "value2") on the new leader.
// 6. Verify "key1" survived the failover.
// 7. Reboot the old leader from disk.
// 8. Verify the old leader rejoins as a follower and catches up all keys!
func TestScenario1_LeaderCrashAndRecovery(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()
	// 1. Wait for initial leader to emerge
	oldLeader := cluster.WaitForLeader(2 * time.Second)
	oldLeaderID := oldLeader.ID()
	// 2. Propose initial write
	_, err := cluster.Propose("key1", "value1")
	if err != nil {
		t.Fatalf("failed to propose key1: %v", err)
	}
	cluster.AssertAllKVConsistent("key1", "value1", 1*time.Second)
	// 3. CHAOS: Kill the leader!
	cluster.Crash(oldLeaderID)
	// 4. Verify the surviving 2 nodes elect a NEW leader
	newLeader := cluster.WaitForLeader(2 * time.Second)
	if newLeader.ID() == oldLeaderID {
		t.Fatalf("expected new leader, but got dead leader %s", oldLeaderID)
	}
	// 5. Commit write on the new leader
	_, err = cluster.Propose("key2", "value2")
	if err != nil {
		t.Fatalf("failed proposing key2 on new leader: %v", err)
	}
	// 6. Verify key1 survived across the election
	val1, ok1 := cluster.GetKV(newLeader.ID(), "key1")
	if !ok1 || val1 != "value1" {
		t.Fatalf("expected key1 to survive leader crash, got ok=%v, val=%s", ok1, val1)
	}
	// 7. Reboot the dead leader from disk!
	cluster.Restart(oldLeaderID)
	// 8. Verify all 3 nodes converge to identical state
	cluster.AssertAllKVConsistent("key1", "value1", 2*time.Second)
	cluster.AssertAllKVConsistent("key2", "value2", 2*time.Second)
}

// Scenario 2: Follower Crash & Catch-up
// 1. Start a 3-node cluster and wait for leader.
// 2. Commit a write ("k1" = "v1").
// 3. Kill ONE follower.
// 4. Commit writes ("k2" = "v2" and "k3" = "v3") while the follower is dead.
//    (Must succeed because 2/3 nodes are alive = Majority Quorum!).
// 5. Restart the dead follower from disk.
// 6. Verify the restarted follower catches up on all missing entries!

func TestScenario2_FollowerCrashAndRecovery(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()
	leader := cluster.WaitForLeader(2 * time.Second)
	leaderID := leader.ID()
	// 1. Commit baseline write
	_, err := cluster.Propose("k1", "v1")
	if err != nil {
		t.Fatalf("failed proposing k1: %v", err)
	}
	cluster.AssertAllKVConsistent("k1", "v1", 1*time.Second)
	// 2. Pick a follower to kill
	var followerID string
	for _, id := range []string{"node1", "node2", "node3"} {
		if id != leaderID {
			followerID = id
			break
		}
	}
	// 3. CHAOS: Kill the follower!
	cluster.Crash(followerID)
	// 4. Cluster continues serving writes (2/3 quorum is alive!)
	_, err = cluster.Propose("k2", "v2")
	if err != nil {
		t.Fatalf("expected write to succeed with 2/3 quorum, got: %v", err)
	}
	_, err = cluster.Propose("k3", "v3")
	if err != nil {
		t.Fatalf("expected write to succeed with 2/3 quorum, got: %v", err)
	}
	// 5. Reboot the follower from disk
	cluster.Restart(followerID)
	// 6. Verify the rebooted follower catches up on all 3 keys!
	cluster.AssertAllKVConsistent("k1", "v1", 2*time.Second)
	cluster.AssertAllKVConsistent("k2", "v2", 2*time.Second)
	cluster.AssertAllKVConsistent("k3", "v3", 2*time.Second)
}
