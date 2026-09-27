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

// Scenario 3: Minority Partition (2 vs 1)
// 1. Start a 3-node cluster and wait for leader.
// 2. Commit a baseline write ("k1" = "v1").
// 3. Partition 1 follower into an isolated island (2 connected, 1 isolated).
// 4. Propose a write ("k2" = "v2") on the majority partition -> Must SUCCEED!
// 5. Attempt a write directly on the isolated follower -> Must FAIL (not a leader)!
// 6. Verify the isolated follower does NOT have "k2".
// 7. Heal the partition.
// 8. Verify the healed follower catches up and all nodes agree on both keys!
func TestScenario3_MinorityPartition(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()

	leader := cluster.WaitForLeader(2 * time.Second)
	leaderID := leader.ID()

	// 1. Baseline write
	_, err := cluster.Propose("k1", "v1")
	if err != nil {
		t.Fatalf("failed baseline propose: %v", err)
	}
	cluster.AssertAllKVConsistent("k1", "v1", 1*time.Second)

	// 2. Identify a follower to isolate
	var followerID string
	for _, id := range []string{"node1", "node2", "node3"} {
		if id != leaderID {
			followerID = id
			break
		}
	}

	// 3. CHAOS: Cut all network cables to this follower!
	cluster.Partition(followerID)

	// 4. Majority partition (leader + 1 surviving follower = 2/3) commits new writes
	_, err = cluster.Propose("k2", "v2")
	if err != nil {
		t.Fatalf("majority partition failed to commit write: %v", err)
	}

	// 5. Verify the isolated follower rejects direct client writes
	_, err = cluster.ProposeOnNode(followerID, "isolated_key", "isolated_val")
	if err == nil {
		t.Fatalf("expected isolated follower to reject client proposal, but it succeeded")
	}

	// 6. Verify the isolated follower does NOT have "k2" in its state machine
	val2, ok2 := cluster.GetKV(followerID, "k2")
	if ok2 {
		t.Fatalf("isolated node unexpectedly received k2=%q while cut off", val2)
	}

	// 7. Heal the network partition (plug cables back in!)
	cluster.Heal()

	// 8. Verify the isolated node caught up on k2 and cluster is 100% consistent
	cluster.AssertAllKVConsistent("k1", "v1", 2*time.Second)
	cluster.AssertAllKVConsistent("k2", "v2", 2*time.Second)
}

// Scenario 4: Leader Partition & Split-Brain Prevention
// 1. Start a 3-node cluster and commit "key1" = "value1".
// 2. Isolate the CURRENT leader from the other 2 nodes.
// 3. The 2 connected nodes elect a NEW leader for a higher term.
// 4. Commit "key2" = "value2" on the new leader.
// 5. Verify the old isolated leader DOES NOT have "key2".
// 6. Heal the network partition.
// 7. Verify the old leader steps down to follower and catches up on "key2"!
func TestScenario4_LeaderPartitionSplitBrain(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()

	// 1. Identify initial leader and commit baseline
	oldLeader := cluster.WaitForLeader(2 * time.Second)
	oldLeaderID := oldLeader.ID()

	_, err := cluster.Propose("key1", "value1")
	if err != nil {
		t.Fatalf("failed baseline propose: %v", err)
	}
	cluster.AssertAllKVConsistent("key1", "value1", 1*time.Second)

	// 2. CHAOS: Isolate the leader into a minority island of 1!
	cluster.Partition(oldLeaderID)

	// 3. The majority side (2 surviving nodes) must elect a NEW leader
	newLeader := cluster.WaitForLeader(3 * time.Second)
	if newLeader.ID() == oldLeaderID {
		t.Fatalf("expected new leader from majority partition, but got isolated leader %s", oldLeaderID)
	}

	// 4. Commit a new write on the majority partition's new leader
	_, err = cluster.Propose("key2", "value2")
	if err != nil {
		t.Fatalf("failed proposing key2 on new leader: %v", err)
	}

	// 5. Verify the new leader applied "key2"
	val2, ok2 := cluster.GetKV(newLeader.ID(), "key2")
	if !ok2 || val2 != "value2" {
		t.Fatalf("expected new leader to have key2=value2, got ok=%v, val=%s", ok2, val2)
	}

	// 6. Verify the old isolated leader did NOT receive "key2"
	_, oldHasKey2 := cluster.GetKV(oldLeaderID, "key2")
	if oldHasKey2 {
		t.Fatalf("isolated old leader unexpectedly received key2 while partitioned!")
	}

	// 7. Heal the partition: old leader reconnects with the cluster
	cluster.Heal()

	// 8. Verify the old leader discovers the new leader, steps down, and synchronizes!
	cluster.AssertAllKVConsistent("key1", "value1", 2*time.Second)
	cluster.AssertAllKVConsistent("key2", "value2", 2*time.Second)
}
