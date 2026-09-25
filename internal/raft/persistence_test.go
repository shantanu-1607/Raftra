package raft

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/storage"
	pb "github.com/shantanu-1607/raftra/proto"
)

// createBboltTestNode creates a RaftNode backed by a real bbolt database on disk.
// If dbPath is empty, it generates a fresh temporary path.
// If dbPath is provided, it opens the existing database (simulating a reboot!).

func createBboltTestNode(t *testing.T, id string, peers []PeerConfig, dbPath string) (*RaftNode, *storage.BboltStore, *kvstore.KVStore, string) {
	t.Helper()

	if dbPath == "" {
		dbPath = filepath.Join(t.TempDir(), fmt.Sprintf("raft-%s.db", id))
	}

	store, err := storage.NewBboltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}

	kv := kvstore.NewKVStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := DefaultConfig(id, peers)
	cfg.ElectionTimeoutMin = 50 * time.Millisecond
	cfg.ElectionTimeoutMax = 100 * time.Millisecond
	cfg.HeartbeatInterval = 20 * time.Millisecond
	node, err := NewRaftNode(cfg, store, kv, logger)
	if err != nil {
		t.Fatalf("failed to create raft node %s: %v", id, err)
	}
	return node, store, kv, dbPath
}

func TestTermAndVoteSurviveCrash(t *testing.T) {
	node1, store1, _, dbPath := createBboltTestNode(t, "node1", nil, "")

	// 1. Advance term and cast vote
	node1.mu.Lock()
	node1.persistent.CurrentTerm = 5
	node1.persistent.VotedFor = "node2"
	_ = store1.SaveTerm(5)
	_ = store1.SaveVotedFor("node2")
	node1.mu.Unlock()

	// 2. SIMULATE CRASH: Stop node and close database
	node1.Stop()
	_ = store1.Close()

	// 3. SIMULATE REBOOT: Start a new node from the same dbPath
	recoveredNode, recoveredStore, _, _ := createBboltTestNode(t, "node1", nil, dbPath)
	defer func() {
		recoveredNode.Stop()
		_ = recoveredStore.Close()
	}()

	// 4. VERIFY TERM AND VOTE PERSISTED
	recoveredNode.mu.Lock()
	defer recoveredNode.mu.Unlock()

	if recoveredNode.persistent.CurrentTerm != 5 {
		t.Fatalf("expected term 5 after recovery, got %d", recoveredNode.persistent.CurrentTerm)
	}
	if recoveredNode.persistent.VotedFor != "node2" {
		t.Fatalf("expected votedFor 'node2' after recovery, got %s", recoveredNode.persistent.VotedFor)
	}

}

// Test Double-Voting Prevention Across Crashes
func TestNoDoubleVotingAfterRestart(t *testing.T) {
	node1, store1, _, dbPath := createBboltTestNode(t, "node1", nil, "")

	// Node 1 receives RequestVote from candidate "node2" in Term 3
	req := &pb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node1.HandleRequestVote(req)
	if !resp.VoteGranted {
		t.Fatalf("expected vote to be granted to node2")
	}

	// SIMULATE CRASH!
	node1.Stop()
	_ = store1.Close()
	// SIMULATE REBOOT!
	recoveredNode, recoveredStore, _, _ := createBboltTestNode(t, "node1", nil, dbPath)
	defer func() {
		recoveredNode.Stop()
		_ = recoveredStore.Close()
	}()

	// In the same Term 3, another candidate "node3" asks for vote
	req2 := &pb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp2 := recoveredNode.HandleRequestVote(req2)
	// MUST BE DENIED! (Safety: at most one vote per term)
	if resp2.VoteGranted {
		t.Fatalf("SAFETY VIOLATION: node voted for two different candidates in term 3!")
	}

}

// Test Log Entries Survive Crash
func TestLogSurvivesCrash(t *testing.T) {
	node1, store1, _, dbPath := createBboltTestNode(t, "node1", nil, "")
	forceLeader(node1, 1)
	// Leader proposes two commands
	idx1, err := node1.ProposeCommand(encodeSet("key1", "val1"))
	if err != nil || idx1 != 1 {
		t.Fatalf("failed proposing key1: %v", err)
	}
	idx2, err := node1.ProposeCommand(encodeSet("key2", "val2"))
	if err != nil || idx2 != 2 {
		t.Fatalf("failed proposing key2: %v", err)
	}

	// SIMULATE CRASH!
	node1.Stop()
	_ = store1.Close()
	// SIMULATE REBOOT!
	recoveredNode, recoveredStore, _, _ := createBboltTestNode(t, "node1", nil, dbPath)
	defer func() {
		recoveredNode.Stop()
		_ = recoveredStore.Close()
	}()
	recoveredNode.mu.Lock()
	defer recoveredNode.mu.Unlock()
	// Verify all log entries survived in memory and storage
	if len(recoveredNode.persistent.Log) != 3 { // Sentinel (index 0) + 2 entries = 3
		t.Fatalf("expected 3 entries in log, got %d", len(recoveredNode.persistent.Log))
	}
	e1 := recoveredNode.persistent.Log[1]
	if e1.Index != 1 || e1.Term != 1 {
		t.Fatalf("corrupted entry 1: %+v", e1)
	}
	e2 := recoveredNode.persistent.Log[2]
	if e2.Index != 2 || e2.Term != 1 {
		t.Fatalf("corrupted entry 2: %+v", e2)
	}
}

// Test Follower Crash, Re-join & Catch-up
func TestFollowerCrashAndCatchUp(t *testing.T) {
	net := newClusterNetwork()
	n1, s1, _, _ := createBboltTestNode(t, "node1", []PeerConfig{{ID: "node2"}, {ID: "node3"}}, "")
	n2, s2, _, _ := createBboltTestNode(t, "node2", []PeerConfig{{ID: "node1"}, {ID: "node3"}}, "")
	n3, s3, kv3, p3 := createBboltTestNode(t, "node3", []PeerConfig{{ID: "node1"}, {ID: "node2"}}, "")
	defer func() {
		n1.Stop()
		_ = s1.Close()
		n2.Stop()
		_ = s2.Close()
	}()
	net.registerNode(n1)
	net.registerNode(n2)
	net.registerNode(n3)
	forceLeader(n1, 1)
	// 1. Commit initial key with all 3 nodes
	_, err := n1.ProposeCommand(encodeSet("initial", "yes"))
	if err != nil {
		t.Fatalf("failed to propose initial: %v", err)
	}
	n1.sendHeartbeats()
	// Verify n3 received it
	waitFor(t, 200*time.Millisecond, func() bool {
		val, ok := kv3.Get("initial")
		return ok && val == "yes"
	}, "n3 receives initial write")
	// 2. CRASH FOLLOWER n3!
	net.isolate("node3")
	n3.Stop()
	_ = s3.Close()
	// 3. Cluster commits 2 more writes while n3 is DEAD (n1 + n2 form 2/3 quorum)
	_, _ = n1.ProposeCommand(encodeSet("during_crash", "valA"))
	_, _ = n1.ProposeCommand(encodeSet("another_key", "valB"))
	// 4. REBOOT FOLLOWER n3 from disk!
	rebootedN3, rebootedS3, rebootedKV3, _ := createBboltTestNode(t, "node3", []PeerConfig{{ID: "node1"}, {ID: "node2"}}, p3)
	defer func() {
		rebootedN3.Stop()
		_ = rebootedS3.Close()
	}()
	// Reconnect to network
	net.registerNode(rebootedN3)
	net.reconnect("node3")
	// Leader sends heartbeats carrying the new commitIndex and missing entries
	n1.sendHeartbeats()
	// 5. Verify n3 caught up and restored all 3 keys into its KV state machine!
	waitFor(t, 500*time.Millisecond, func() bool {
		v1, ok1 := rebootedKV3.Get("initial")
		v2, ok2 := rebootedKV3.Get("during_crash")
		v3, ok3 := rebootedKV3.Get("another_key")
		return ok1 && v1 == "yes" && ok2 && v2 == "valA" && ok3 && v3 == "valB"
	}, "rebooted n3 catches up all committed writes")
}

func TestExLeaderRestartsAndStepsDown(t *testing.T) {
	net := newClusterNetwork()

	n1, s1, _, p1 := createBboltTestNode(t, "node1", []PeerConfig{{ID: "node2"}, {ID: "node3"}}, "")
	n2, s2, _, _ := createBboltTestNode(t, "node2", []PeerConfig{{ID: "node1"}, {ID: "node3"}}, "")
	n3, s3, _, _ := createBboltTestNode(t, "node3", []PeerConfig{{ID: "node1"}, {ID: "node2"}}, "")

	defer func() {
		n2.Stop()
		_ = s2.Close()
		n3.Stop()
		_ = s3.Close()
	}()

	net.registerNode(n1)
	net.registerNode(n2)
	net.registerNode(n3)

	// n1 is leader in Term 1
	forceLeader(n1, 1)

	// n1 crashes!
	net.isolate("node1")
	n1.Stop()
	_ = s1.Close()

	// n2 is elected new Leader in Term 2
	forceLeader(n2, 2)
	_, _ = n2.ProposeCommand(encodeSet("new_term_key", "term2_val"))

	// n1 restarts from disk!
	rebootedN1, rebootedS1, _, _ := createBboltTestNode(t, "node1", []PeerConfig{{ID: "node2"}, {ID: "node3"}}, p1)
	defer func() {
		rebootedN1.Stop()
		_ = rebootedS1.Close()
	}()

	net.registerNode(rebootedN1)
	net.reconnect("node1")

	// New leader n2 sends heartbeat to n1
	n2.sendHeartbeats()

	// n1 must discover Term 2 and step down as Follower!
	waitFor(t, 200*time.Millisecond, func() bool {
		rebootedN1.mu.Lock()
		defer rebootedN1.mu.Unlock()
		return rebootedN1.role == Follower && rebootedN1.persistent.CurrentTerm == 2
	}, "rebooted ex-leader steps down to Follower in Term 2")
}
