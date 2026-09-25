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
	node1, store1, _, dbpath := createBboltTestNode(t, "node", nil, "")

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
