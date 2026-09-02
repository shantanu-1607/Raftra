package raft

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/storage"
	pb "github.com/shantanu-1607/raftra/proto"
)

// mockTransport implements the raft.Transport interface for in-memory unit tests
type mockTransport struct {
	sendVoteFunc   func(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	sendAppendFunc func(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

func (m *mockTransport) SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if m.sendVoteFunc != nil {
		return m.sendVoteFunc(peerID, req)
	}
	return &pb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (m *mockTransport) SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if m.sendAppendFunc != nil {
		return m.sendAppendFunc(peerID, req)
	}
	return &pb.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (m *mockTransport) Close() error {
	return nil
}

// helper function to create a test node with silent logger
func createTestNode(id string, peers []PeerConfig) (*RaftNode, *storage.MemoryStore) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewMemoryStore()
	kv := kvstore.NewKVStore()
	cfg := DefaultConfig(id, peers)
	cfg.ElectionTimeoutMin = 50 * time.Millisecond
	cfg.ElectionTimeoutMax = 100 * time.Millisecond
	cfg.HeartbeatInterval = 20 * time.Millisecond

	node, _ := NewRaftNode(cfg, store, kv, logger)
	return node, store
}

// 1. A single node cluster should trivially elect itself leader
func TestSingleNodeBecomesLeader(t *testing.T) {
	node, _ := createTestNode("node1", nil)
	node.SetTransport(&mockTransport{})

	node.Start()
	defer node.Stop()

	// Wait for election timeout to fire
	time.Sleep(150 * time.Millisecond)

	if node.Role() != Leader {
		t.Fatalf("expected node to become Leader, got %v", node.Role())
	}
	if node.Term() < 1 {
		t.Fatalf("expected term >= 1, got %d", node.Term())
	}
}

// 2. Reject vote requests with stale terms
func TestVoteDeniedForStaleTerm(t *testing.T) {
	node, store := createTestNode("node1", []PeerConfig{{ID: "node2"}})
	_ = store.SaveTerm(2)
	node.persistent.CurrentTerm = 2

	// Candidate on Term 1 asks for vote
	req := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.HandleRequestVote(req)
	if resp.VoteGranted {
		t.Fatalf("expected vote to be denied for stale term 1 when current term is 2")
	}
	if resp.Term != 2 {
		t.Fatalf("expected returned term to be 2, got %d", resp.Term)
	}
}

// 3. Node should not vote twice in the same term
func TestNoDoubleVoting(t *testing.T) {
	node, _ := createTestNode("node1", []PeerConfig{{ID: "node2"}, {ID: "node3"}})

	// First vote request from node2 for Term 1
	req1 := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp1 := node.HandleRequestVote(req1)
	if !resp1.VoteGranted {
		t.Fatalf("expected first vote for node2 to be granted")
	}

	// Second vote request from node3 for the same Term 1
	req2 := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp2 := node.HandleRequestVote(req2)
	if resp2.VoteGranted {
		t.Fatalf("expected second vote in same term to be denied (double voting violation)")
	}
}

// 4. Deny vote if candidate's log is less up-to-date
func TestVoteDeniedForStaleLog(t *testing.T) {
	node, store := createTestNode("node1", []PeerConfig{{ID: "node2"}})

	// Give voter an entry at Term 2, Index 3
	_ = store.AppendEntries([]*pb.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})
	_ = store.SaveTerm(2)
	node.persistent.CurrentTerm = 2

	// Candidate has an older term in its last log entry (Term 1, Index 3)
	req := &pb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node2",
		LastLogIndex: 3,
		LastLogTerm:  1, // Stale term!
	}

	resp := node.HandleRequestVote(req)
	if resp.VoteGranted {
		t.Fatalf("expected vote to be denied for candidate with stale log term")
	}
}

// 5. Candidate reverts to Follower upon seeing higher term
func TestCandidateRevertsOnHigherTerm(t *testing.T) {
	node, _ := createTestNode("node1", []PeerConfig{{ID: "node2"}})
	node.role = Candidate
	node.persistent.CurrentTerm = 2

	// Leader sends heartbeat with Term 3
	heartbeat := &pb.AppendEntriesRequest{
		Term:     3,
		LeaderId: "node2",
	}

	resp := node.HandleAppendEntries(heartbeat)
	if !resp.Success {
		t.Fatalf("expected heartbeat to be accepted")
	}
	if node.Role() != Follower {
		t.Fatalf("expected candidate to step down to Follower, got %v", node.Role())
	}
	if node.Term() != 3 {
		t.Fatalf("expected term to update to 3, got %d", node.Term())
	}
}

// 6. Valid heartbeat resets the follower's election timer
func TestHeartbeatResetsTimer(t *testing.T) {
	node, _ := createTestNode("node1", []PeerConfig{{ID: "node2"}})
	node.SetTransport(&mockTransport{})
	node.Start()
	defer node.Stop()

	// Repeatedly send heartbeats every 25ms to keep node as Follower
	for i := 0; i < 5; i++ {
		time.Sleep(25 * time.Millisecond)
		node.HandleAppendEntries(&pb.AppendEntriesRequest{
			Term:     node.Term(),
			LeaderId: "node2",
		})
	}

	// Node should still be Follower because heartbeats kept it calm
	if node.Role() != Follower {
		t.Fatalf("expected node to remain Follower under continuous heartbeats, got %v", node.Role())
	}
}

// 7. Log Up-To-Date check: When terms are equal, longer log index wins (§5.4.1)
func TestLogUpToDateTieBreaker(t *testing.T) {
	node, store := createTestNode("node1", []PeerConfig{{ID: "node2"}})

	// Voter has 3 entries, all at Term 1 (LastLogIndex = 3, LastLogTerm = 1)
	_ = store.AppendEntries([]*pb.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	_ = store.SaveTerm(1)
	node.persistent.CurrentTerm = 1

	// Case A: Candidate has same term (Term 1) but SHORTER log (Index 2) -> DENY
	reqShorter := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node2",
		LastLogIndex: 2,
		LastLogTerm:  1,
	}
	respA := node.HandleRequestVote(reqShorter)
	if respA.VoteGranted {
		t.Fatalf("expected vote denied when candidate log is shorter in same term")
	}

	// Case B: Candidate has same term (Term 1) and EQUAL log length (Index 3) -> GRANT
	reqEqual := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node2",
		LastLogIndex: 3,
		LastLogTerm:  1,
	}
	respB := node.HandleRequestVote(reqEqual)
	if !respB.VoteGranted {
		t.Fatalf("expected vote granted when candidate log has equal term and length")
	}
}

// 8. Follower rejects heartbeats from stale/deposed leaders (§5.1)
func TestRejectAppendEntriesFromStaleLeader(t *testing.T) {
	node, store := createTestNode("node1", []PeerConfig{{ID: "node2"}})
	_ = store.SaveTerm(5)
	node.persistent.CurrentTerm = 5

	// Old leader tries to send heartbeat from Term 4
	staleHeartbeat := &pb.AppendEntriesRequest{
		Term:     4,
		LeaderId: "node2",
	}

	resp := node.HandleAppendEntries(staleHeartbeat)
	if resp.Success {
		t.Fatalf("expected follower to reject heartbeat from stale leader on term 4 when follower is on term 5")
	}
	if resp.Term != 5 {
		t.Fatalf("expected returned term to be follower's current term 5, got %d", resp.Term)
	}
}

// 9. Candidate increments term on repeated election timeouts
func TestTermIncrementsAcrossElections(t *testing.T) {
	node, _ := createTestNode("node1", []PeerConfig{{ID: "node2"}})
	// Mock peer to always reject votes so election fails
	mock := &mockTransport{
		sendVoteFunc: func(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
			return &pb.RequestVoteResponse{Term: req.Term, VoteGranted: false}, nil
		},
	}
	node.SetTransport(mock)

	node.Start()
	defer node.Stop()

	// Let the node attempt elections across multiple timeouts
	time.Sleep(250 * time.Millisecond)

	// Since peer rejected votes, node should have started multiple terms
	if node.Term() < 2 {
		t.Fatalf("expected term to increment to at least 2 after failed elections, got %d", node.Term())
	}
	if node.Role() != Candidate {
		t.Fatalf("expected node to remain Candidate, got %v", node.Role())
	}
}

