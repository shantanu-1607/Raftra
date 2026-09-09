package raft

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	pb "github.com/shantanu-1607/raftra/proto"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// Stage 1: In-Memory Network Router (Simulates wires, packets & cable pulls)
// ============================================================================

// clusterNetwork acts like a virtual Wi-Fi router connecting our in-memory nodes.
type clusterNetwork struct {
	mu           sync.RWMutex
	nodes        map[string]*RaftNode // nodeID -> *RaftNode
	disconnected map[string]bool      // nodeID -> true (simulates disconnected cable)
}

func newClusterNetwork() *clusterNetwork {
	return &clusterNetwork{
		nodes:        make(map[string]*RaftNode),
		disconnected: make(map[string]bool),
	}
}

// registerNode connects a node to the virtual network and attaches its transport.
func (cn *clusterNetwork) registerNode(node *RaftNode) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.nodes[node.config.NodeID] = node
	node.SetTransport(&networkTransport{
		nodeID:  node.config.NodeID,
		network: cn,
	})
}

// isolate simulates pulling the network plug on a node (drops all messages to/from it).
func (cn *clusterNetwork) isolate(nodeID string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.disconnected[nodeID] = true
}

// reconnect plugs the network cable back in.
func (cn *clusterNetwork) reconnect(nodeID string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	delete(cn.disconnected, nodeID)
}

// networkTransport implements the raft.Transport interface for a node.
type networkTransport struct {
	nodeID  string
	network *clusterNetwork
}

func (nt *networkTransport) SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	nt.network.mu.RLock()
	defer nt.network.mu.RUnlock()
	// If sender or receiver is unplugged, the network drops the packet!
	if nt.network.disconnected[nt.nodeID] || nt.network.disconnected[peerID] {
		return nil, errors.New("network unreachable: node is disconnected")
	}
	target, exists := nt.network.nodes[peerID]
	if !exists {
		return nil, fmt.Errorf("peer %s not found in network", peerID)
	}

	// proto.Clone copies the message so nodes don't share memory pointers (like real network bytes)
	clonedReq := proto.Clone(req).(*pb.RequestVoteRequest)
	resp := target.HandleRequestVote(clonedReq)
	return proto.Clone(resp).(*pb.RequestVoteResponse), nil
}

func (nt *networkTransport) SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	nt.network.mu.RLock()
	defer nt.network.mu.RUnlock()
	if nt.network.disconnected[nt.nodeID] || nt.network.disconnected[peerID] {
		return nil, errors.New("network unreachable: node is disconnected")
	}
	target, exists := nt.network.nodes[peerID]
	if !exists {
		return nil, fmt.Errorf("peer %s not found in network", peerID)
	}
	clonedReq := proto.Clone(req).(*pb.AppendEntriesRequest)
	resp := target.HandleAppendEntries(clonedReq)
	return proto.Clone(resp).(*pb.AppendEntriesResponse), nil
}

func (nt *networkTransport) Close() error {
	return nil
}

// ============================================================================
// Test Helpers: Quick ways to set up a 3-node cluster and commands
// ============================================================================

// createThreeNodeCluster wires up 3 nodes into our virtual network
func createThreeNodeCluster() (*clusterNetwork, *RaftNode, *RaftNode, *RaftNode) {
	net := newClusterNetwork()
	node1, _ := createTestNode("node1", []PeerConfig{{ID: "node2"}, {ID: "node3"}})
	node2, _ := createTestNode("node2", []PeerConfig{{ID: "node1"}, {ID: "node3"}})
	node3, _ := createTestNode("node3", []PeerConfig{{ID: "node1"}, {ID: "node2"}})
	net.registerNode(node1)
	net.registerNode(node2)
	net.registerNode(node3)
	return net, node1, node2, node3
}

// forceLeader promotes a node to Leader immediately without waiting for election timers
func forceLeader(node *RaftNode, term uint64) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.role = Candidate
	node.persistent.CurrentTerm = term
	node.becomeLeader()
}

// encodeSet serializes a SET command into bytes for ProposeCommand
func encodeSet(key, val string) []byte {
	cmd := kvstore.Command{Type: kvstore.CmdSet, Key: key, Value: val}
	b, _ := cmd.Encode()
	return b
}

// encodeDelete serializes a DELETE command into bytes for ProposeCommand
func encodeDelete(key string) []byte {
	cmd := kvstore.Command{Type: kvstore.CmdDelete, Key: key}
	b, _ := cmd.Encode()
	return b
}

// waitFor polls until cond returns true or timeout expires
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, desc)
}

// ============================================================================
// Stage 2: Replication & Consensus Tests
// ============================================================================

// 1. Single node cluster fast-path (1 node trivially forms a majority)
func TestSingleNodeProposal(t *testing.T) {
	node, _ := createTestNode("node1", nil)
	forceLeader(node, 1)

	idx, err := node.ProposeCommand(encodeSet("single", "value123"))
	if err != nil {
		t.Fatalf("expected proposal to succeed, got: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected log index 1, got %d", idx)
	}
	if node.CommitIndex() != 1 {
		t.Fatalf("expected commitIndex 1, got %d", node.CommitIndex())
	}
	if val, ok := node.Get("single"); !ok || val != "value123" {
		t.Fatalf("expected single=value123 in state machine, got ok=%v, val=%s", ok, val)
	}
}

// 2. Basic SET then GET returns correct value on Leader and replicated Followers
func TestBasicSetGet(t *testing.T) {
	_, n1, n2, n3 := createThreeNodeCluster()
	forceLeader(n1, 1)

	idx, err := n1.ProposeCommand(encodeSet("user", "shantanu"))
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected index 1, got %d", idx)
	}

	// Verify leader applied it to state machine
	if val, ok := n1.Get("user"); !ok || val != "shantanu" {
		t.Fatalf("leader state machine missing key 'user', got %s", val)
	}

	// Followers receive the committed entry via heartbeat
	n1.sendHeartbeats()

	waitFor(t, 200*time.Millisecond, func() bool {
		v2, ok2 := n2.Get("user")
		v3, ok3 := n3.Get("user")
		return ok2 && v2 == "shantanu" && ok3 && v3 == "shantanu"
	}, "followers to apply committed 'user' key")

	if n2.CommitIndex() != 1 || n3.CommitIndex() != 1 {
		t.Fatalf("expected follower commitIndex to be 1, got n2=%d, n3=%d", n2.CommitIndex(), n3.CommitIndex())
	}
}

// 3. All followers have identical log entries after multiple proposals
func TestReplicationToFollowers(t *testing.T) {
	_, n1, n2, n3 := createThreeNodeCluster()
	forceLeader(n1, 1)

	commands := []string{"alpha", "beta", "gamma"}
	for i, cmdVal := range commands {
		idx, err := n1.ProposeCommand(encodeSet(fmt.Sprintf("k%d", i+1), cmdVal))
		if err != nil {
			t.Fatalf("failed to propose %s: %v", cmdVal, err)
		}
		if idx != uint64(i+1) {
			t.Fatalf("expected index %d, got %d", i+1, idx)
		}
	}

	// Verify all 3 nodes have the same 3 log entries
	for i := uint64(1); i <= 3; i++ {
		e1, err1 := n1.storage.GetEntry(i)
		e2, err2 := n2.storage.GetEntry(i)
		e3, err3 := n3.storage.GetEntry(i)

		if err1 != nil || err2 != nil || err3 != nil {
			t.Fatalf("error retrieving entry %d from nodes: err1=%v, err2=%v, err3=%v", i, err1, err2, err3)
		}
		if e1.Term != 1 || e2.Term != 1 || e3.Term != 1 {
			t.Fatalf("entry %d terms do not match: e1=%d, e2=%d, e3=%d", i, e1.Term, e2.Term, e3.Term)
		}
		if string(e1.Command) != string(e2.Command) || string(e1.Command) != string(e3.Command) {
			t.Fatalf("entry %d commands do not match across cluster", i)
		}
	}
}

// 4. Entry is not committed until a majority (2/3) is reachable
func TestCommitRequiresMajority(t *testing.T) {
	net, n1, _, _ := createThreeNodeCluster()
	forceLeader(n1, 1)

	// Case A: 1 follower disconnected (2/3 reachable -> majority exists)
	net.isolate("node3")
	idx1, err := n1.ProposeCommand(encodeSet("quorum_key1", "val1"))
	if err != nil {
		t.Fatalf("expected proposal to succeed with 2/3 nodes online, got: %v", err)
	}
	if idx1 != 1 {
		t.Fatalf("expected index 1, got %d", idx1)
	}
	if n1.CommitIndex() != 1 {
		t.Fatalf("expected commitIndex 1, got %d", n1.CommitIndex())
	}

	// Case B: Both followers disconnected (1/3 reachable -> NO majority!)
	net.isolate("node2")

	doneCh := make(chan error, 1)
	go func() {
		_, err := n1.ProposeCommand(encodeSet("quorum_key2", "val2"))
		doneCh <- err
	}()

	// Proposal must BLOCK because quorum cannot be reached
	select {
	case err := <-doneCh:
		t.Fatalf("expected proposal to block without majority, but returned: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: still waiting for quorum!
	}

	if n1.CommitIndex() != 1 {
		t.Fatalf("expected commitIndex to stay at 1 without quorum, got %d", n1.CommitIndex())
	}

	// Reconnect node2 -> Quorum is restored!
	net.reconnect("node2")
	n1.mu.Lock()
	n1.broadcastAppendEntriesLocked()
	n1.mu.Unlock()

	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("expected proposal to complete after restoring quorum, got err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("proposal timed out after quorum was restored")
	}

	if n1.CommitIndex() != 2 {
		t.Fatalf("expected commitIndex to advance to 2, got %d", n1.CommitIndex())
	}
	if val, ok := n1.Get("quorum_key2"); !ok || val != "val2" {
		t.Fatalf("expected quorum_key2=val2 applied to state machine")
	}
}

// 5. A disconnected follower catches up with all missed entries upon reconnecting
func TestFollowerCatchUp(t *testing.T) {
	net, n1, _, n3 := createThreeNodeCluster()
	forceLeader(n1, 1)

	// Disconnect node3
	net.isolate("node3")

	// Propose 2 commands while node3 is offline
	_, err := n1.ProposeCommand(encodeSet("catch_up_1", "v1"))
	if err != nil {
		t.Fatalf("propose 1 failed: %v", err)
	}
	_, err = n1.ProposeCommand(encodeSet("catch_up_2", "v2"))
	if err != nil {
		t.Fatalf("propose 2 failed: %v", err)
	}

	// Node3 missed these entries
	lastIdx, _ := n3.storage.LastIndex()
	if lastIdx != 0 {
		t.Fatalf("expected node3 lastIndex 0 while isolated, got %d", lastIdx)
	}

	// Reconnect node3
	net.reconnect("node3")

	// Propose a 3rd command -> Leader broadcasts all missing entries to node3!
	idx3, err := n1.ProposeCommand(encodeSet("catch_up_3", "v3"))
	if err != nil {
		t.Fatalf("propose 3 failed: %v", err)
	}
	if idx3 != 3 {
		t.Fatalf("expected index 3, got %d", idx3)
	}

	// Send heartbeat to propagate commitIndex to node3
	n1.sendHeartbeats()

	// Wait for node3 to catch up and apply entries to its state machine
	waitFor(t, 300*time.Millisecond, func() bool {
		v1, ok1 := n3.Get("catch_up_1")
		v2, ok2 := n3.Get("catch_up_2")
		v3, ok3 := n3.Get("catch_up_3")
		return ok1 && v1 == "v1" && ok2 && v2 == "v2" && ok3 && v3 == "v3"
	}, "node3 to catch up all 3 entries")

	if n3.CommitIndex() != 3 {
		t.Fatalf("expected node3 commitIndex 3, got %d", n3.CommitIndex())
	}
}

// 6. Conflicting uncommitted entries on a follower are truncated and overwritten (§5.3)
func TestLogConflictResolution(t *testing.T) {
	_, n1, n2, n3 := createThreeNodeCluster()

	// Manually populate node3 with conflicting entries from an uncommitted Term 1
	// Node3 has:
	//   Index 1: Term 1 (matching)
	//   Index 2: Term 1 (conflicts with Leader's Term 2!)
	//   Index 3: Term 1 (conflicts with Leader's Term 2!)
	cmdOld := encodeSet("k", "old_uncommitted")
	_ = n3.storage.AppendEntries([]*pb.LogEntry{
		{Index: 1, Term: 1, Command: encodeSet("k1", "base")},
		{Index: 2, Term: 1, Command: cmdOld},
		{Index: 3, Term: 1, Command: cmdOld},
	})
	n3.persistent.CurrentTerm = 1

	// Leader n1 and follower n2 have:
	//   Index 1: Term 1
	//   Index 2: Term 2
	_ = n1.storage.AppendEntries([]*pb.LogEntry{
		{Index: 1, Term: 1, Command: encodeSet("k1", "base")},
		{Index: 2, Term: 2, Command: encodeSet("k2", "new_leader_entry")},
	})
	_ = n2.storage.AppendEntries([]*pb.LogEntry{
		{Index: 1, Term: 1, Command: encodeSet("k1", "base")},
		{Index: 2, Term: 2, Command: encodeSet("k2", "new_leader_entry")},
	})

	// Promote n1 to Leader in Term 2
	forceLeader(n1, 2)

	// Propose a new entry at Index 3 in Term 2
	idx, err := n1.ProposeCommand(encodeSet("k3", "term2_entry"))
	if err != nil {
		t.Fatalf("proposal failed: %v", err)
	}
	if idx != 3 {
		t.Fatalf("expected index 3, got %d", idx)
	}

	// Propagate commit to followers
	n1.sendHeartbeats()

	// Node3 must have truncated its conflicting entries at index 2 & 3 and accepted Leader's log!
	waitFor(t, 300*time.Millisecond, func() bool {
		e2, err2 := n3.storage.GetEntry(2)
		e3, err3 := n3.storage.GetEntry(3)
		if err2 != nil || err3 != nil || e2 == nil || e3 == nil {
			return false
		}
		// Terms must now be 2, NOT the old Term 1!
		return e2.Term == 2 && e3.Term == 2 && n3.CommitIndex() == 3
	}, "node3 to truncate conflicts and match leader log")

	// Verify node3 state machine has the new values, not the stale uncommitted values
	val2, ok2 := n3.Get("k2")
	if !ok2 || val2 != "new_leader_entry" {
		t.Fatalf("expected k2=new_leader_entry on node3, got %s", val2)
	}
	val3, ok3 := n3.Get("k3")
	if !ok3 || val3 != "term2_entry" {
		t.Fatalf("expected k3=term2_entry on node3, got %s", val3)
	}
}

// 7. Safety Invariant (§5.4.2): Leader cannot commit entries from prior terms by counting replicas alone
func TestLeaderOnlyCommitsCurrentTerm(t *testing.T) {
	_, n1, n2, _ := createThreeNodeCluster()

	// Put an entry from prior Term 1 into n1 and n2
	entryTerm1 := &pb.LogEntry{
		Index:   1,
		Term:    1, // Prior term!
		Command: encodeSet("prior_key", "prior_val"),
	}
	_ = n1.storage.AppendEntries([]*pb.LogEntry{entryTerm1})
	_ = n2.storage.AppendEntries([]*pb.LogEntry{entryTerm1})

	// Promote n1 to Leader in Term 2
	forceLeader(n1, 2)

	// Simulate that node2 acknowledges entry 1
	n1.mu.Lock()
	n1.leader.MatchIndex["node2"] = 1
	// Explicitly evaluate commit index check
	n1.checkAndUpdateCommitIndexLocked()
	commitIdxAfterCheck := n1.volatile.CommitIndex
	n1.mu.Unlock()

	// Invariant: CommitIndex must NOT advance to 1, because entry.Term (1) != CurrentTerm (2)!
	if commitIdxAfterCheck != 0 {
		t.Fatalf("violation of §5.4.2: leader committed an entry from prior term directly! commitIndex=%d", commitIdxAfterCheck)
	}
	if _, ok := n1.Get("prior_key"); ok {
		t.Fatalf("entry from prior term should not be applied yet")
	}

	// Now propose an entry in the CURRENT term (Term 2)
	idx2, err := n1.ProposeCommand(encodeSet("current_key", "current_val"))
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	if idx2 != 2 {
		t.Fatalf("expected index 2, got %d", idx2)
	}

	// Committing entry 2 in current term indirectly commits entry 1!
	if n1.CommitIndex() != 2 {
		t.Fatalf("expected commitIndex to advance to 2, got %d", n1.CommitIndex())
	}
	if val, ok := n1.Get("prior_key"); !ok || val != "prior_val" {
		t.Fatalf("expected prior_key to be committed indirectly, got %s", val)
	}
	if val, ok := n1.Get("current_key"); !ok || val != "current_val" {
		t.Fatalf("expected current_key to be committed, got %s", val)
	}
}

// 8. Follower rejects write proposals and reports current LeaderID
func TestNonLeaderRejectsWrites(t *testing.T) {
	_, n1, n2, _ := createThreeNodeCluster()
	forceLeader(n1, 1)

	// Heartbeat informs n2 of leader ID
	n1.sendHeartbeats()
	time.Sleep(50 * time.Millisecond)

	// Attempt write on Follower n2
	idx, err := n2.ProposeCommand(encodeSet("any", "thing"))
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got: %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected index 0 on rejected write, got %d", idx)
	}

	// Follower must know who the current leader is
	if n2.LeaderID() != "node1" {
		t.Fatalf("expected LeaderID 'node1', got '%s'", n2.LeaderID())
	}
}

// 9. DELETE removes key from state machine across the entire cluster
func TestDeleteOperation(t *testing.T) {
	_, n1, n2, n3 := createThreeNodeCluster()
	forceLeader(n1, 1)

	// 1. SET
	idx1, err := n1.ProposeCommand(encodeSet("temp_key", "temp_value"))
	if err != nil || idx1 != 1 {
		t.Fatalf("SET failed: %v", err)
	}
	if v, ok := n1.Get("temp_key"); !ok || v != "temp_value" {
		t.Fatalf("expected temp_key=temp_value on leader")
	}

	// 2. DELETE
	idx2, err := n1.ProposeCommand(encodeDelete("temp_key"))
	if err != nil || idx2 != 2 {
		t.Fatalf("DELETE failed: %v", err)
	}
	if _, ok := n1.Get("temp_key"); ok {
		t.Fatalf("expected temp_key to be deleted from leader state machine")
	}

	// Propagate commit to followers
	n1.sendHeartbeats()

	waitFor(t, 200*time.Millisecond, func() bool {
		_, ok2 := n2.Get("temp_key")
		_, ok3 := n3.Get("temp_key")
		return !ok2 && !ok3 && n2.CommitIndex() == 2 && n3.CommitIndex() == 2
	}, "followers to apply DELETE command")
}

// 10. Sequence of operations applied in strict order across all nodes
func TestMultipleOperations(t *testing.T) {
	_, n1, n2, n3 := createThreeNodeCluster()
	forceLeader(n1, 1)

	commands := [][]byte{
		encodeSet("balance", "100"),
		encodeSet("balance", "200"),
		encodeDelete("balance"),
		encodeSet("balance", "500"),
		encodeSet("currency", "USD"),
	}

	for i, cmd := range commands {
		expectedIdx := uint64(i + 1)
		idx, err := n1.ProposeCommand(cmd)
		if err != nil {
			t.Fatalf("command %d failed: %v", i+1, err)
		}
		if idx != expectedIdx {
			t.Fatalf("expected index %d, got %d", expectedIdx, idx)
		}
	}

	if v, ok := n1.Get("balance"); !ok || v != "500" {
		t.Fatalf("expected balance=500 on leader, got %s", v)
	}
	if v, ok := n1.Get("currency"); !ok || v != "USD" {
		t.Fatalf("expected currency=USD on leader, got %s", v)
	}

	n1.sendHeartbeats()

	waitFor(t, 200*time.Millisecond, func() bool {
		v2, ok2 := n2.Get("balance")
		v3, ok3 := n3.Get("balance")
		c2, okC2 := n2.Get("currency")
		c3, okC3 := n3.Get("currency")
		return ok2 && v2 == "500" && ok3 && v3 == "500" && okC2 && c2 == "USD" && okC3 && c3 == "USD"
	}, "followers to match multi-operation state machine")
}

// 11. Multiple concurrent writes from clients are serialized and committed safely
func TestConcurrentWrites(t *testing.T) {
	_, n1, n2, n3 := createThreeNodeCluster()
	forceLeader(n1, 1)

	numWrites := 10
	var wg sync.WaitGroup
	errCh := make(chan error, numWrites)

	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			k := fmt.Sprintf("conc_key_%d", val)
			v := fmt.Sprintf("val_%d", val)
			_, err := n1.ProposeCommand(encodeSet(k, v))
			if err != nil {
				errCh <- fmt.Errorf("write %d failed: %w", val, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent write error: %v", err)
	}

	if n1.CommitIndex() != uint64(numWrites) {
		t.Fatalf("expected commitIndex %d, got %d", numWrites, n1.CommitIndex())
	}

	// Verify all writes exist on leader
	for i := 0; i < numWrites; i++ {
		k := fmt.Sprintf("conc_key_%d", i)
		expected := fmt.Sprintf("val_%d", i)
		if v, ok := n1.Get(k); !ok || v != expected {
			t.Fatalf("expected leader to have %s=%s, got %s", k, expected, v)
		}
	}

	// Propagate to followers
	n1.sendHeartbeats()

	waitFor(t, 300*time.Millisecond, func() bool {
		return n2.CommitIndex() == uint64(numWrites) && n3.CommitIndex() == uint64(numWrites)
	}, "followers to replicate all concurrent writes")

	for i := 0; i < numWrites; i++ {
		k := fmt.Sprintf("conc_key_%d", i)
		expected := fmt.Sprintf("val_%d", i)
		if v, ok := n2.Get(k); !ok || v != expected {
			t.Fatalf("follower 2 missing %s=%s, got %s", k, expected, v)
		}
		if v, ok := n3.Get(k); !ok || v != expected {
			t.Fatalf("follower 3 missing %s=%s, got %s", k, expected, v)
		}
	}
}
