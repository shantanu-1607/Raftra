package chaos

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/raft"
	"github.com/shantanu-1607/raftra/internal/storage"
	pb "github.com/shantanu-1607/raftra/proto"
	"google.golang.org/protobuf/proto"
)

// ChaosNetwork is an in-memory virtual network router.
// It sits between all Raft nodes, allowing our tests to simulate cable cuts,
// network partitions, and packet delays programmatically.
type ChaosNetwork struct {
	mu           sync.RWMutex
	nodes        map[string]*raft.RaftNode
	blockedPairs map[string]map[string]bool // [sender][receiver] -> true (traffic blocked)
	isolated     map[string]bool            // nodes completely cut off from everyone
	delays       map[string]time.Duration   // nodeID -> artificial packet delay
}

// NewChaosNetwork initializes a fresh virtual network switch.
func NewChaosNetwork() *ChaosNetwork {
	return &ChaosNetwork{
		nodes:        make(map[string]*raft.RaftNode),
		blockedPairs: make(map[string]map[string]bool),
		isolated:     make(map[string]bool),
		delays:       make(map[string]time.Duration),
	}
}

// RegisterNode connects a Raft node to the virtual switch.
func (cn *ChaosNetwork) RegisterNode(node *raft.RaftNode) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.nodes[node.ID()] = node
}

// IsBlocked checks if traffic from sender to receiver is currently prohibited.
func (cn *ChaosNetwork) IsBlocked(sender, receiver string) bool {
	cn.mu.RLock()
	defer cn.mu.RUnlock()

	// If either sender or receiver is completely isolated, drop packet
	if cn.isolated[sender] || cn.isolated[receiver] {
		return true
	}

	// Check if this specific pair is blocked
	if blockedRecvs, exists := cn.blockedPairs[sender]; exists && blockedRecvs[receiver] {
		return true
	}
	return false

}

// BlockLink cuts the network cable bidirectionally between nodeA and nodeB.
func (cn *ChaosNetwork) BlockLink(nodeA, nodeB string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	if _, ok := cn.blockedPairs[nodeA]; !ok {
		cn.blockedPairs[nodeA] = make(map[string]bool)
	}
	if _, ok := cn.blockedPairs[nodeB]; !ok {
		cn.blockedPairs[nodeB] = make(map[string]bool)
	}
	cn.blockedPairs[nodeA][nodeB] = true
	cn.blockedPairs[nodeB][nodeA] = true
}

// BlockOneWay drops traffic from sender to receiver only; receiver can still reach sender.
func (cn *ChaosNetwork) BlockOneWay(sender, receiver string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	if _, ok := cn.blockedPairs[sender]; !ok {
		cn.blockedPairs[sender] = make(map[string]bool)
	}
	cn.blockedPairs[sender][receiver] = true
}

// Isolate completely disconnects a node from every other node in the cluster.
func (cn *ChaosNetwork) Isolate(nodeID string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.isolated[nodeID] = true
}

// Reconnect restores a node's network connection.
func (cn *ChaosNetwork) Reconnect(nodeID string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	delete(cn.isolated, nodeID)
}

// Partition splits the cluster into two isolated islands (groupA and groupB).
// Nodes within groupA can talk to each other.
// Nodes within groupB can talk to each other.
// NO traffic can cross between groupA and groupB!
func (cn *ChaosNetwork) Partition(groupA, groupB []string) {
	for _, a := range groupA {
		for _, b := range groupB {
			cn.BlockLink(a, b)
		}
	}
}

// HealAll clears all network blocks, isolations, and delays.
func (cn *ChaosNetwork) HealAll() {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.blockedPairs = make(map[string]map[string]bool)
	cn.isolated = make(map[string]bool)
	cn.delays = make(map[string]time.Duration)
}

// TestTransport connects an individual RaftNode to the ChaosNetwork.
type TestTransport struct {
	nodeID  string
	network *ChaosNetwork
}

// NewTestTransport creates a network adapter for a node.
func NewTestTransport(nodeID string, network *ChaosNetwork) *TestTransport {
	return &TestTransport{
		nodeID:  nodeID,
		network: network,
	}
}

// SendRequestVote delivers a vote request through the virtual network.
func (tt *TestTransport) SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// 1. Check if the link is cut
	if tt.network.IsBlocked(tt.nodeID, peerID) {
		return nil, errors.New("network partition: packet dropped")
	}
	tt.network.mu.RLock()
	target, exists := tt.network.nodes[peerID]
	tt.network.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("peer %s not found on virtual network", peerID)
	}
	// 2. Clone the protobuf message to simulate physical network serialization!
	// (Prevents nodes from sharing pointers in memory)
	clonedReq := proto.Clone(req).(*pb.RequestVoteRequest)
	resp := target.HandleRequestVote(clonedReq)
	return proto.Clone(resp).(*pb.RequestVoteResponse), nil
}

// SendAppendEntries delivers replicated entries/heartbeats through the virtual network.
func (tt *TestTransport) SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	// 1. Check if the link is cut
	if tt.network.IsBlocked(tt.nodeID, peerID) {
		return nil, errors.New("network partition: packet dropped")
	}
	tt.network.mu.RLock()
	target, exists := tt.network.nodes[peerID]
	tt.network.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("peer %s not found on virtual network", peerID)
	}
	// 2. Clone the protobuf message
	clonedReq := proto.Clone(req).(*pb.AppendEntriesRequest)
	resp := target.HandleAppendEntries(clonedReq)
	return proto.Clone(resp).(*pb.AppendEntriesResponse), nil
}

// Close satisfies the raft.Transport interface.
func (tt *TestTransport) Close() error {
	return nil
}

// TestCluster manages a group of Raft nodes connected via a ChaosNetwork.
// It provides programmatic cluster controls, fault injection, and state inspection.
type TestCluster struct {
	t       testing.TB
	mu      sync.Mutex
	network *ChaosNetwork
	nodes   map[string]*raft.RaftNode
	stores  map[string]*storage.BboltStore
	kvs     map[string]*kvstore.KVStore
	dbPaths map[string]string
	peers   []string
	baseDir string
}

// NewTestCluster creates a multi-node cluster (typically 3 or 5 nodes)
// with real bbolt persistence and an in-memory chaos router.
func NewTestCluster(t testing.TB, size int) *TestCluster {
	t.Helper()

	baseDir := t.TempDir()
	network := NewChaosNetwork()

	peerIDs := make([]string, size)
	for i := 0; i < size; i++ {
		peerIDs[i] = fmt.Sprintf("node%d", i+1)
	}

	tc := &TestCluster{
		t:       t,
		network: network,
		nodes:   make(map[string]*raft.RaftNode),
		stores:  make(map[string]*storage.BboltStore),
		kvs:     make(map[string]*kvstore.KVStore),
		dbPaths: make(map[string]string),
		peers:   peerIDs,
		baseDir: baseDir,
	}

	for _, id := range peerIDs {
		tc.createNode(id)
	}
	return tc
}

// createNode initializes a single node with its own bbolt DB and registers it on the network.
func (tc *TestCluster) createNode(id string) {
	tc.t.Helper()

	// 1. Prepare peer list excluding self
	var peers []raft.PeerConfig
	for _, p := range tc.peers {
		if p != id {
			peers = append(peers, raft.PeerConfig{ID: p})

		}
	}

	// 2. Open node-specific bbolt database in temp dir
	dbPath := filepath.Join(tc.baseDir, fmt.Sprintf("raft-%s.db", id))
	store, err := storage.NewBboltStore(dbPath)
	if err != nil {
		tc.t.Fatalf("failed to create bbolt store for %s: %v", id, err)

	}
	kv := kvstore.NewKVStore()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// 3. Fast timeouts for speedy tests!
	cfg := raft.DefaultConfig(id, peers)
	cfg.ElectionTimeoutMin = 150 * time.Millisecond
	cfg.ElectionTimeoutMax = 300 * time.Millisecond
	cfg.HeartbeatInterval = 30 * time.Millisecond

	node, err := raft.NewRaftNode(cfg, store, kv, logger)
	if err != nil {
		tc.t.Fatalf("failed to create raft node %s: %v", id, err)
	}

	// 4. Attach chaos transport and register on network
	trans := NewTestTransport(id, tc.network)
	node.SetTransport(trans)
	tc.network.RegisterNode(node)

	tc.nodes[id] = node
	tc.stores[id] = store
	tc.kvs[id] = kv
	tc.dbPaths[id] = dbPath

}

// Start launches all nodes in the cluster.
func (tc *TestCluster) Start() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, node := range tc.nodes {
		node.Start()
	}
}

// Stop gracefully shuts down all nodes and closes their bbolt databases.
func (tc *TestCluster) Stop() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for id, node := range tc.nodes {
		node.Stop()
		if store, ok := tc.stores[id]; ok {
			_ = store.Close()
		}
	}
}

// WaitForLeader polls the cluster until exactly one node becomes Leader.
func (tc *TestCluster) WaitForLeader(timeout time.Duration) *raft.RaftNode {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		leaders := tc.GetLeaders()
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}

	tc.t.Fatalf("timed out after %v waiting for a unique leader", timeout)
	return nil
}

// WaitForNewLeader polls the cluster until exactly one node becomes Leader, and its ID is not oldLeaderID.
func (tc *TestCluster) WaitForNewLeader(oldLeaderID string, timeout time.Duration) *raft.RaftNode {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		leaders := tc.GetLeaders()
		for _, l := range leaders {
			if l.ID() != oldLeaderID {
				return l
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	tc.t.Fatalf("timed out after %v waiting for a new leader different from %s", timeout, oldLeaderID)
	return nil
}

// GetLeaders returns all nodes that currently consider themselves Leader.
func (tc *TestCluster) GetLeaders() []*raft.RaftNode {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	var leaders []*raft.RaftNode
	for _, node := range tc.nodes {
		if node.Role() == raft.Leader {
			leaders = append(leaders, node)
		}
	}
	return leaders
}

// Propose submits a key-value write to the current cluster leader.
// It retries briefly if the cluster is in the middle of a leader election.
func (tc *TestCluster) Propose(key, val string) (uint64, error) {
	tc.t.Helper()
	deadline := time.Now().Add(3 * time.Second)

	cmd := kvstore.Command{Type: kvstore.CmdSet, Key: key, Value: val}
	b, err := cmd.Encode()
	if err != nil {
		return 0, err
	}

	var lastErr error
	for time.Now().Before(deadline) {
		leaders := tc.GetLeaders()
		if len(leaders) > 0 {
			type result struct {
				idx uint64
				err error
			}
			resCh := make(chan result, len(leaders))
			for _, l := range leaders {
				go func(leader *raft.RaftNode) {
					idx, err := leader.ProposeCommand(b)
					resCh <- result{idx, err}
				}(l)
			}

			// Wait for success, but timeout quickly to loop and find new leaders if stuck on a zombie
			select {
			case res := <-resCh:
				if res.err == nil {
					return res.idx, nil
				}
				lastErr = res.err
			case <-time.After(100 * time.Millisecond):
				lastErr = errors.New("timeout waiting for leader to commit")
			}
		} else {
			lastErr = errors.New("no leader found in the cluster")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, lastErr
}

// ProposeOnNode submits a write directly to a specific node.
// Useful to prove that an isolated minority node rejects or fails to commit writes!
func (tc *TestCluster) ProposeOnNode(nodeID, key, val string) (uint64, error) {
	tc.mu.Lock()
	node, ok := tc.nodes[nodeID]
	tc.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("node %s not found", nodeID)
	}

	cmd := kvstore.Command{Type: kvstore.CmdSet, Key: key, Value: val}

	b, err := cmd.Encode()
	if err != nil {
		return 0, err
	}

	return node.ProposeCommand(b)
}

// Crash simulates sudden power loss on a node:
// stops its event loop, isolates it from the network, and closes its database file.
func (tc *TestCluster) Crash(nodeID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if node, ok := tc.nodes[nodeID]; ok {
		node.Stop()
		delete(tc.nodes, nodeID)
	}
	if store, ok := tc.stores[nodeID]; ok {
		_ = store.Close()
		delete(tc.stores, nodeID)
	}
	tc.network.Isolate(nodeID)

}

// Restart simulates a crashed node booting back up.
// It opens the existing bbolt DB file from disk, starts a new RaftNode instance,
// reconnects its network adapter, and launches its event loop.
func (tc *TestCluster) Restart(nodeID string) *raft.RaftNode {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// 1. Re-open existing bbolt database on disk
	dbPath := tc.dbPaths[nodeID]
	store, err := storage.NewBboltStore(dbPath)

	if err != nil {
		tc.t.Fatalf("failed to reopen bbolt store for %s: %v", nodeID, err)
	}

	// 2. Prepare peer list excluding self
	var peers []raft.PeerConfig
	for _, p := range tc.peers {
		if p != nodeID {
			peers = append(peers, raft.PeerConfig{ID: p})
		}
	}

	// 3. New state machine and silent logger
	kv := kvstore.NewKVStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := raft.DefaultConfig(nodeID, peers)
	cfg.ElectionTimeoutMin = 150 * time.Millisecond
	cfg.ElectionTimeoutMax = 300 * time.Millisecond
	cfg.HeartbeatInterval = 30 * time.Millisecond

	// 4. New RaftNode instance (recovers term, vote, and log from bbolt)
	node, err := raft.NewRaftNode(cfg, store, kv, logger)
	if err != nil {
		tc.t.Fatalf("failed to create raft node %s: %v", nodeID, err)
	}

	// 5. Reconnect to virtual network
	trans := NewTestTransport(nodeID, tc.network)
	node.SetTransport(trans)
	tc.network.RegisterNode(node)
	tc.network.Reconnect(nodeID)
	tc.nodes[nodeID] = node
	tc.stores[nodeID] = store
	tc.kvs[nodeID] = kv
	node.Start()
	return node
}

// Partition isolates a single node from all peers.
func (tc *TestCluster) Partition(nodeID string) {
	tc.network.Isolate(nodeID)
}

// PartitionGroup divides the cluster into two disconnected groups.
// Nodes in groupA can only talk to groupA.
// Nodes in groupB can only talk to groupB.
func (tc *TestCluster) PartitionGroup(groupA, groupB []string) {
	tc.network.Partition(groupA, groupB)
}

// BlockOneWay drops messages from sender to receiver, while receiver -> sender still works.
func (tc *TestCluster) BlockOneWay(sender, receiver string) {
	tc.network.BlockOneWay(sender, receiver)
}

// Heal restores all network connections across the entire cluster.
func (tc *TestCluster) Heal() {
	tc.network.HealAll()
}

// GetKV retrieves a key from a specific node's state machine.
func (tc *TestCluster) GetKV(nodeID, key string) (string, bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if kv, ok := tc.kvs[nodeID]; ok {
		return kv.Get(key)
	}
	return "", false
}

// AssertAllKVConsistent polls until every active node has applied and agrees on the key-value pair.
func (tc *TestCluster) AssertAllKVConsistent(key, expectedVal string, timeout time.Duration) {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		allMatch := true
		tc.mu.Lock()
		for id := range tc.nodes {
			val, ok := tc.kvs[id].Get(key)
			if !ok || val != expectedVal {
				allMatch = false
				break
			}
		}
		tc.mu.Unlock()

		if allMatch {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatalf("timed out after %v waiting for all nodes to agree on key %q = %q", timeout, key, expectedVal)
}
