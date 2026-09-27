package chaos

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	t       *testing.T
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
func NewTestCluster(t *testing.T, size int) *TestCluster {
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 3. Fast timeouts for speedy tests!
	cfg := raft.DefaultConfig(id, peers)
	cfg.ElectionTimeoutMin = 60 * time.Millisecond
	cfg.ElectionTimeoutMax = 120 * time.Millisecond
	cfg.HeartbeatInterval = 20 * time.Millisecond

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
