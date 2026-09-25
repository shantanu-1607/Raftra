package chaos

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/shantanu-1607/raftra/internal/raft"
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
