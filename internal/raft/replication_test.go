package raft

import (
	"sync"
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

