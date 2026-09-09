package raft

import (
	"errors"
	"fmt"
	"sync"

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
