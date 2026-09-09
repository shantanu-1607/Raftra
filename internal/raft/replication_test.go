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
