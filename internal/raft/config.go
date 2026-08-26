package raft

import (
	"time"
)

// PeerConfig holds connection info for another node in the cluster
type PeerConfig struct {
	ID      string // e.g. "node2"
	Address string // e.g. "localhost:50052"

}

// Config defines the configuration settings for a single Raft node
type Config struct {
	NodeID             string        // Unique identifier for this node (e.g. "node1")
	Peers              []PeerConfig  // Addresses of all other nodes in the cluster
	ElectionTimeoutMin time.Duration // Minimum election timeout (e.g. 150ms)
	ElectionTimeoutMax time.Duration //Maximum election timeout (e.g. 300ms)
	HeartbeatInterval  time.Duration // Frequency of leader heartbeats (e.g. 50ms)
	DataDir            string        // Directory for disk persistence
}

// DefaultConfig returns a sane default configuration for testing and development
func DefaultConfig(nodeID string, peers []PeerConfig) Config {
	return Config{
		NodeID:             nodeID,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		DataDir:            "./data",
	}
}
