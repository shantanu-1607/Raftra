package chaos

import (
	"sync"
	"time"

	"github.com/shantanu-1607/raftra/internal/raft"
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
