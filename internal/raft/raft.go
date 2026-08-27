package raft

import (
	"log/slog"
	"sync"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/storage"
)

// RaftNode represents a single consensus node in the cluster
type RaftNode struct {
	mu sync.Mutex

	//identity and config
	config Config
	role   NodeRole
	peers  map[string]PeerConfig // peerID -> PeerConfig

	// Raft State
	persistent PersistentState
	volatile   VolatileState
	leader     *LeaderState // nil when not leader

	// State Machine & Persistence Backend
	kvStore *kvstore.KVStore
	storage storage.StorageBackend

	// Coordination Channels & Timers
	stopCh         chan struct{}
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Structured Logger
	logger *slog.Logger
}
