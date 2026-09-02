package raft

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/storage"
	pb "github.com/shantanu-1607/raftra/proto"
)

// Transport defines the outbound RPC interface required by RaftNode
type Transport interface {
	SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)

	Close() error
}

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

// NewRaftNode creates and initializes a RaftNode instance
func NewRaftNode(config Config, storage storage.StorageBackend, kv *kvstore.KVStore, logger *slog.Logger) (*RaftNode, error) {
	// 1. Recover persistent state from storage if it exists
	term, err := storage.LoadTerm()
	if err != nil {
		return nil, err
	}
	votedFor, err := storage.LoadVotedFor()
	if err != nil {
		return nil, err
	}
	entries, err := storage.LoadAllEntries()
	if err != nil {
		return nil, err
	}
	// 2. Build a fast lookup map of cluster peers
	peerMap := make(map[string]PeerConfig)
	for _, p := range config.Peers {
		peerMap[p.ID] = p
	}
	// 3. Assemble the RaftNode
	rn := &RaftNode{
		config: config,
		role:   Follower, // All nodes always start as Followers
		peers:  peerMap,
		persistent: PersistentState{
			CurrentTerm: term,
			VotedFor:    votedFor,
			Log:         entries,
		},
		volatile: VolatileState{
			CommitIndex: 0,
			LastApplied: 0,
		},
		leader:  nil,
		kvStore: kv,
		storage: storage,
		stopCh:  make(chan struct{}),
		logger:  logger.With("node_id", config.NodeID),
	}
	return rn, nil
}

// Role returns the current role of the node (thread-safe)
func (rn *RaftNode) Role() NodeRole {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.role
}

// Term returns the current term of the node (thread-safe)
func (rn *RaftNode) Term() uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.persistent.CurrentTerm
}

// randomizedElectionTimeout returns a random duration between Min and Max timeout
func (rn *RaftNode) randomizedElectionTimeout() time.Duration {
	diff := rn.config.ElectionTimeoutMax - rn.config.ElectionTimeoutMin
	randomExtra := time.Duration(rand.Int63n(int64(diff)))
	return rn.config.ElectionTimeoutMin + randomExtra
}
