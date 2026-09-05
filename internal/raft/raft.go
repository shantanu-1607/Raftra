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
	leaderID   string       //id of the current known leader

	// State Machine, Persistence & Transport
	kvStore   *kvstore.KVStore
	storage   storage.StorageBackend
	transport Transport

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

// SetTransport attaches the outbound transport layer to the RaftNode
func (rn *RaftNode) SetTransport(transport Transport) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.transport = transport
}

// Start kicks off the Raft node background event loop
func (rn *RaftNode) Start() {
	rn.mu.Lock()
	rn.electionTimer = time.NewTimer(rn.randomizedElectionTimeout())
	rn.heartbeatTimer = time.NewTimer(rn.config.HeartbeatInterval)
	rn.mu.Unlock()
	go rn.run()
}

// Stop cleanly terminates the node event loop
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	select {
	case <-rn.stopCh:
		//already stopped
		return
	default:
		//normal path
		close(rn.stopCh)
	}

	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}

}

// run is the central event loop listening on timers and signals
func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.stopCh:
			rn.logger.Info("raft node event loop stopped")
			return
		case <-rn.electionTimer.C:
			rn.mu.Lock()
			role := rn.role
			rn.mu.Unlock()
			if role != Leader {
				rn.logger.Warn("election timeout reached, starting election")
				rn.startElection()
			} else {
				// Leaders do not hold elections, reset timer
				rn.resetElectionTimer()
			}
		case <-rn.heartbeatTimer.C:
			rn.mu.Lock()
			role := rn.role
			rn.mu.Unlock()
			if role == Leader {
				rn.sendHeartbeats()
			}
			rn.resetHeartbeatTimer()
		}
	}
}

// resetElectionTimer resets the election timer to a fresh randomized timeout
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		if !rn.electionTimer.Stop() {
			select {
			case <-rn.electionTimer.C:
			default:
			}
		}
		rn.electionTimer.Reset(rn.randomizedElectionTimeout())
	}
}

// resetHeartbeatTimer resets the heartbeat timer to the configured interval
func (rn *RaftNode) resetHeartbeatTimer() {
	if rn.heartbeatTimer != nil {
		if !rn.heartbeatTimer.Stop() {
			select {
			case <-rn.heartbeatTimer.C:
			default:
			}
		}
		rn.heartbeatTimer.Reset(rn.config.HeartbeatInterval)
	}
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
	if diff <= 0 {
		return rn.config.ElectionTimeoutMin
	}
	randomExtra := time.Duration(rand.Int63n(int64(diff)))
	return rn.config.ElectionTimeoutMin + randomExtra
}

// LeaderID returns the ID of the current known leader (thread-safe).

func (rn *RaftNode) LeaderID() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.role == Leader {
		return rn.config.NodeID
	}
	return rn.leaderID
}
