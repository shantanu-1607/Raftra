package raft

import (
	pb "github.com/shantanu-1607/raftra/proto"
)

// NodeRole represents the current role of the Raft node
type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

// String helper to print readable role names in logs
func (r NodeRole) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// PersistentState contains state that must be saved to stable storage before responding to RPCs
type PersistentState struct {
	CurrentTerm uint64         // Latest term server has seen
	VotedFor    string         // CandidateID that received vote in current term (empty string if none)
	Log         []*pb.LogEntry // Log entries (1-indexed, index 0 is a dummy sentinel entry)
}

// VolatileState contains state that is kept in memory on all servers
type VolatileState struct {
	CommitIndex uint64 // Index of highest log entry known to be committed
	LastApplied uint64 // Index of highest log entry applied to state machine
}

// LeaderState contains state maintained only by the leader (reinitialized after each election)
type LeaderState struct {
	NextIndex  map[string]uint64 // For each peer, index of the next log entry to send (initialized to leader last log index + 1)
	MatchIndex map[string]uint64 // For each peer, index of highest log entry known to be replicated on peer
}
