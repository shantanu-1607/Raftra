package raft

import (
	"errors"

	pb "github.com/shantanu-1607/raftra/proto"
)

// ErrNotLeader is returned when a write command is proposed to a non-leader node.

var ErrNotLeader = errors.New("node is not a leader")

// ProposeCommand proposes a client command to the Raft cluster.
// If this node is the leader, it appends the command to its local log and triggers replication.
// Returns the allocated log index, or ErrNotLeader if this node is not the leader.

func (r *RaftNode) ProposeCommand(cmd []byte) (uint64, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Only the Leader can accept write proposals from clients
	if rn.role != Leader {
		return 0, ErrNotLeader
	}

	lastIndex, _ := rn.storage.LastIndex()
	newIndex := lasrIndex + 1
	currentTerm := rn.persistent.CurrentTerm

	entry := &pb.LogEntry{
		Index:   newIndex,
		Term:    currentTerm,
		Command: cmd,
	}

	if err := rn.storage.AppendEntries([]*pb.LogEntry{entry}); err != nil {
		return 0, err
	}

	rn.persistent.Log = append(rn.persistent.Log, entry)

	rn.logger.Info("proposing new command", "index", newIndex, "term", currentTerm)

}
