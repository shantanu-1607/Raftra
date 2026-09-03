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
	newIndex := lastIndex + 1
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

	// 2. Fast-path: In a single-node cluster, 1 node is already the majority!
	if len(rn.peers) == 0 {
		rn.volatile.CommitIndex = newIndex
		rn.applyCommittedEntriesLocked()
		return newIndex, nil
	}

	// 3. Immediately replicate the new entry to all followers
	rn.broadcastAppendEntriesLocked()

	return newIndex, nil

}

// broadcastAppendEntriesLocked replicates pending log entries to all peers.
