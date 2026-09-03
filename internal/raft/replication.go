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

func (rn *RaftNode) ProposeCommand(cmd []byte) (uint64, error) {
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
func (rn *RaftNode) broadcastAppendEntriesLocked() {
	if rn.role != Leader {
		return
	}

	for peerID := range rn.peers {
		rn.sendAppendEntriesToPeerLocked(peerID)
	}
}

// sendAppendEntriesToPeerLocked sends an AppendEntries RPC with pending entries to a single follower.
func (rn *RaftNode) sendAppendEntriesToPeerLocked(peerID string) {

	if rn.role != Leader || rn.leader == nil {
		return
	}

	currentTerm := rn.persistent.CurrentTerm
	leaderID := rn.config.NodeID
	commitIndex := rn.volatile.CommitIndex

	nextIdx := rn.leader.NextIndex[peerID]
	if nextIdx == 0 {
		nextIdx = 1
		rn.leader.NextIndex[peerID] = 1
	}
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevEntry, err := rn.storage.GetEntry(prevLogIndex); err == nil && prevEntry != nil {
		prevLogTerm = prevEntry.Term
	}
	// Fetch all log entries from nextIndex to the end of the log
	entries, _ := rn.storage.GetEntriesFrom(nextIdx)
	req := &pb.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	go func(peer string, r *pb.AppendEntriesRequest, numEntries int) {
		resp, err := rn.transport.SendAppendEntries(peer, r)
		if err != nil {
			rn.logger.Debug("failed to send AppendEntries to peer", "peer", peer, "err", err)
			return
		}
		rn.mu.Lock()
		defer rn.mu.Unlock()
		// 1. If follower has a higher term, step down immediately (§5.1)
		if rn.checkTerm(resp.Term) {
			rn.resetElectionTimer()
			return
		}
		// 2. Ignore stale responses if our role or term changed while waiting
		if rn.role != Leader || rn.persistent.CurrentTerm != currentTerm {
			return
		}
		// 3. Handle response (§5.3)
		if resp.Success {
			// Success: advance nextIndex and matchIndex for this peer
			newNext := r.PrevLogIndex + uint64(numEntries) + 1
			newMatch := newNext - 1
			if newNext > rn.leader.NextIndex[peer] {
				rn.leader.NextIndex[peer] = newNext
			}
			if newMatch > rn.leader.MatchIndex[peer] {
				rn.leader.MatchIndex[peer] = newMatch
			}
			// Check if any entries are now confirmed by a majority (§5.3 & §5.4.2)
			rn.checkAndUpdateCommitIndexLocked()
		} else {
			// Log inconsistency: decrement nextIndex and retry (§5.3)
			if rn.leader.NextIndex[peer] > 1 {
				rn.leader.NextIndex[peer]--
				rn.logger.Debug("log inconsistency detected, decrementing nextIndex",
					"peer", peer,
					"newNextIndex", rn.leader.NextIndex[peer],
				)
				// Retry immediately with the lower nextIndex so follower catches up faster
				rn.sendAppendEntriesToPeerLocked(peer)
			}
		}

	}(peerID, req, len(entries))

}
