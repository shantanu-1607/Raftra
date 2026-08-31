package raft

import (
	"sync"

	pb "github.com/shantanu-1607/raftra/proto"
)

// checkTerm updates the node's term and reverts it to a Follower if an incoming term is higher.
// NOTE: Caller MUST hold rn.mu before calling checkTerm.
func (rn *RaftNode) checkTerm(incomingTerm uint64) bool {
	if incomingTerm > rn.persistent.CurrentTerm {
		rn.logger.Info("discovered higher term, stepping down to follower",
			"old_term", rn.persistent.CurrentTerm,
			"new_term", incomingTerm,
			"old_role", rn.role.String(),
		)
		rn.persistent.CurrentTerm = incomingTerm
		rn.persistent.VotedFor = ""
		rn.role = Follower
		rn.leader = nil

		// Persist the updated term and cleared vote
		_ = rn.storage.SaveTerm(incomingTerm)
		_ = rn.storage.SaveVotedFor("")
		return true
	}
	return false
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	// 1. Increment current term and become Candidate
	rn.persistent.CurrentTerm++
	rn.role = Candidate
	rn.persistent.VotedFor = rn.config.NodeID
	rn.leader = nil

	currentTerm := rn.persistent.CurrentTerm
	candidateID := rn.config.NodeID

	rn.logger.Info("starting election", "term", currentTerm, "role", rn.role.String())

	_ = rn.storage.SaveTerm(currentTerm)
	_ = rn.storage.SaveVotedFor(candidateID)

	// 2. Reset election timer with a fresh randomized deadline
	rn.resetElectionTimer()

	// 3. Obtain last log index and term for the log up-to-date check
	lastLogIndex, _ := rn.storage.LastIndex()
	lastLogTerm, _ := rn.storage.LastTerm()

	req := &pb.RequestVoteRequest{
		Term:         currentTerm,
		CandidateId:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1
	totalNodes := len(rn.peers) + 1
	majority := (totalNodes / 2) + 1

	// Fast path: In a single-node cluster, we already have the majority!
	if votesReceived >= majority {
		rn.becomeLeader()
		rn.mu.Unlock()
		return
	}

	rn.mu.Unlock()

	var voteMu sync.Mutex

	for peerID := range rn.peers {
		go func(peer string) {
			res, err := rn.transport.SendRequestVote(peer, req)
			if err != nil {
				rn.logger.Debug("failed to send RequestVote to peer", "peer", peer, "err", err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Check if peer has a higher term than us
			if rn.checkTerm(res.Term) {
				rn.resetElectionTimer()
				return
			}
			// Ignore responses if we are no longer a candidate or term has progressed
			if rn.role != Candidate || rn.persistent.CurrentTerm != currentTerm {
				return
			}

			// If vote was granted, increment vote count
			if res.VoteGranted {
				votesReceived++
				hasMajority := votesReceived >= majority
				voteMu.Unlock()
				if hasMajority {
					rn.becomeLeader()
				}
			}

		}(peerID)
	}
}
