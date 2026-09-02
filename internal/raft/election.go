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
				// Use a mutex to safely update the shared vote counter
				voteMu.Lock()
				votesReceived++
				hasMajority := votesReceived >= majority
				voteMu.Unlock()
				if hasMajority && rn.role == Candidate {
					rn.becomeLeader()
				}
			}

		}(peerID)
	}
}

// becomeLeader transitions a candidate to the Leader role and starts pulsing heartbeats.
// NOTE: Caller MUST hold rn.mu.
func (rn *RaftNode) becomeLeader() {
	if rn.role != Candidate {
		return
	}

	rn.role = Leader
	lastLogIndex, _ := rn.storage.LastIndex()

	// Initialize volatile leader state (re-initialized after each election)
	nextIndex := make(map[string]uint64)
	matchIndex := make(map[string]uint64)
	for peerID := range rn.peers {
		nextIndex[peerID] = lastLogIndex + 1
		matchIndex[peerID] = 0
	}

	rn.leader = &LeaderState{
		NextIndex:  nextIndex,
		MatchIndex: matchIndex,
	}

	rn.logger.Info("election won: become leader", "term", rn.persistent.CurrentTerm)

	// Send immediate heartbeats using the locked version (we already hold rn.mu!)
	rn.sendHeartbeatsLocked()

	// Reset heartbeat ticker to pulse periodically
	rn.resetHeartbeatTimer()
}

// sendHeartbeats is called by the timer when rn.mu is NOT held.
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.sendHeartbeatsLocked()
}

// sendHeartbeatsLocked broadcasts empty AppendEntries RPCs to all peers in parallel.
// NOTE: Caller MUST hold rn.mu.
func (rn *RaftNode) sendHeartbeatsLocked() {
	if rn.role != Leader {
		return
	}

	currentTerm := rn.persistent.CurrentTerm
	leaderID := rn.config.NodeID
	commitIndex := rn.volatile.CommitIndex

	for peerID := range rn.peers {
		prevIndex := rn.leader.NextIndex[peerID] - 1
		var prevTerm uint64
		if entry, err := rn.storage.GetEntry(prevIndex); err == nil && entry != nil {
			prevTerm = entry.Term
		}

		req := &pb.AppendEntriesRequest{
			Term:         currentTerm,
			LeaderId:     leaderID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      nil, // Empty slice signifies a heartbeat
			LeaderCommit: commitIndex,
		}

		go func(peer string, r *pb.AppendEntriesRequest) {
			resp, err := rn.transport.SendAppendEntries(peer, r)
			if err != nil {
				rn.logger.Debug("failed to send heartbeat to peer", "peer", peer, "err", err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if rn.checkTerm(resp.Term) {
				rn.resetElectionTimer()
				return
			}
		}(peerID, req)
	}
}

// HandleRequestVote handles an incoming RequestVote RPC from a candidate.
func (rn *RaftNode) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Rule 1: Reject votes if candidate's term is older than our current term
	if req.Term < rn.persistent.CurrentTerm {
		return &pb.RequestVoteResponse{
			Term:        rn.persistent.CurrentTerm,
			VoteGranted: false,
		}
	}

	// If candidate's term is newer, step down to Follower
	if req.Term > rn.persistent.CurrentTerm {
		rn.checkTerm(req.Term)
	}

	// Rule 2: We can only vote if we haven't voted yet in this term, or already voted for this candidate
	canVote := rn.persistent.VotedFor == "" || rn.persistent.VotedFor == req.CandidateId

	// Rule 3: Election Safety (Raft §5.4.1) — Log Up-To-Date check:
	// A voter denies its vote if its own log is more up-to-date than the candidate's.
	lastLogIndex, _ := rn.storage.LastIndex()
	lastLogTerm, _ := rn.storage.LastTerm()

	logsUpToDate := false
	if req.LastLogTerm > lastLogTerm {
		logsUpToDate = true
	} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex {
		logsUpToDate = true
	}

	if canVote && logsUpToDate {
		rn.persistent.VotedFor = req.CandidateId
		_ = rn.storage.SaveVotedFor(req.CandidateId)
		rn.resetElectionTimer() // Granting a vote resets the election timer
		// we want to give them time to finish the election and send us a heartbeat

		rn.logger.Info("granted vote to candidate", "candidate", req.CandidateId, "term", req.Term)
		return &pb.RequestVoteResponse{
			Term:        rn.persistent.CurrentTerm,
			VoteGranted: true,
		}
	}

	return &pb.RequestVoteResponse{
		Term:        rn.persistent.CurrentTerm,
		VoteGranted: false,
	}
}

func (rn *RaftNode) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Reply false if leader's term is older than current term (§5.1)
	if req.Term < rn.persistent.CurrentTerm {
		return &pb.AppendEntriesResponse{
			Term:    rn.persistent.CurrentTerm,
			Success: false,
		}
	}

	// If leader's term is higher, update our term and step down
	if req.Term > rn.persistent.CurrentTerm {
		rn.checkTerm(req.Term)
	}

	// If we were a candidate and received a valid heartbeat from the leader of the current term,
	// acknowledge the leader and revert to follower
	if rn.role == Candidate && req.Term == rn.persistent.CurrentTerm {
		rn.role = Follower
		rn.logger.Info("received heartbeat from leader, stepping down to follower", "leader", req.LeaderId, "term", req.Term)
	}

	// Reset election timer because we received a valid heartbeat from the active leader
	rn.resetElectionTimer()

	// (Phase 3 will add full log consistency checks and entry appending here)
	return &pb.AppendEntriesResponse{
		Term:    rn.persistent.CurrentTerm,
		Success: true,
	}
}
