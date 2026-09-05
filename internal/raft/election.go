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

// HandleAppendEntries processes incoming AppendEntries RPCs from the leader (replicated logs and heartbeats).
func (rn *RaftNode) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 1. Reply false if leader's term is older than our current term (§5.1)
	if req.Term < rn.persistent.CurrentTerm {
		return &pb.AppendEntriesResponse{
			Term:    rn.persistent.CurrentTerm,
			Success: false,
		}
	}

	// 2. If leader's term is higher, update our term and step down (§5.1)
	if req.Term > rn.persistent.CurrentTerm {
		rn.checkTerm(req.Term)
	}

	// If we were a candidate and received a valid heartbeat from the leader of the current term,
	// acknowledge the leader and revert to follower (§5.2)
	if rn.role == Candidate && req.Term == rn.persistent.CurrentTerm {
		rn.role = Follower
		rn.logger.Info("received valid heartbeat from leader,stepping down to follower", "leader", req.LeaderId, "term", req.Term)
	}

	// 3. Log consistency check (§5.3):
	// Reply false if our log doesn't contain an entry at req.PrevLogIndex matching req.PrevLogTerm

	if req.PrevLogIndex > 0 {
		entry, err := rn.storage.GetEntry(req.PrevLogIndex)
		if err != nil || entry == nil {
			// Follower is missing the entry at PrevLogIndex!
			return &pb.AppendEntriesResponse{
				Term:    rn.persistent.CurrentTerm,
				Success: false,
			}
		}
		if entry.Term != req.PrevLogTerm {
			// Term mismatch at PrevLogIndex!
			return &pb.AppendEntriesResponse{
				Term:    rn.persistent.CurrentTerm,
				Success: false,
			}
		}
	}

	// 4. Handle log conflicts and append new entries (§5.3):
	// If an existing entry conflicts with a new one (same index but different term),
	// delete the existing entry and all that follow it (§5.3)

	for i, newEntry := range req.Entries {
		existingIndex := req.PrevLogIndex + 1 + uint64(i)
		existing, err := rn.storage.GetEntry(existingIndex)
		if err != nil || existing == nil {
			// No existing entry at this index: append this entry and all subsequent entries
			toAppend := req.Entries[i:]
			if err := rn.storage.AppendEntries(toAppend); err != nil {
				rn.logger.Info("failed to append entries to storage", "err", err)

				return &pb.AppendEntriesResponse{
					Term:    rn.persistent.CurrentTerm,
					Success: false,
				}
				rn.persistent.Log = append(rn.persistent.Log, toAppend...)
				break
			}

		}

		if existing.Term != newEntry.Term {
			// Conflict detected! Truncate log from existingIndex onward
			rn.logger.Warn("log conflict detected, truncating from index",
				"index", existingIndex,
				"existingTerm", existing.Term,
				"newTerm", newEntry.Term,
			)

			if err := rn.storage.TruncateFrom(existingIndex); err != nil {

				rn.logger.Error("failed to truncate log", "err", err)
				return &pb.AppendEntriesResponse{
					Term:    rn.persistent.CurrentTerm,
					Success: false,
				}
			}

			if existingIndex < uint64(len(rn.persistent.Log)) {
				rn.persistent.Log = rn.persistent.Log[:existingIndex]
			}

			// Append new entry and all remaining entries from leader
			toAppend := req.Entries[i:]
			if err := rn.storage.AppendEntries(toAppend); err != nil {
				rn.logger.Info("failed to append entries to storage after truncate", "error", err)
				return &pb.AppendEntriesResponse{
					Term:    rn.persistent.CurrentTerm,
					Success: false,
				}
			}
			rn.persistent.Log = append(rn.persistent.Log, toAppend...)
			break

		}

		// If existing.Term == newEntry.Term, entry already matches! Keep it and check next entry.

	}
	// 5. Update follower's commitIndex (§5.3):
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > rn.volatile.CommitIndex {
		lastLogIndex, _ := rn.storage.LastIndex()
		newCommitIndex := req.LeaderCommit
		if lastLogIndex < req.LeaderCommit {
			newCommitIndex = lastLogIndex
		}

		if newCommitIndex > rn.volatile.CommitIndex {
			rn.volatile.CommitIndex = newCommitIndex
			rn.logger.Info("follower advanced commitIndex", "commitIndex", rn.volatile.CommitIndex, "leaderCommit", req.LeaderCommit)
			

			// 6. Apply newly committed entries to the follower's KV state machine!
			rn.applyCommittedEntriesLocked()
	}
	return &pb.AppendEntriesResponse{
		Term:    rn.persistent.CurrentTerm,
		Success: true,
	}

}
