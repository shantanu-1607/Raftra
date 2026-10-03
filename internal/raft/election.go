package raft

import (
	"sync"
	"time"

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

		//adding the metrics
		rn.metrics.SetNodeRole(0) //0 = follower
		rn.metrics.SetCurrentTerm(incomingTerm)

		// Abort any in-flight proposals on this node since leadership was lost (§5.1)
		for idx, ch := range rn.pendingCommits {
			ch <- ErrNotLeader
			delete(rn.pendingCommits, idx)
		}

		// Persist the updated term and cleared vote
		_ = rn.storage.SaveTerm(incomingTerm)
		_ = rn.storage.SaveVotedFor("")
		return true
	}
	return false
}

// startElection runs when the election timer fires. It first holds a pre-vote
// (Raft thesis §9.6): it asks the peers whether they would vote for it in the
// next term, without anyone changing their term. Only if a majority says yes
// does it start the real election (campaignLocked). A node that merely cannot
// hear a healthy leader (e.g. a restarted node the leader has not reconnected
// to yet) is refused, so it can no longer bump the term and depose that leader.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.resetElectionTimer()

	// Single-node cluster: there is nobody to ask.
	if len(rn.peers) == 0 {
		rn.campaignLocked()
		return
	}

	term := rn.persistent.CurrentTerm
	lastLogIndex, _ := rn.storage.LastIndex()
	lastLogTerm, _ := rn.storage.LastTerm()
	req := &pb.RequestVoteRequest{
		Term:         term + 1,
		CandidateId:  rn.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		PreVote:      true,
	}

	rn.logger.Info("starting pre-vote", "proposed_term", term+1)

	// Shared by the goroutines below, which only touch them while holding rn.mu.
	votes := 1 // our own
	majority := (len(rn.peers)+1)/2 + 1
	started := time.Now()
	campaigned := false

	for peerID := range rn.peers {
		go func(peer string) {
			rn.metrics.IncRequestVoteTotal()
			res, err := rn.transport.SendRequestVote(peer, req)
			if err != nil {
				rn.logger.Debug("failed to send pre-vote to peer", "peer", peer, "err", err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// A voter in a newer term refused us: adopt its term (§5.1)
			if rn.checkTerm(res.Term) {
				rn.resetElectionTimer()
				return
			}
			// Ignore stale answers: we already moved on, or a leader has contacted us since
			if campaigned || rn.role == Leader || rn.persistent.CurrentTerm != term || rn.lastLeaderContact.After(started) {
				return
			}
			if res.VoteGranted {
				votes++
				if votes >= majority {
					campaigned = true
					rn.campaignLocked()
				}
			}
		}(peerID)
	}
}

// campaignLocked runs a real election (§5.2): increment the term, vote for
// ourselves and request votes from every peer.
// NOTE: Caller MUST hold rn.mu.
func (rn *RaftNode) campaignLocked() {
	// 1. Increment current term and become Candidate
	rn.persistent.CurrentTerm++
	rn.role = Candidate
	rn.persistent.VotedFor = rn.config.NodeID
	rn.leader = nil

	//metrics
	rn.metrics.SetNodeRole(1) // 1= candidate
	rn.metrics.SetCurrentTerm(rn.persistent.CurrentTerm)
	rn.metrics.IncLeaderElections()

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
		return
	}

	var voteMu sync.Mutex

	for peerID := range rn.peers {
		go func(peer string) {
			rn.metrics.IncRequestVoteTotal()
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
	rn.metrics.SetNodeRole(2) //2 = leader
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

	// Append a no-op entry from the new term (Raft §8). A leader may only commit
	// entries from its own term (§5.4.2), so without it, entries left over from
	// earlier terms (e.g. after every node restarted and lost commitIndex) would
	// stay uncommitted and unreadable until the next client write. The empty
	// Command is skipped by applyCommittedEntriesLocked.
	noop := &pb.LogEntry{Index: lastLogIndex + 1, Term: rn.persistent.CurrentTerm}
	if err := rn.storage.AppendEntries([]*pb.LogEntry{noop}); err != nil {
		rn.logger.Error("failed to append no-op entry", "err", err)
	} else {
		rn.persistent.Log = append(rn.persistent.Log, noop)
	}
	// In a single-node cluster nobody will acknowledge it: commit right away.
	rn.checkAndUpdateCommitIndexLocked()

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

// sendHeartbeatsLocked broadcasts AppendEntries RPCs to all peers in parallel.
// Per Raft §5.2 & §5.3, heartbeats carry any pending log entries needed to bring followers up to date.
// NOTE: Caller MUST hold rn.mu.
func (rn *RaftNode) sendHeartbeatsLocked() {
	rn.broadcastAppendEntriesLocked()
}

// HandleRequestVote handles an incoming RequestVote RPC from a candidate.
func (rn *RaftNode) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.PreVote {
		return rn.handlePreVoteLocked(req)
	}

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

	// Rule 3: Election Safety (Raft §5.4.1) — Log Up-To-Date check
	logsUpToDate := rn.candidateLogUpToDateLocked(req)

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

// handlePreVoteLocked answers a pre-vote (Raft thesis §9.6) without changing any
// state: no term update, no recorded vote, no timer reset. It grants only if the
// proposed term is newer than ours, the candidate's log is at least as up-to-date
// as ours (§5.4.1), and we have not heard from a leader within the minimum
// election timeout. A leader never grants, since it is the live leader.
// NOTE: Caller MUST hold rn.mu.
func (rn *RaftNode) handlePreVoteLocked(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	leaderIsAlive := rn.role == Leader || time.Since(rn.lastLeaderContact) < rn.config.ElectionTimeoutMin
	grant := req.Term > rn.persistent.CurrentTerm && !leaderIsAlive && rn.candidateLogUpToDateLocked(req)

	return &pb.RequestVoteResponse{
		Term:        rn.persistent.CurrentTerm,
		VoteGranted: grant,
	}
}

// candidateLogUpToDateLocked reports whether the candidate's log is at least as
// up-to-date as ours (§5.4.1): a later last term wins; with equal last terms, the
// longer log wins. A voter denies its vote to a candidate whose log is behind.
// NOTE: Caller MUST hold rn.mu.
func (rn *RaftNode) candidateLogUpToDateLocked(req *pb.RequestVoteRequest) bool {
	lastLogIndex, _ := rn.storage.LastIndex()
	lastLogTerm, _ := rn.storage.LastTerm()

	if req.LastLogTerm != lastLogTerm {
		return req.LastLogTerm > lastLogTerm
	}
	return req.LastLogIndex >= lastLogIndex
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
		rn.logger.Info("received valid heartbeat from leader, stepping down to follower", "leader", req.LeaderId, "term", req.Term)
	}

	// Record the current leader ID and reset election timer (§5.2)
	rn.leaderID = req.LeaderId
	rn.lastLeaderContact = time.Now()
	rn.resetElectionTimer()

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
				rn.logger.Error("failed to append entries to storage", "err", err)
				return &pb.AppendEntriesResponse{
					Term:    rn.persistent.CurrentTerm,
					Success: false,
				}
			}
			rn.persistent.Log = append(rn.persistent.Log, toAppend...)
			break
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
				rn.logger.Error("failed to append entries to storage after truncate", "error", err)
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
		// Only entries up to PrevLogIndex+len(Entries) are known to match the leader's log;
		// the local tail beyond that may be divergent (AppendEntries batches are capped).
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		newCommitIndex := req.LeaderCommit
		if lastNewIndex < newCommitIndex {
			newCommitIndex = lastNewIndex
		}

		if newCommitIndex > rn.volatile.CommitIndex {
			rn.volatile.CommitIndex = newCommitIndex
			rn.logger.Info("follower advanced commitIndex", "commitIndex", rn.volatile.CommitIndex, "leaderCommit", req.LeaderCommit)

			// 6. Apply newly committed entries to the follower's KV state machine!
			rn.applyCommittedEntriesLocked()
		}
	}

	return &pb.AppendEntriesResponse{
		Term:    rn.persistent.CurrentTerm,
		Success: true,
	}
}
