package raft

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
