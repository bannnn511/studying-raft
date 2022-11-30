package studying_raft

type RequestVoteArgs struct {
	// Term is candidate's term.
	Term uint64

	// CandidateId is candidate requesting vote.
	CandidateId string

	// LastLogIndex is index of last log index.
	LastLogIndex uint64

	// LastLogTerm is term of candidate's last log entry.
	LastLogTerm uint64
}

type RequestVoteReply struct {
	// Term is current term for candidate to update itself.
	Term uint64

	// VoteGranted is true means candidate received vote.
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Term is leader term.
	Term uint64

	// LeaderId for follower to redirect client.
	LeaderId string

	// PrevLogIndex is index of log entry immediately preceding the new ones in its log.
	PrevLogIndex uint64

	// PrevLogTerm is term of prevLogIndex.
	PrevLogTerm uint64

	// LeaderCommit is leader's commit index.
	LeaderCommit uint64

	// log entries to store.
	Entries Log
}

type AppendEntriesReply struct {
	// current term for leader to update itself.
	Term uint64

	// True if follower contained entry matching PrevLogIndex and PrevLogTerm.
	Success bool
}
