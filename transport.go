package studying_raft

type Transport interface {
	AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error
	RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error
}
