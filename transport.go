package studying_raft

type Transport interface {
	AppendEntries(peerId string, args AppendEntriesArgs, reply *AppendEntriesReply) error
	RequestVote(peerId string, args RequestVoteArgs, reply *RequestVoteReply) error
}
