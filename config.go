package studying_raft

import "time"

type Config struct {
	// id is the Server ID of this CM.
	id int

	// HeartbeatTimeout specifies the time in follower state without contact
	// from a leader before we attempt an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout specifies the time in candidate state without contact
	// from a leader before we attempt an election.
	ElectionTimeout time.Duration
}
