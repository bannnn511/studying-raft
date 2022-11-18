package studying_raft

import "time"

type Config struct {
	// HeartbeatTimeout specifies the time in follower state without contact
	// from a leader before we attempt an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout specifies the time in candidate state without contact
	// from a leader before we attempt an election.
	ElectionTimeout time.Duration
}

func (r *Raft) DefaultConfiguration() *Config {
	return &Config{
		HeartbeatTimeout: 50 * time.Millisecond,
		ElectionTimeout:  50 * time.Millisecond,
	}
}
