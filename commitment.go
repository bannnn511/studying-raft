package studying_raft

import "sync"

type commitment struct {
	// protects matchIndexes and commitIndex
	sync.Mutex
	// notified when commitIndex increases
	commitCh chan struct{}
	// voter ID to log index: the server stores up through this log entry
	matchIndexes map[string]uint64
	// a quorum stores up through this log entry. monotonically increases.
	commitIndex uint64
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	startIndex uint64
}

// newCommitment returns
func newCommitment(commitCh chan struct{}, peerIds []string, startIndex uint64) *commitment {
	matchIndexes := make(map[string]uint64)
	for _, server := range peerIds {
		matchIndexes[server] = 0
	}

	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
	}
}

func (c commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}
