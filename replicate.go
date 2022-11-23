package studying_raft

import "sync/atomic"

type followerReplication struct {
	// id is id of follower
	id string

	// currentTerm is the term of this leader, to be included in AppendEntries request.
	currentTerm uint64

	// triggerCh is notified everytime new entries are appended to the log.
	triggerCh chan struct{}

	// stopCh is notified when leader step down from.
	stopCh chan struct{}

	// nextIndex is the index of the next log entry to send to the follower,
	// which may fall past the end of the log.
	nextIndex uint64
}

// replicate trying to replicate log entries to a single Follower.
func (r *Raft) replicate(s *followerReplication) {
	stopCh := make(chan struct{}, 1)
	defer func() {
		close(stopCh)
	}()
	r.heartBeat(s, stopCh)

	for {
		select {
		case <-s.stopCh:
			return
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastEntry()
			r.replicateTo(s, lastLogIdx)
		}
	}
}

// heartBeat send AppendEntries RPC to follower.
func (r *Raft) heartBeat(s *followerReplication, stopCh <-chan struct{}) {
	args := AppendEntriesArgs{
		Term:     s.currentTerm,
		LeaderId: r.id,
	}
	for {
		select {
		case <-randomTimeout(r.config().HeartbeatTimeout):
		case <-stopCh:
			return
		case <-r.shutDownCh:
			return
		}

		reply := AppendEntriesReply{
			Term:    r.getCurrentTerm(),
			Success: false,
		}

		err := r.trans.Call(s.id, "Raft.AppendEntries", args, &reply)
		if err != nil {
			r.slog("failed to send heartbeat AppendEntries RPC", "peerId", s.id)
			// return will violate one leader rule
			// because previous leader does not know about new leader.
			//return
		}
		r.setLastContact()
	}
}

// replicateTo is a helper function of replicate(), used to replicate the logs
// up a given last index.
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) {
	lastLogIdx, lastLogTerm := r.getLastEntry()
	req := AppendEntriesArgs{
		Term:         s.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: lastLogIdx,
		PrevLogTerm:  lastLogTerm,
		LeaderCommit: 0,
		Entries:      nil,
	}

	var reply AppendEntriesReply
	if err := r.trans.Call(s.id, "Raft.AppendEntries", req, &reply); err != nil {
		r.error("failed to AppendEntries to", "peerId", s.id, "err", err)
	}

	if reply.Term > r.getCurrentTerm() {
		r.slog("peer has newer term, stopping replication", "peer", s.id)
		r.setState(Follower)
		return
	}

	r.setLastContact()

	if reply.Success {

	} else {
		atomic.StoreUint64(&s.nextIndex, s.nextIndex-1)
		r.slog("AppendEntries failed, sending older logs", "peer", s.id, "next", atomic.LoadUint64(&s.nextIndex))
	}
}
