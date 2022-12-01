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

	// commitment tracks the entries acknowledged
	commitment *commitment
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
	args := AppendEntriesReq{
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

		reply := AppendEntriesResp{
			Term:    r.getCurrentTerm(),
			Success: false,
		}

		err := r.trans.Call(s.id, "Raft.AppendEntries", args, &reply)
		if err != nil {
			r.error("failed to send heartbeat AppendEntries RPC", "peerId", s.id)
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
	req := AppendEntriesReq{
		Term:         s.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: lastLogIdx,
		PrevLogTerm:  lastLogTerm,
		LeaderCommit: 0,
		Entries:      nil,
	}

	var reply AppendEntriesResp
	if err := r.trans.Call(s.id, "Raft.AppendEntries", req, &reply); err != nil {
		r.error("failed to AppendEntries to", "peerId", s.id, "err", err)
	}

	// START: Leader receiving log acknowledgements.

	// check for newer term, stop running
	if reply.Term > r.getCurrentTerm() {
		r.slog("peer has newer term, stopping replication", "peer", s.id)
		r.setState(Follower)
		return
	}

	r.setLastContact()

	// update s based on success
	if reply.Success {
		updateLastAppended(s, &req)
	} else {
		atomic.StoreUint64(&s.nextIndex, s.nextIndex-1)
		r.slog("AppendEntries failed, sending older logs", "peer", s.id, "next", atomic.LoadUint64(&s.nextIndex))
	}
	// END: Leader receiving log acknowledgements.
}

// updateLastAppended is used to update follower replication state after a successful AppendEntries RPC.
func updateLastAppended(s *followerReplication, req *AppendEntriesReq) {
	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		atomic.StoreUint64(&s.nextIndex, last.Index-1)
		s.commitment.match(s.id, last.Index)
	}
}
