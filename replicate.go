package studying_raft

// replicate trying to replicate log entries to a single Follower.
func (r *Raft) replicate(peerId string) {
	stopCh := make(chan struct{}, 1)
	defer func() {
		close(stopCh)
		r.slog("leader stop replicate")
	}()
	r.heartBeat(peerId, stopCh)
}

// heartBeat send AppendEntries RPC to follower.
func (r *Raft) heartBeat(peerId string, stopCh <-chan struct{}) {
	args := AppendEntriesArgs{
		Term:     r.getCurrentTerm(),
		LeaderId: r.id,
	}
	for {
		select {
		case <-randomTimeout(r.config().HeartbeatTimeout):
		case <-stopCh:
			return
		}

		reply := AppendEntriesReply{
			Term:    r.getCurrentTerm(),
			Success: false,
		}

		err := r.trans.Call(peerId, "Raft.AppendEntries", args, &reply)
		if err != nil {
			r.slog("failed to send AppendEntries RPC", "peerId", peerId)
			return
		}
		r.setLastContact()
	}
}
