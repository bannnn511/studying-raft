package studying_raft

import (
	"fmt"
	"log"
	"time"
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

type CMState int

const DebugCM = 1

func (r *Raft) run() {
	for {
		select {
		case <-r.shutDownCh:
			r.setLeader("")
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower runs the main loop while in the Follower state.
// Transition into candidate state if heartbeat failed.
func (r *Raft) runFollower() {
	r.slog("entering Follower state", "Follower", r, "Leader", r.leaderId)
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout)

	for r.getState() == Follower {
		select {
		case <-heartbeatTimer:
			// reset heartbeat timer
			hbTimeout := r.config().HeartbeatTimeout
			heartbeatTimer = randomTimeout(hbTimeout)

			// check if we still have contact
			lastContact := r.LastContact()
			if time.Now().Sub(lastContact) <= hbTimeout {
				continue
			}

			// heartbeat failed, transition to Candidate state.
			r.setLeader("")
			r.setState(Candidate)
		case <-r.shutDownCh:
			return
		}
	}
}

// START: CANDIDATE
type voteResult struct {
	RequestVoteResp
	voterID string
}

func (r *Raft) electSelf() <-chan voteResult {
	respCh := make(chan voteResult, len(r.peerIds))
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	lastLogIndex, lastLogTerm := r.getLastEntry()
	req := RequestVoteReq{
		Term:         r.getCurrentTerm(),
		CandidateId:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	askPeer := func(peerId string) {
		reply := voteResult{voterID: peerId}
		err := r.trans.Call(peerId, "Raft.RequestVote", req, &reply.RequestVoteResp)
		if err != nil {
			r.error("failed to make requestVote RPC",
				"target", peerId,
				"error", err,
				"term", req.Term)
			reply.Term = req.Term
			reply.VoteGranted = false
		}

		respCh <- reply
	}

	for _, peerId := range r.peerIds {
		if peerId == r.id {
			r.slog("vote for self", "term", req.Term, "candidateId", r.id)
			if err := r.persistVote(r.getCurrentTerm(), []byte(peerId)); err != nil {
				r.dlog("failed to persist vote")
				return nil
			}

			respCh <- voteResult{
				RequestVoteResp: RequestVoteResp{
					Term:        r.getCurrentTerm(),
					VoteGranted: true,
				},
				voterID: r.id,
			}
		} else {
			r.slog("asking for vote", "term", req.Term, "peerId", peerId)
			askPeer(peerId)
		}
	}

	return respCh
}

// runCandidate runs the main loop while in Candidate state.
func (r *Raft) runCandidate() {
	term := r.getCurrentTerm() + 1
	r.slog("entering Candidate state", "Candidate", r.id, "Term", term)

	electionTimeout := r.config().ElectionTimeout
	electionTimeoutTimer := randomTimeout(electionTimeout)

	grantedVotes := 0
	votesNeeded := r.quorumSize()

	// Candidate elect for itself
	voteResultCh := r.electSelf()

	for r.getState() == Candidate {
		select {
		case <-r.shutDownCh:
			return
		case result := <-voteResultCh:
			// Check if term is higher than ours.
			if result.Term > r.getCurrentTerm() {
				r.slog("newer term discovered, fallback to Follower", "term", result.Term)
				r.setCurrentTerm(result.Term)
				r.setState(Follower)
				return
			}

			// Check if vote is granted.
			if result.VoteGranted {
				grantedVotes++
				r.slog("vote is granted", "from", result.voterID, "term", result.Term)
			}

			if grantedVotes >= votesNeeded {
				r.setState(Leader)
				r.setLeader(r.id)
				r.slog("election won", "votes", grantedVotes, "term", result.Term)
				return
			}
		case <-electionTimeoutTimer:
			r.dlog("Election timeout reached, restarting election")
			return
		}
	}
}

// END: CANDIDATE

// START: LEADER
func (r *Raft) setLeader(id string) {
	r.leaderLock.Lock()
	r.leaderId = id
	r.leaderLock.Unlock()
}

func (r *Raft) setupLeaderState() {
	r.leaderState.commitCh = make(chan struct{}, 1)
	r.leaderState.stepDown = make(chan struct{}, 1)
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh, r.peerIds, r.getLastIndex())
}

// startStopReplication called on the leader whenever there is a message in the log
// and periodically send heartbeat to followers.
func (r *Raft) startStopReplication() {
	lastIdx := r.getLastIndex()

	for _, peerId := range r.peerIds {
		peerId := peerId
		if r.id == peerId {
			continue
		}
		s := &followerReplication{
			id:          r.id,
			currentTerm: r.getCurrentTerm(),
			triggerCh:   make(chan struct{}, 1),
			stopCh:      make(chan struct{}, 1),
			nextIndex:   lastIdx + 1,
			commitment:  r.leaderState.commitment,
		}
		r.goFunc(func() { r.replicate(s) })
	}
}

// runLeaders setup leader state, teardown and start replicating
// then stay in the main leader loop.
func (r *Raft) runLeader() {
	r.slog("entering leader state", "leader", r.id)

	r.setupLeaderState()

	defer func() {
		r.setLeader("")
		r.setLastContact()
		// TODO: stop replication
		// nil vs closed channel ?
		r.leaderState.commitCh = nil
		r.leaderState.stepDown = nil
	}()

	r.startStopReplication()

	r.leaderLoop()
}

func (r *Raft) leaderLoop() {
	for r.getState() == Leader {
		select {
		case <-r.shutDownCh:
			return
		case <-r.leaderState.commitCh:
			newCommitIndex := r.leaderState.commitment.getCommitIndex()
			r.setCommitIndex(newCommitIndex)
		case <-r.leaderState.stepDown:
			r.slog("stepping down")
			return
		}
	}
}

// END: LEADER

// START: RPC

func (r *Raft) RequestVote(req RequestVoteReq, resp *RequestVoteResp) error {
	resp.Term = r.getCurrentTerm()
	resp.VoteGranted = false

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return nil
	}

	if req.Term > r.getCurrentTerm() {
		r.slog("term out of date in RequestVote",
			"candidate", req.CandidateId,
			"currentTerm", r.getCurrentTerm(),
			"candidateTerm", req.Term)
		r.leaderState.stepDown <- struct{}{}
		r.setCurrentTerm(req.Term)
		r.setState(Follower)
		resp.Term = req.Term
		r.slog("become follower", "term", req.Term)
	}

	// Check if we have voted yet.
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err != ErrKeyNotFound {
		r.slog("failed to get last vote term", "error", err)
		return nil
	}

	lastVoteCandidate, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err != ErrKeyNotFound {
		r.slog("failed to get last vote candidate", "error", err)
		return nil
	}

	if lastVoteTerm == req.Term && lastVoteCandidate != nil {
		r.slog("duplicate requestVote for same term", "candidate", req.CandidateId, "term", req.Term)
		if string(lastVoteCandidate) == req.CandidateId {
			r.slog("duplicate requestVote from", "candidate", req.CandidateId)
		}
		return nil
	}

	lastLogIdx, lastTerm := r.getLastEntry()
	// reject if their term is older.
	if lastTerm > req.LastLogTerm {
		r.slog("rejecting vote request since our last log term is greater",
			"candidate", req.CandidateId,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return nil
	}

	if lastTerm == req.LastLogTerm && lastLogIdx > req.LastLogIndex {
		r.slog("rejecting vote request since our last log index is greater",
			"candidate", req.CandidateId,
			"last-index", lastLogIdx,
			"last-candidate-index", req.LastLogIndex)
		return nil
	}

	// Persist vote for safety
	if err := r.persistVote(req.Term, []byte(req.CandidateId)); err != nil {
		r.slog("failed to persist vote", "error", err)
		return nil
	}

	resp.VoteGranted = true
	r.setLastContact()

	return nil
}

func (r *Raft) AppendEntries(req AppendEntriesReq, reply *AppendEntriesResp) error {
	reply.Term = r.getCurrentTerm()
	reply.Success = false

	// ignore older term.
	if req.Term < r.getCurrentTerm() {
		return nil
	}

	if req.Term > r.getCurrentTerm() || r.getState() != Follower {
		r.slog("become follower")
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
	}

	r.setLeader(req.LeaderId)
	if req.PrevLogIndex > 0 {
		// verify last log entry.
		logOk := false
		lastEntryIdx, lastEntryTerm := r.getLastEntry()
		if lastEntryIdx >= req.PrevLogIndex &&
			(req.PrevLogIndex == 0 || lastEntryTerm == req.PrevLogTerm) {
			logOk = true
		}
		if !logOk {
			r.error("last log entry mismatch",
				"follower last entry term", lastEntryTerm,
				"leader previous term", req.PrevLogTerm,
				"follower last entry index", lastEntryIdx,
				"leader previous entry index", req.PrevLogIndex,
			)
			return nil
		}

		// process new entry
		if len(req.Entries) > 0 {
			// delete any conflicting entries, skip any duplicates
			var newEntries Log
			lastLogIdx, _ := r.getLastEntry()
			for i, entry := range req.Entries {
				if entry.Index > lastLogIdx {
					newEntries = req.Entries[i:]
					break
				}

				lastLog := r.log[len(r.log)-1]
				if entry.Term != lastLog.Term {
					r.log = r.log.deleteRange(int(entry.Index), int(lastLogIdx))
					newEntries = req.Entries[i:]
					break
				}
			}

			if n := len(newEntries); n > 0 {
				r.log = append(r.log, newEntries...)
				last := r.log[n-1]
				r.setLastEntry(last)
			}
		}

		// update the commit index
		if req.LeaderCommit > 0 && req.LeaderCommit > r.getCommitIndex() {
			idx := min(req.LeaderCommit, r.getLastIndex())
			r.setCommitIndex(idx)
			r.processLog(idx)
		}
	}

	reply.Success = true
	r.setLastContact()

	return nil
}

// END: RPC

func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}

	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}

	return nil
}

// dlog logs a debugging message is DebugCM > 0.
func (r *Raft) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%s] ", r.id) + format
		log.Printf(format, args...)
	}
}

// slog logs a debugging message is DebugCM > 0.
func (r *Raft) slog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%s-%s] ", r.id, r.getState()) + format
		r.logger.Infow(format, args...)
	}
}

// slog logs a debugging message is DebugCM > 0.
func (r *Raft) error(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%s-%s]", r.id, r.getState()) + format
		r.logger.Errorw(format, args...)
	}
}

func (r *Raft) config() Config {
	return r.conf.Load().(Config)
}

func (r *Raft) setLastContact() {
	r.lastContactLock.RLock()
	r.lastContact = time.Now()
	r.lastContactLock.RUnlock()
}

func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()

	return last
}

func (r *Raft) quorumSize() int {
	voters := len(r.peerIds)

	return voters/2 + 1
}

// Report reports the state of this CM.
func (r *Raft) Report() (id string, term uint64, isLeader bool) {
	return r.id, r.getCurrentTerm(), r.getState() == Leader
}

// processLog applies all committed entries that haven't been applied
// up to the given index limit.
// Follower call this from AppendEntries.
func (r *Raft) processLog(index uint64) {
	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		r.error("skipping application of old log", "index", index, "lastApplied", lastApplied)
	}

	savedTerm := r.getCurrentTerm()
	for idx := lastApplied + 1; idx <= index; idx++ {
		r.commitChan <- CommitEntry{
			Command: r.log[idx],
			Index:   lastApplied + idx + 1,
			Term:    savedTerm,
		}
	}
}
