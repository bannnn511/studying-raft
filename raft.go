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

// CommitEntry is the data reported by raft to the commit channel.
// Each entry notifies the client that consensus was reached on a command and
// it can be applied to client's state machine.
type CommitEntry struct {
	// Command is the client command being committed.
	Command interface{}

	// Index is the log index at which the client command is committed.
	Index uint64

	// Term is the Raft term at which the client command is committed
	Term uint64
}

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
	RequestVoteReply
	voterID string
}

func (r *Raft) electSelf() <-chan voteResult {
	respCh := make(chan voteResult, len(r.peerIds))
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	lastLogIndex, lastLogTerm := r.getLastEntry()
	req := RequestVoteArgs{
		Term:         r.getCurrentTerm(),
		CandidateId:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	askPeer := func(peerId string) {
		reply := voteResult{voterID: peerId}
		err := r.trans.Call(peerId, "Raft.RequestVote", req, &reply.RequestVoteReply)
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
				RequestVoteReply: RequestVoteReply{
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
}

// startStopReplication called on the leader whenever there is a message in the log
// and periodically send heartbeat to followers.
func (r *Raft) startStopReplication() {
	for _, peerId := range r.peerIds {
		peerId := peerId
		if r.id == peerId {
			continue
		}
		r.goFunc(func() { r.replicate(peerId) })
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
		// TODO: handle leaderState signal
		case <-r.leaderState.commitCh:
		case <-r.leaderState.stepDown:
			/*
				case leader lease:
				check
			*/
		}
	}
}

// END: LEADER

// START: RPC

func (r *Raft) RequestVote(req RequestVoteArgs, resp *RequestVoteReply) error {
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

func (r *Raft) AppendEntries(req AppendEntriesArgs, reply *AppendEntriesReply) error {
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
	// TODO: verify last log entry
	// TODO: process new entry
	// TODO: update the commit index

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
		format = fmt.Sprintf("[%v][%s-%s] ", r.getCurrentTerm(), r.id, r.getState()) + format
		r.logger.Infow(format, args...)
	}
}

// slog logs a debugging message is DebugCM > 0.
func (r *Raft) error(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%s-%s: term-%v] ", r.id, r.getState(), r.getCurrentTerm()) + format
		r.logger.Errorf(format, args...)
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
