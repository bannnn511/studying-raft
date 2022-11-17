package studying_raft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
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

// Raft implements a single node of Raft consensus.
type Raft struct {
	raftState

	// id is the Server ID of this CM.
	id string

	// Transport protocol.
	trans Transport

	// lastContact is the last time that we have contact with
	// leader node.
	lastContact     time.Time
	lastContactLock sync.RWMutex

	// conf stores the current configuration to use. This is the most recent one
	// provided. All reads of config values should use the config() helper method
	// to read this safely.
	conf atomic.Value

	// commitChan is the channel where this CM is going to report committed log
	// entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on commitChan.
	newCommitReadyChan chan struct{}

	// peerIds is ID list of all servers in the cluster.
	peerIds []string

	// server is the server contains this CM to issues RPC calls to peers.
	server *Server

	// stable is a StableStore implementation for durable state
	// It provides stable storage for many fields in raftState
	stable StableStore

	votedFor int

	log []CommitEntry

	shutDownCh chan struct{}

	leaderLock sync.RWMutex
	leaderId   string

	logger *zap.SugaredLogger
}

func NewRaft(config *Config, peerIds []string, server *Server) *Raft {
	logger, _ := zap.NewProduction()

	cm := new(Raft)

	cm.logger = logger.Sugar()
	// Initialize as Follower
	cm.setState(Follower)
	cm.peerIds = peerIds
	cm.votedFor = -1
	cm.conf.Store(config)

	cm.goFunc(cm.run)

	return cm
}

func (r *Raft) run() {
	for {
		select {
		case <-r.shutDownCh:
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

func (r *Raft) runFollower() {
	r.slog("entering Follower state", "Follower", r, "Leader", r.leaderId)
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout)

	for r.getState() == Follower {
		select {
		case <-heartbeatTimer:

			lastContact := r.LastContact()
			hbTimeout := r.config().HeartbeatTimeout
			currentTime := time.Now()

			// check if we still have contact
			if currentTime.Sub(lastContact) <= hbTimeout {
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

// START: LEADER
func (r *Raft) setLeader(id string) {
	r.leaderLock.Lock()
	r.leaderId = id
	r.leaderLock.Unlock()
}

func (r *Raft) runLeader() {

}

// END: LEADER

// START: CANDIDATE
type voteResult struct {
	RequestVoteReply
	voterID string
}

func (r *Raft) electSelf() <-chan voteResult {
	respCh := make(chan voteResult, len(r.peerIds))
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	lastLogIndex, lastLogTerm := r.getLastEntry()
	requestVote := RequestVoteArgs{
		Term:         r.getCurrentTerm(),
		CandidateId:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	askPeer := func(peerId string) {
		reply := voteResult{voterID: peerId}
		err := r.trans.RequestVote(requestVote, &reply.RequestVoteReply)
		if err != nil {
			reply.Term = r.getCurrentTerm()
			reply.VoteGranted = false
		}

		respCh <- reply
	}

	for _, peerId := range r.peerIds {
		if peerId == r.id {
			r.slog("vote for itself", "candidateId", r.id)
			if err := r.persistVote(r.getCurrentTerm(), []byte(fmt.Sprintf("%v", peerId))); err != nil {
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
			r.slog("sending RequestVote", "peerId", peerId)
			askPeer(peerId)
		}
	}

	return respCh
}

func (r *Raft) runCandidate() {
	term := r.getCurrentTerm() + 1
	r.slog("entering Candidate state", "Candidate", r, "Term", term)

	electionTimeout := r.config().ElectionTimeout
	electionTimeoutTimer := randomTimeout(electionTimeout)

	grantedVotes := 0
	votesNeeded := r.quorumSize()

	// Candidate elect for itself
	voteResultCh := r.electSelf()

	for r.getState() == Candidate {
		select {
		case result := <-voteResultCh:
			// Check if term is higher than ours.
			if result.Term > r.getCurrentTerm() {
				r.slog("newer term discovered, fallback to Follower", "term", result.Term)
				r.setCurrentTerm(result.Term)
				r.setState(Follower)
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
		case <-r.shutDownCh:
			return
		}
	}
}

// END: CANDIDATE

func (r *Raft) RequestVote(req RequestVoteArgs, resp *RequestVoteReply) {
	resp = &RequestVoteReply{
		Term:        r.getCurrentTerm(),
		VoteGranted: false,
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	if req.Term > r.getCurrentTerm() {
		r.slog("lost leadership because received a RequestVote with a newer term",
			"currentTerm", r.getCurrentTerm(),
			"candidateTerm", req.Term)
		r.setCurrentTerm(req.Term)
		r.setState(Follower)
		resp.Term = req.Term
	}

	// Check if we have voted yet.
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		r.slog("failed to get last vote term", "error", err)
		return
	}

	lastVoteCandidate, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		r.slog("failed to get last vote candidate", "error", err)
		return
	}
	if lastVoteTerm == req.Term && lastVoteCandidate != nil {
		r.slog("duplicate requestVote for same term", "term", req.Term)
		if string(lastVoteCandidate) == req.CandidateId {
			r.slog("duplicate requestVote from", "candidate", req.CandidateId)
		}
		return
	}

	lastLogIdx, lastTerm := r.getLastEntry()
	// reject if their term is older.
	if lastTerm > req.LastLogTerm {
		r.slog("rejecting vote request since our last log term is greater",
			"candidate", req.CandidateId,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastLogIdx > req.LastLogIndex {
		r.slog("rejecting vote request since our last log index is greater",
			"candidate", req.CandidateId,
			"last-index", lastLogIdx,
			"last-candidate-index", req.LastLogIndex)
		return
	}

	// Persist vote for safety
	if err := r.persistVote(req.Term, []byte(req.CandidateId)); err != nil {
		r.slog("failed to persist vote", "error", err)
		return
	}

	resp.VoteGranted = true
	r.setLastContact()

	return
}

func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyCurrentTerm, term); err != nil {
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
		format = fmt.Sprintf("[%d] ", r.id) + format
		log.Printf(format, args...)
	}
}

// slog logs a debugging message is DebugCM > 0.
func (r *Raft) slog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", r.id) + format
		r.logger.Infow(format, args...)
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
