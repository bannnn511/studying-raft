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
	id int

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
	peerIds []int

	// server is the server contains this CM to issues RPC calls to peers.
	server *Server

	// stable is a StableStore implementation for durable state
	// It provides stable storage for many fields in raftState
	stable StableStore

	votedFor int

	log []CommitEntry

	shutDownCh chan struct{}

	leaderLock sync.RWMutex
	leaderId   int

	logger *zap.SugaredLogger
}

func NewRaft(config *Config, peerIds []int, server *Server) *Raft {
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
			r.setLeader(-1)
			r.setState(Candidate)
		case <-r.shutDownCh:
			return
		}
	}
}

// START: LEADER
func (r *Raft) setLeader(id int) {
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
	voterID int
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

	askPeer := func(peerId int) {
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
	voteResult := r.electSelf()

	for r.getState() == Candidate {
		select {
		case result := <-voteResult:
			if result.Term == r.getCurrentTerm() && result.VoteGranted {
				grantedVotes++
			}

			if grantedVotes >= votesNeeded {
				r.setState(Leader)
				r.setLeader(r.id)
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

	var lastTerm uint64 = 0
	var logLength = uint64(len(r.log))
	if logLength > 0 {
		lastTerm = r.log[logLength-1].Term
	}
	logOke := (req.LastLogTerm > lastTerm) ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= logLength)

	if req.Term == r.getCurrentTerm() && logOke && r.votedFor == -1 {
		resp.VoteGranted = true
		return
	}
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
