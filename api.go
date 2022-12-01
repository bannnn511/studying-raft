package studying_raft

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

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
	commitChan chan CommitEntry

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

	log Log

	shutDownCh chan struct{}

	leaderLock sync.RWMutex
	leaderId   string
	// leaderState used only while state is leader
	leaderState leaderState

	logger *zap.SugaredLogger
}

func NewRaft(id string, config *Config, peerIds []string, server *Server, store StableStore) *Raft {
	logger, _ := zap.NewProduction()

	r := new(Raft)

	r.id = id
	r.logger = logger.Sugar()
	// Initialize as Follower
	r.setState(Follower)
	r.peerIds = peerIds
	r.conf.Store(*config)
	r.trans = server
	r.stable = store
	r.shutDownCh = make(chan struct{})

	r.goFunc(r.run)

	return r
}

type leaderState struct {
	commitCh   chan struct{}
	stepDown   chan struct{}
	commitment *commitment
}

func (r *Raft) Shutdown() {
	close(r.shutDownCh)
	r.setState(Shutdown)
}

func (r *Raft) Submit(command interface{}) bool {
	r.leaderLock.Lock()
	defer r.leaderLock.Unlock()

	//r.slog("Submit received by %v: %v", r.getState(), command)
	if r.getState() == Leader {
		r.log = append(r.log, CommitEntry{
			Command: command,
			Term:    r.getCurrentTerm(),
		})
		return true
	}
	return false
}
