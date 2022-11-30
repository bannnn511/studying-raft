package studying_raft

import (
	"sync"
	"sync/atomic"
)

type RaftState uint32

const (
	// Follower is the initial state of a Raft node.
	Follower RaftState = iota

	// Candidate is one of the valid state of a Raft node.
	Candidate

	// Leader is one of the valid state of a Raft node.
	Leader

	// Shutdown is the terminal state of a Raft node.
	Shutdown
)

func (rs RaftState) String() string {
	switch rs {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

type raftState struct {
	// Persistent state on all servers.
	// updated on stable storage before responding to RPCs.
	currentTerm uint64

	// commitIndex is index of the highest log entry known to be committed.
	commitIndex uint64

	// lastApplied is index of the highest log entry applied to state machine.
	lastApplied uint64

	// mu is mutex lock to protect concurrent access to a CM.
	mu sync.Mutex

	// index and term of candidate’s last log entry
	lastLogIndex uint64
	lastLogTerm  uint64

	// Volatile state on leader.
	// for each server, index of next log entry to send to that server.
	nextIndex map[int]int
	// for each server,index of the highest log entry known to be replicated on that server.
	matchIndex map[int]int

	// The current state
	state RaftState

	// routinesGroup tracks running routines.
	routinesGroup sync.WaitGroup
}

func (r *raftState) getState() RaftState {
	return RaftState(atomic.LoadUint32((*uint32)(&r.state)))
}

func (r *raftState) setState(s RaftState) {
	atomic.StoreUint32((*uint32)(&r.state), uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&r.commitIndex, index)
}

func (r *raftState) getLastEntry() (uint64, uint64) {
	r.mu.Lock()
	lastLogIndex := r.lastLogIndex
	lastLogTerm := r.lastLogTerm
	r.mu.Unlock()

	return lastLogIndex, lastLogTerm
}

func (r *raftState) setLastEntry(entry CommitEntry) {
	r.mu.Lock()
	r.lastLogIndex = entry.Index
	r.lastLogTerm = entry.Term
	r.mu.Unlock()
}

func (r *raftState) getLastIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastLogIndex
}

func (r *raftState) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

func (r *raftState) WaitShutdown() {
	r.routinesGroup.Wait()
}
