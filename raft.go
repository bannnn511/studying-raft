package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
)

type CMState int

// CommitEntry is the data reported by raft to the commit channel.
// Each entry notifies the client that consensus was reached on a command and
// it can be applied to client's state machine.
type CommitEntry struct {
	// Command is the client command being committed.
	Command interface{}

	// Index is the log index at which the client command is committed.
	Index int

	// Term is the Raft term at which the client command is committed
	Term int
}

const DebugCM = 1

const (
	StateFollower CMState = iota
	StateCandidate
	StateLeader
	StateDead
)

// ConsensusModule (CM) implements a single node of Raft consensus.
type ConsensusModule struct {
	// id is the Server ID of this CM.
	id int

	// mu is mutex lock to protect concurrent access to a CM.
	mu sync.Mutex

	// peerIds is ID list of all servers in the cluster.
	peerIds []int

	// server is the server contains this CM to issues RPC calls to peers.
	server *Server

	// Persistent state on all servers.
	// updated on stable storage before responding to RPCs.
	currentTerm int
	votedFor    int
	logs        []CommitEntry

	// Volatile state on all servers.
	// commitIndex is index of the highest log entry known to be committed.
	commitIndex int

	// lastApplied is index of the highest log entry applied to state machine.
	lastApplied int

	// Volatile state on leader.
	// for each server, index of next log entry to send to that server.
	nextIndex map[int]int
	// for each server,index of the highest log entry known to be replicated on that server.
	matchIndex map[int]int

	state              CMState
	electionResetEvent time.Time

	// commitChan is the channel where this CM is going to report committed log
	// entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on commitChan.
	newCommitReadyChan chan struct{}

	logger *zap.Logger
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine. commitChan is going to be used by the
// CM to send log entries that have been committed by the Raft cluster.
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.state = StateFollower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	go func() {
		// The CM is dormant until ready is signaled; then, it starts a countdown
		// for leader election.
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
	return cm
}

type RequestVoteArgs struct {
	// Term is candidate's term.
	Term int

	// CandidateId is candidate requesting vote.
	CandidateId int

	// LastLogIndex is index of last log index.
	LastLogIndex int

	// LastLogTerm is term of candidate's last log entry.
	LastLogTerm int
}

type RequestVoteReply struct {
	// Term is current term for candidate to update itself.
	Term int

	// VoteGranted is true means candidate received vote.
	VoteGranted bool
}

// RequestVote invoked by candidates to gather votes.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == StateDead {
		return nil
	}
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// check that request lastLogIndex and lastLogTerm must be higher than cm's last log index and term
	if args.Term == cm.currentTerm &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogIndex && args.LastLogIndex > lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)

	return nil
}

type AppendEntriesArgs struct {
	// Term is leader term.
	Term int

	// LeaderId for follower to redirect client.
	LeaderId int

	// PrevLogIndex is index of log entry immediately preceding the new ones in its log.
	PrevLogIndex int

	// PrevLogTerm is term of prevLogIndex.
	PrevLogTerm int

	// LeaderCommit is leader's commit index.
	LeaderCommit int

	// log entries to store.
	Entries []CommitEntry
}

type AppendEntriesReply struct {
	// current term for leader to update itself.
	Term int

	// True if follower contained entry matching PrevLogIndex and PrevLogTerm.
	Success bool
}

// AppendEntries invoked by leader to replicated log entries, also used as heartbeat.
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == StateDead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
		return nil
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != StateFollower {
			cm.becomeFollower(args.Term)
		}

		// START: find which entries to apply to the log.
		// if the follower does not find an entry in its log with same index and the same term
		// 	-> refuses the new entries.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.logs) && args.PrevLogTerm == cm.logs[args.PrevLogIndex].Term) {
			reply.Success = true
			cm.electionResetEvent = time.Now()

			// logInsertIndex should point at the end of follower logs or at the term mismatch with leader entry.
			logInsertIndex := args.PrevLogIndex + 1
			// newEntriesIndex should point at the start of
			newEntriesIndex := 0
			for {
				if args.PrevLogIndex > len(cm.logs) || newEntriesIndex >= len(args.Entries) {
					break
				}

				// compare term between current log index with new entries term.
				if cm.logs[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.logs = append(cm.logs[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			}

			// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > cm.commitIndex {
				if args.LeaderCommit > len(cm.logs)-1 {
					cm.commitIndex = len(cm.logs) - 1
				} else {
					cm.commitIndex = args.LeaderCommit
				}
			}
		}
		// END: find which entries to apply to the log.
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)

	return nil
}

// Submit appends command into the log with current term.
func (cm *ConsensusModule) Submit(command interface{}) bool {
	return false
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		cm.mu.Lock()

		if cm.state != StateCandidate && cm.state != StateFollower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elapse := time.Since(cm.electionResetEvent); elapse >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())

	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

/*
Becoming a candidate
1. Switch the state to candidate and increment the term
2. Send RV RPC to all peers, asking them to vote for us in this election.
3. Wait for replies to these RPCs and count if we got enough votes to become a leader.
*/
func (cm *ConsensusModule) startElection() {
	cm.state = StateCandidate
	cm.currentTerm++
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog(
		"becomes Candidate (currentTerm=%d); log=%v",
		savedCurrentTerm,
		cm.logs)

	votesReceived := 1
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			/*
				TODO:
				lock to get last log index and last log term
				for sending to requestVote RPC
			*/
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}

			reply := &RequestVoteReply{}

			err := cm.server.Call(
				peerId,
				"ConsensusModule.RequestVote",
				args,
				reply)
			if err != nil {
				fmt.Println(err)
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()
			cm.dlog("sending RequestVote to %d: %+v", peerId, args)

			if cm.state != StateCandidate {
				cm.dlog("while waiting for reply, state = %v", cm.state)
				return
			}

			if reply.Term > savedCurrentTerm {
				cm.dlog("term out of date in RequestVoteReply")
				cm.becomeFollower(reply.Term)
				return
			}

			if reply.Term == savedCurrentTerm {
				if reply.VoteGranted {
					votesReceived++
					if 2*votesReceived+1 > len(cm.peerIds)+1 {
						cm.dlog(
							"wins election with %d votes",
							votesReceived,
						)
						cm.startLeader()
						return
					}
				}
			}

		}(peerId)
	}

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.logs)
	cm.state = StateLeader

	go func() {
		timer := time.NewTicker(50 * time.Millisecond)
		defer timer.Stop()

		for {
			cm.leaderSendHeartBeats()
			<-timer.C

			cm.mu.Lock()
			if cm.state != StateLeader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}

	}()
}

func (cm *ConsensusModule) leaderSendHeartBeats() {
	cm.mu.Lock()
	if cm.state != StateLeader {
		cm.mu.Unlock()
		return
	}
	savedTerm := cm.currentTerm
	cm.mu.Unlock()

	// START: Send heartbeats to peers.
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			// START: Get AppendEntries RPC args.
			cm.mu.Lock()
			peerNextIndex := cm.nextIndex[peerId]
			prevLogIndex := peerNextIndex - 1
			prevLogTerm := -1
			if prevLogIndex > 0 {
				prevLogTerm = cm.logs[prevLogIndex].Term
			}

			entries := cm.logs[peerNextIndex:]
			cm.mu.Unlock()

			args := &AppendEntriesArgs{
				Term:         savedTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: cm.commitIndex,
				Entries:      entries,
			}
			// END: Get AppendEntries RPC args.
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)

			reply := &AppendEntriesReply{}
			err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, reply)
			if err != nil {
				return
			}

			if reply.Term > savedTerm {
				cm.dlog("term out of date in heartbeat reply")
				cm.becomeFollower(reply.Term)
			}

			// START: check if entry is ready to be replicated.
			if savedTerm == reply.Term {
				if reply.Success {
					cm.nextIndex[peerId] = peerNextIndex + len(entries)
					cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

					savedCommitIndex := cm.commitIndex
					matchCount := 0
					for i := savedCommitIndex + 1; i < len(cm.logs); i++ {
						if cm.matchIndex[peerId] > i {
							matchCount++
						}
						if 2*matchCount+1 > len(cm.peerIds)+1 {
							cm.commitIndex = i
						}
					}
					if cm.commitIndex > savedCommitIndex {
						cm.newCommitReadyChan <- struct{}{}
					} else {
						peerNextIndex--
					}
				}
			}
			// END: check if entry is ready to be replicated.

		}(peerId)
	}
	// END: Send heartbeats to peers.
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.logs)
	cm.state = StateFollower
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	cm.currentTerm = term

	go cm.runElectionTimer()
}

// Stop stops this CM, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) for all goroutines to
// exit.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = StateDead
	cm.dlog("become dead")
}

// dlog logs a debugging message if DebugCM > 0.
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == StateLeader
}

// commitChanSender waits for signal from cm.newCommitReadyChan
// then it will send entry to cm.commitChan.
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		if cm.commitIndex > cm.lastApplied {
			entries := cm.logs[cm.lastApplied+1 : cm.commitIndex+1]
			for _, entry := range entries {
				cm.commitChan <- CommitEntry{
					Command: entry.Command,
					Index:   entry.Index,
					Term:    entry.Term,
				}
			}
		}
	}
}

// lastLogIndexAndTerm returns index of the last log entry and its term.
// if the cm's logs is empty then return -1,-1
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.logs) <= 0 {
		return -1, -1
	}

	lastIndex := len(cm.logs) - 1

	return lastIndex, cm.logs[lastIndex].Term
}
