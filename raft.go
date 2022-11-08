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
type LogEntry struct {
	Command interface{}
	Term    int
}

const DebugCM = 1

const (
	StateFollower CMState = iota
	StateCandidate
	StateLeader
	StateDead
)

type ConsensusModule struct {
	id                 int
	mu                 sync.Mutex
	currentTerm        int
	state              CMState
	electionResetEvent time.Time
	votedFor           int
	logs               []LogEntry
	peerIds            []int
	server             *Server

	logger *zap.Logger
}

func NewConsensusModule(serverId int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = serverId
	cm.state = StateFollower
	cm.votedFor = -1
	cm.peerIds = peerIds
	cm.server = server
	cm.logger = zap.NewExample()

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
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
			fmt.Printf("candidate %v time %v\n", cm.id, cm.electionResetEvent)
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
	fmt.Printf("candidate: %v, currentTerm: %v\n", cm.id, cm.currentTerm)
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.logs)

	votesReceived := 1
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}

			reply := &RequestVoteReply{}

			err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, reply)
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
						cm.dlog("wins election with %d votes", votesReceived)
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

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := &AppendEntriesArgs{
				Term:     savedTerm,
				LeaderId: cm.id,
			}
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)

			reply := &AppendEntriesReply{}
			err := cm.server.Call(cm.id, "ConsensusModule.AppendEntries", args, reply)
			if err != nil {
				return
			}
			if reply.Term > savedTerm {
				cm.dlog("term out of date in heartbeat reply")
				cm.becomeFollower(reply.Term)
			}
		}(peerId)
	}

}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("----becomes Follower with term=%d; log=%v", term, cm.logs)
	cm.state = StateFollower
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	cm.currentTerm = term

	go cm.runElectionTimer()
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == StateDead {
		return nil
	}
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if args.Term == cm.currentTerm && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

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
		reply.Success = true
		cm.electionResetEvent = time.Now()
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)

	return nil
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
