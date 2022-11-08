package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type CMState int

const (
	StateFollower CMState = iota
	StateCandidate
	StateLeader
	StateDead
)

type ConsensusModule struct {
	id                 int
	mu                 *sync.Mutex
	currentTerm        int
	state              CMState
	electionResetEvent time.Time
	votedFor           int
	peerIds            []int
	server             *Server
}

func NewConsensusModule(serverId int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := &ConsensusModule{
		id:       serverId,
		state:    StateFollower,
		votedFor: -1,
		peerIds:  peerIds,
		server:   server,
	}

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

/*
1. selecting a pseudo-random election timeout.
calling cm.electionTimeout
The range is from 150-300ms

lock the struct
get termStarted from cm's current term

2.main loop
run a ticker for 10ms.

	wait for ticker channel
	lock

	if cm.state != candidate && cm.state != follower
	unlock
	return

	if termStarted != currentTerm
	-> term changed
	unlock
	return

	Start an election if we haven't heard from a leader or
	haven't voted for someone during the duration of election timeout.
	if elapse >= timeoutDuration
	cm.startElection()
	unlock
	return

	unlock
*/
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		cm.mu.Lock()

		if cm.state != StateCandidate && cm.state != StateFollower {
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.mu.Unlock()
			return
		}

		if elapse := time.Since(cm.electionResetEvent); elapse < timeoutDuration {
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

	voteReceived := 1
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				term:        savedCurrentTerm,
				candidateId: cm.id,
			}
			reply := &RequestVoteReply{}
			err := cm.server.Call(cm.id, "ConsensusModule.RequestVote", args, reply)
			if err != nil {
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()
			if cm.state != StateCandidate {
				return
			}

			if reply.term > savedCurrentTerm {
				cm.becomeFollower(reply.term)
				return
			}

			if reply.term == savedCurrentTerm {
				if reply.voteGranted {
					voteReceived++
					if 2*voteReceived+1 > len(cm.peerIds)+1 {
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
	cm.mu.Lock()
	cm.state = StateLeader
	cm.mu.Unlock()

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
				term:     savedTerm,
				leaderId: cm.id,
			}
			reply := &AppendEntriesReply{}
			err := cm.server.Call(cm.id, "ConsensusModule.AppendEntries", args, reply)
			if err != nil {
				return
			}
			if reply.term > savedTerm {
				cm.becomeFollower(reply.term)
			}
		}(peerId)
	}

}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.mu.Lock()
	cm.state = StateFollower
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	cm.currentTerm = term
	cm.mu.Unlock()

	go cm.runElectionTimer()
}

type RequestVoteArgs struct {
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

type RequestVoteReply struct {
	term        int
	voteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == StateDead {
		return nil
	}

	if args.term > cm.currentTerm {
		return errors.New(fmt.Sprintf("client %v term %v out of dated", cm.currentTerm, cm.currentTerm))
	}

	if args.term == cm.currentTerm && (cm.votedFor == -1 || cm.votedFor == args.candidateId) {
		reply.voteGranted = true
		cm.votedFor = args.candidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.voteGranted = false
	}
	reply.term = cm.currentTerm

	return nil
}

type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []interface{}
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == StateDead {
		return nil
	}

	if args.term > cm.currentTerm {
		cm.becomeFollower(args.term)
		return nil
	}

	reply.success = false
	if args.term == cm.currentTerm {
		if cm.state != StateFollower {
			cm.becomeFollower(args.term)
		}
		reply.success = true
		cm.electionResetEvent = time.Now()
	}

	reply.term = cm.currentTerm

	return nil
}

// Stop stops this CM, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) for all goroutines to
// exit.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = StateDead
}
