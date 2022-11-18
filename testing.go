package studying_raft

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

type Cluster struct {
	mu sync.Mutex

	// cluster is a list of all the raft servers participating in a cluster.
	cluster []*Server

	commitChans []chan CommitEntry

	commits [][]CommitEntry

	connected []bool

	n int
	t *testing.T
}

func NewCluster(t *testing.T, n int) *Cluster {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	ready := make(chan struct{})
	commitChan := make(chan CommitEntry)

	// Create all Servers in this cluster, assign ids and peer ids.
	for i := 0; i < n; i++ {
		peerIds := make([]string, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, strconv.Itoa(p))
			}
		}

		ns[i] = NewServer(i, peerIds, ready, commitChan)
		ns[i].Serve()
	}

	// Connect all peers to each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(strconv.Itoa(j), ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	return &Cluster{
		cluster:   ns,
		connected: connected,
		n:         n,
		t:         t,
	}
}

// Shutdown shuts down all the servers in the harness and waits for them to
// stop running.
func (h *Cluster) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
}

// DisconnectPeer disconnects a server from all other servers in the cluster.
func (h *Cluster) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(strconv.Itoa(id))
		}
	}
	h.connected[id] = false
}

func (h *Cluster) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			if err := h.cluster[id].ConnectToPeer(strconv.Itoa(j), h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(strconv.Itoa(id), h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

func (h *Cluster) CheckSingleLeader() (int, int) {
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].raft.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = int(term)
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}
