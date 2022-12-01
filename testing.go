package studying_raft

import (
	"fmt"
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
			//if p != i {
			peerIds = append(peerIds, strconv.Itoa(p))
			//}
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
func (c *Cluster) Shutdown() {
	for i := 0; i < c.n; i++ {
		c.cluster[i].DisconnectAll()
		c.connected[i] = false
	}
	for i := 0; i < c.n; i++ {
		c.cluster[i].Shutdown()
	}
}

// DisconnectPeer disconnects a server from all other servers in the cluster.
func (c *Cluster) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	c.cluster[id].DisconnectAll()
	for j := 0; j < c.n; j++ {
		if j != id {
			c.cluster[j].DisconnectPeer(strconv.Itoa(id))
		}
	}
	c.connected[id] = false
}

func (c *Cluster) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < c.n; j++ {
		if j != id {
			if err := c.cluster[id].ConnectToPeer(strconv.Itoa(j), c.cluster[j].GetListenAddr()); err != nil {
				c.t.Fatal(err)
			}
			if err := c.cluster[j].ConnectToPeer(strconv.Itoa(id), c.cluster[id].GetListenAddr()); err != nil {
				c.t.Fatal(err)
			}
		}
	}
	c.connected[id] = true
}

func (c *Cluster) CheckSingleLeader() (int, int) {
	c.t.Helper()
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < c.n; i++ {
			if c.connected[i] {
				_, term, isLeader := c.cluster[i].raft.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = int(term)
					} else {
						c.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	c.t.Fatalf("leader not found")
	return -1, -1
}

func (c *Cluster) CheckNoLeader() {
	c.t.Helper()
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			_, _, isLeader := c.cluster[i].raft.Report()
			if isLeader {
				log.Fatal(fmt.Sprintf("server %d leader, want none", i))
			}
		}
	}
}

// SubmitToServer submits the command to serverId.
func (c *Cluster) SubmitToServer(serverId int, cmd interface{}) bool {
	return c.cluster[serverId].raft.Submit(cmd)
}

func (c *Cluster) CheckCommitted(cmd int) (nc int, index int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find the length of the commits slice for connected servers.
	commitsLen := -1
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			if commitsLen >= 0 {
				// If this was set already, expect the new length to be the same.
				if len(c.commits[i]) != commitsLen {
					c.t.Fatalf("commits[%d] = %d, commitsLen = %d", i, c.commits[i], commitsLen)
				}
			} else {
				commitsLen = len(c.commits[i])
			}
		}
	}

	// Check consistency of commits from the start and to the command we're asked
	// about. This loop will return once a command=cmd is found.
	for h := 0; h < commitsLen; h++ {
		cmdAtC := -1
		for i := 0; i < c.n; i++ {
			if c.connected[i] {
				cmdOfN := c.commits[i][h].Command.(int)
				if cmdAtC >= 0 {
					if cmdOfN != cmdAtC {
						c.t.Errorf("got %d, want %d at c.commits[%d][%d]", cmdOfN, cmdAtC, i, h)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		if cmdAtC == cmd {
			// Check consistency of Index.
			index := -1
			nc := 0
			for i := 0; i < c.n; i++ {
				if c.connected[i] {
					if index >= 0 && int(c.commits[i][h].Index) != index {
						c.t.Errorf("got Index=%d, want %d at c.commits[%d][%d]", c.commits[i][h].Index, index, i, h)
					} else {
						index = int(c.commits[i][h].Index)
					}
					nc++
				}
			}
			return nc, index
		}
	}

	// If there's no early return, we haven't found the command we were looking
	// for.
	c.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

func (c *Cluster) CheckCommittedN(cmd int, n int) {
	nc, _ := c.CheckCommitted(cmd)
	if nc != n {
		c.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

// CheckNotCommitted verifies that no command equal to cmd has been committed
// by any of the active servers yet.
func (c *Cluster) CheckNotCommitted(cmd int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			for j := 0; i < len(c.commits[i]); j++ {
				gotCmd := c.commits[i][j].Command.(int)
				if gotCmd == cmd {
					c.t.Errorf("found %d at commits[%d][%d], expected none", cmd, i, j)
				}
			}
		}
	}
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
