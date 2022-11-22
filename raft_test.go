package studying_raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRaft_Basic(t *testing.T) {
	clusters := NewCluster(t, 3)
	defer clusters.Shutdown()
	sleepMs(350)
	clusters.CheckSingleLeader()
}

func TestRaft_LeaderDisconnect(t *testing.T) {
	cluster := NewCluster(t, 3)
	defer cluster.Shutdown()

	originLeader, originTerm := cluster.CheckSingleLeader()

	cluster.DisconnectPeer(originLeader)
	sleepMs(300)

	newLeader, newTerm := cluster.CheckSingleLeader()

	// new leader should not be the same as origin leader.
	require.NotEqual(t, originLeader, newLeader)

	// new term should be greater than origin terms.
	require.Greater(t, newTerm, originTerm)
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	cluster := NewCluster(t, 3)
	defer cluster.Shutdown()

	originLeader, _ := cluster.CheckSingleLeader()

	cluster.DisconnectPeer(originLeader)
	otherId := (originLeader + 1) % 3
	cluster.DisconnectPeer(otherId)

	// No quorum
	sleepMs(450)
	cluster.CheckNoLeader()

	// Reconnect one other server; now we'll have quorum
	cluster.ReconnectPeer(otherId)
	cluster.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	cluster := NewCluster(t, 3)
	defer cluster.Shutdown()

	sleepMs(100)
	for i := 0; i < 3; i++ {
		cluster.DisconnectPeer(i)
	}
	sleepMs(450)
	cluster.CheckNoLeader()

	// Reconnect all servers. A leader will be found
	for i := 0; i < 3; i++ {
		cluster.ReconnectPeer(i)
	}
	cluster.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	cluster := NewCluster(t, 3)
	defer cluster.Shutdown()

	originLeader, _ := cluster.CheckSingleLeader()

	cluster.DisconnectPeer(originLeader)

	sleepMs(350)
	newLeaderId, newLeaderTerm := cluster.CheckSingleLeader()

	cluster.ReconnectPeer(originLeader)
	sleepMs(150)

	againLeader, againTerm := cluster.CheckSingleLeader()

	require.Equal(t, newLeaderId, againLeader)
	require.Equal(t, newLeaderTerm, againTerm)
}

func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	cluster := NewCluster(t, 3)
	defer cluster.Shutdown()

	originLeader, _ := cluster.CheckSingleLeader()
	cluster.DisconnectPeer(originLeader)
	sleepMs(150)

	newLeaderId, newLeaderTerm := cluster.CheckSingleLeader()
	cluster.ReconnectPeer(originLeader)
	sleepMs(150)

	againLeader, againTerm := cluster.CheckSingleLeader()
	require.Equal(t, newLeaderId, againLeader)
	require.Equal(t, newLeaderTerm, againTerm)
}

func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	c := NewCluster(t, 3)
	defer c.Shutdown()

	origLeaderId, origTerm := c.CheckSingleLeader()

	otherId := (origLeaderId + 1) % 3
	c.DisconnectPeer(otherId)
	sleepMs(650)
	c.ReconnectPeer(otherId)
	sleepMs(150)

	_, newTerm := c.CheckSingleLeader()
	require.GreaterOrEqual(t, newTerm, origTerm)
}

func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	c := NewCluster(t, 3)
	defer c.Shutdown()

	for cycle := 0; cycle < 5; cycle++ {
		leaderId, _ := c.CheckSingleLeader()

		c.DisconnectPeer(leaderId)
		otherId := (leaderId + 1) % 3
		c.DisconnectPeer(otherId)
		sleepMs(310)
		c.CheckNoLeader()

		// Reconnect both.
		c.ReconnectPeer(otherId)
		c.ReconnectPeer(leaderId)

		// Give it time to settle
		sleepMs(250)
	}
}
