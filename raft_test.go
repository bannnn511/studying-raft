package studying_raft

import (
	"testing"

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
