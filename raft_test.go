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

	sleepMs(350)
	originLeader, originTerm := cluster.CheckSingleLeader()

	cluster.DisconnectPeer(originLeader)
	sleepMs(5000)

	newLeader, newTerm := cluster.CheckSingleLeader()

	// new leader should not be the same as origin leader.
	require.NotEqual(t, originLeader, newLeader)

	// new term should be greater than origin terms.
	require.Greater(t, newTerm, originTerm)
}
