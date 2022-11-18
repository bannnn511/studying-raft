package studying_raft

import (
	"testing"
)

func TestRaft_Basic(t *testing.T) {
	clusters := NewCluster(t, 3)
	defer clusters.Shutdown()
	clusters.CheckSingleLeader()
}
