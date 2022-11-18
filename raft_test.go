package studying_raft

import (
	"testing"
	"time"
)

func TestRaft_Basic(t *testing.T) {
	clusters := NewCluster(t, 3)
	defer clusters.Shutdown()
	time.Sleep(3 * time.Second)
	clusters.CheckSingleLeader()
}
