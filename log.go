package studying_raft

// CommitEntry is the data reported by raft to the commit channel.
// Each entry notifies the client that consensus was reached on a command and
// it can be applied to client's state machine.
type CommitEntry struct {
	// Command is the client command being committed.
	Command interface{}

	// Index is the log index at which the client command is committed.
	Index uint64

	// Term is the Raft term at which the client command is committed
	Term uint64
}

type Log []CommitEntry

func (l Log) deleteRange(min, max int) Log {
	newLogs := make([]CommitEntry, 0, len(l)-(max-min)-1)
	for i := 0; i < min; i++ {
		newLogs = append(newLogs, l[i])
	}
	for i := max + 1; i < len(l); i++ {
		newLogs = append(newLogs, l[i])
	}

	return newLogs
}
