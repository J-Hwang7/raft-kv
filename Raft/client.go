package raft

// Submit is called by the leader's HTTP handler to propose a new command.
// It appends to the leader's own log; the replication loop does the rest.
// Returns (index, isLeader): the log index the command landed at, and whether
// this node was actually the leader (clients must retry elsewhere if not).
func (rn *RaftNode) Submit(cmd Command) (int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

    if rn.state != Leader {
        return -1, false
    }

	// Appends client entry into leader's logs
    rn.log = append(rn.log, LogEntry{Term: rn.currentTerm, Command: cmd})

	// Return the new entry's index and true.
	rn.persist()
    index := rn.lastLogIndex()
	return index, true
}

// Get reads a key directly from the state machine (leader-only, uncommitted-safe
// enough for our purposes). Returns (value, ok, isLeader).
func (rn *RaftNode) Get(key string) (string, bool, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return "", false, false
	}
	val, ok := rn.kv[key]
	return val, ok, true
}