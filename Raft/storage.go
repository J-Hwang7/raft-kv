package raft

import (
	"encoding/json"
	"fmt"
	"os"
)

// persistentState is exactly the subset Raft requires to be durable. Nothing
// else belongs here — commitIndex/lastApplied/nextIndex/matchIndex/kv are all
// volatile and rebuilt after a restart.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

func (rn *RaftNode) stateFile() string {
	return fmt.Sprintf("raft-state-%d.json", rn.id) // per-node, so 3 local nodes don't clobber each other
}

// persist writes durable state to disk. Call while holding rn.mu.
//
// It writes a temp file then renames it into place. os.Rename is atomic on the
// OS (including Windows), so a crash mid-write can never leave a half-written,
// corrupt state file: you either keep the intact old file or get the complete
// new one — never a torn one. That atomicity is the whole reason for the dance.
func (rn *RaftNode) persist() {
	ps := persistentState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
		Log:         rn.log,
	}
	data, err := json.Marshal(ps)
	if err != nil {
		rn.logf("persist: marshal failed: %v", err)
		return
	}
	tmp := rn.stateFile() + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		rn.logf("persist: write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, rn.stateFile()); err != nil {
		rn.logf("persist: rename failed: %v", err)
	}
}

// loadState restores durable state if a file exists. Call once at startup,
// before the node serves anything. No file = first boot = keep fresh defaults.
func (rn *RaftNode) loadState() {
	data, err := os.ReadFile(rn.stateFile())
	if err != nil {
		if os.IsNotExist(err) {
			rn.logf("no persisted state — starting fresh")
			return
		}
		rn.logf("loadState: read failed: %v", err)
		return
	}
	var ps persistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		rn.logf("loadState: unmarshal failed: %v", err)
		return
	}
	rn.currentTerm = ps.CurrentTerm
	rn.votedFor = ps.VotedFor
	rn.log = ps.Log
	rn.logf("restored state: term %d, votedFor %d, log length %d", rn.currentTerm, rn.votedFor, rn.lastLogIndex())
}