package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Command is one operation the cluster agrees on and applies to the KV store.
// PUT and DELETE mutate state and go through the log; GET is a read served
// directly by the leader and is never logged.
type Command struct {
	Op    string // "PUT" or "DELETE"
	Key   string
	Value string
}

// LogEntry is one slot in the replicated log: a command plus the term the
// leader created it in. The (index, term) pair identifies it clusterwide.
type LogEntry struct {
	Term    int
	Command Command
}

type RaftNode struct {
	mu sync.Mutex

	id    int
	peers []string

	currentTerm int
	votedFor    int
	log         []LogEntry // index 0 is a sentinel; real entries start at 1

	state           State
	lastHeard       time.Time
	electionTimeout time.Duration
	commitIndex     int // highest index known committed
	lastApplied     int // highest index applied to kv

	// Leader-only, reinitialised after each election win:
	nextIndex  []int // per follower: next log index to send
	matchIndex []int // per follower: highest index known replicated there

	kv map[string]string 
}

func NewRaftNode(id int, peers []string) *RaftNode {
	rn := &RaftNode{
		id:          id,
		peers:       peers,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}}, // sentinel at index 0
		state:       Follower,
		lastHeard:   time.Now(),
		commitIndex: 0,
		lastApplied: 0,
		kv:          make(map[string]string),
	}
	rn.loadState()
	rn.resetElectionTimeout()
	return rn
}

func (rn *RaftNode) resetElectionTimeout() {
	rn.electionTimeout = time.Duration(400+rand.Intn(300)) * time.Millisecond
}

// lastLogIndex / lastLogTerm, calls while holding rn.mu.
func (rn *RaftNode) lastLogIndex() int {
	return len(rn.log) - 1
}

func (rn *RaftNode) lastLogTerm() int {
	return rn.log[len(rn.log)-1].Term
}

func (rn *RaftNode) logf(format string, args ...any) {
	prefix := fmt.Sprintf("[node %d | term %d | %-9s] ", rn.id, rn.currentTerm, rn.state)
	log.Printf(prefix+format, args...)
}

// applyCommitted aplies entries inputted between lastApplied and commitIndex
func (rn *RaftNode) applyCommitted() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		switch entry.Command.Op {
		case "PUT":
			rn.kv[entry.Command.Key] = entry.Command.Value
			rn.logf("applied PUT %s=%s at index %d", entry.Command.Key, entry.Command.Value, rn.lastApplied)
		case "DELETE":
			delete(rn.kv, entry.Command.Key)
			rn.logf("applied DELETE %s at index %d", entry.Command.Key, rn.lastApplied)
		}
	}
} 