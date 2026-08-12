package raft

import (
	"net"
	"net/rpc"
	"time"
)

type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int        // index of the entry immediately before Entries
	PrevLogTerm  int        // term of that preceding entry
	Entries      []LogEntry // new entries (empty for a pure heartbeat)
	LeaderCommit int        // leader's commitIndex, so followers can advance theirs
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

// RequestVote is what a campaigning peer calls ON this node.
// YOU write the vote-granting logic here next step. For now it refuses
// everything, so the cluster compiles and boots.
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.logf("got RequestVote from node %d (their term %d)", args.CandidateID, args.Term)


	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = -1
		rn.state = Follower
	}

	if rn.votedFor == -1 || rn.votedFor == args.CandidateID {
		rn.votedFor = args.CandidateID
		rn.lastHeard = time.Now() 
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	rn.persist()
	reply.Term = rn.currentTerm
	return nil
}

// sendRequestVote dials a peer and asks for its vote. (nil, false) means
// the peer was unreachable — in Raft that's normal (it might be dead) and
// just counts as "no vote."
func (rn *RaftNode) sendRequestVote(peerID int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	client, err := rpc.Dial("tcp", rn.peers[peerID])
	if err != nil {
		return nil, false
	}
	defer client.Close()

	reply := &RequestVoteReply{}
	if err := client.Call("Raft.RequestVote", args, reply); err != nil {
		return nil, false
	}
	return reply, true
}

func (rn *RaftNode) sendAppendEntries(peerID int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	client, err := rpc.Dial("tcp", rn.peers[peerID])
	if err != nil {
		return nil, false
	}
	defer client.Close()

	reply := &AppendEntriesReply{}
	if err := client.Call("Raft.AppendEntries", args, reply); err != nil {
		return nil, false
	}
	return reply, true
}

type rpcProxy struct{ rn *RaftNode }

func (p *rpcProxy) RequestVote(a *RequestVoteArgs, r *RequestVoteReply) error {
	return p.rn.RequestVote(a, r)
}

func (p *rpcProxy) AppendEntries(a *AppendEntriesArgs, r *AppendEntriesReply) error {
	return p.rn.AppendEntries(a, r)
}

func (rn *RaftNode) StartServer() error {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &rpcProxy{rn}); err != nil {
		return err
	}
	l, err := net.Listen("tcp", rn.peers[rn.id])
	if err != nil {
		return err
	}
	rn.logf("listening on %s", rn.peers[rn.id])
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()
	return nil
}

func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	persistNeeded := false // set true whenever currentTerm or log changes

	// A. Stale leader — reject. (no durable change, so no persist)
	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.Success = false
		return nil
	}

	// B. Leader is ahead — adopt their term.
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = -1
		persistNeeded = true // term changed
	}

	// C. Valid leader — become follower, reset clock.
	rn.state = Follower
	rn.lastHeard = time.Now()

	// 1 — CONSISTENCY CHECK.
	if rn.lastLogIndex() < args.PrevLogIndex || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if persistNeeded {
			rn.persist() // a term adopted in B must survive even on rejection
		}
		reply.Success = false
		reply.Term = rn.currentTerm
		return nil
	}

	// 2 — APPEND, resolving conflicts.
	for i := range args.Entries {
		targetIndex := args.PrevLogIndex + 1 + i

		if targetIndex <= rn.lastLogIndex() {
			if rn.log[targetIndex].Term != args.Entries[i].Term {
				rn.log = rn.log[:targetIndex]
				rn.log = append(rn.log, args.Entries[i:]...)
				persistNeeded = true // log changed
				break
			}
		} else {
			rn.log = append(rn.log, args.Entries[i:]...)
			persistNeeded = true // log changed
			break
		}
	}

	// 3 — ADVANCE COMMIT.
	if args.LeaderCommit > rn.commitIndex {
		lastIdx := rn.lastLogIndex()
		if args.LeaderCommit < lastIdx {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastIdx
		}
		rn.applyCommitted()
	}

	if persistNeeded {
		rn.persist()
	}

	reply.Success = true
	reply.Term = rn.currentTerm
	return nil
}

// heartbeatLoop runs while this node is Leader, pinging peers so their
// election timeouts keep resetting and no one challenges leadership.
func (rn *RaftNode) heartbeatLoop(termWhenElected int) {
	for {
		rn.mu.Lock()

		// Stop this loop if we're no longer the rightful leader.
		if rn.state != Leader || rn.currentTerm != termWhenElected {
			rn.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{
			Term:     rn.currentTerm,
			LeaderID: rn.id,
		}
		rn.mu.Unlock()

		// Send heartbeat to every peer
		for i := range rn.peers {
			if i == rn.id {
				continue
			}

			rn.sendAppendEntries(i, &args)
		}

		time.Sleep(50 * time.Millisecond) //heartbeat interval
	}
}
