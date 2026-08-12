package raft

import (
	"time"
)

//Checks whether the follower nodes has gone too long from hearing from the leader node
//If it has been to long since last communication, starts an election
func(rn *RaftNode) Run() {
	for {
		time.Sleep(10 * time.Millisecond) 

		rn.mu.Lock()

		// If you're the leader, don't need to run election timeouts
		if rn.state == Leader {
			rn.mu.Unlock()
			continue
		}

		//If election timeout has not elapsed, unlocks
		if time.Since(rn.lastHeard) < rn.electionTimeout {
			rn.mu.Unlock()
			continue
		}

		//If election timeout has elapsed, assumes leader is dead and starts election
		rn.mu.Unlock()
		rn.startElection()
	}
}	

// startElection transitions this node to Candidate and campaigns.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	//Becoming a Candidate
	rn.state = Candidate 
	rn.currentTerm++ //New election cycle
	rn.votedFor = rn.id //Vote for yourself
	rn.resetElectionTimeout ()
	rn.lastHeard = time.Now() //Prevents re-timmeout since node just acted
	termForThisElection := rn.currentTerm //Current Term
	args := RequestVoteArgs {
		Term:        termForThisElection,
		CandidateID: rn.id,
	}
	rn.logf("Election Timeout — Starting Election")//Creates log to display the election happening

	rn.persist()
	rn.mu.Unlock()

	//Counting votes
	votes := 1 //Starts with node's own vote
	for i := range rn.peers{ //Loops through each node that is not yourself
		if i == rn.id {
			continue
		}

		reply, ok := rn.sendRequestVote(i, &args)
		if !ok {
			continue 
		}

		rn.mu.Lock()
		//If another node;s term is newer step down
		if reply.Term > rn.currentTerm {
			rn.currentTerm = reply.Term
			rn.votedFor = -1
			rn.state = Follower
			rn.logf("stepping down: peer %d has higher term %d", i, reply.Term)
			rn.mu.Unlock()
			return
		}
		
		rn.mu.Unlock()

		if reply.VoteGranted {
			votes++ 
		}
	}

	//Outcome of the election
	majority := len(rn.peers)/2 + 1
	rn.mu.Lock()

	//If node is the leader
	if (rn.state == Candidate) && rn.currentTerm == termForThisElection && votes >= majority {
		rn.state = Leader
		rn.logf("WON election, now LEADER")
		go rn.heartbeatLoop(termForThisElection)
	} else { //If node is not leader
		rn.logf("Did not win Election")
	}

	rn.mu.Unlock()
}	