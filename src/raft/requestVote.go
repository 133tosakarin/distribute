package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) resetElection() {
	now := time.Now()
	now = now.Add(300 * time.Millisecond)
	ms := rand.Int63() % 150
	now = now.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = now
}
func (rf *Raft) becomeCandidate() {
	rf.voteFor = rf.me
	rf.currentTerm++
	rf.role = Candidate
	//rf.resetElection()
}
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("rf.me = %d, tick\n", rf.me)
	if rf.role == Leader {
		//DPrintf("leader\n")
		rf.resetElection()
		rf.sendAppendL(true)
	}

	if time.Now().After(rf.electionTime) {
		rf.resetElection()
		rf.startElect()
	}
}
func (rf *Raft) newTerm(term int) {
	rf.role = Follower
	rf.voteFor = -1
	rf.currentTerm = term
}
func (rf *Raft) startElect() {
	rf.becomeCandidate()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.log.lastLogTerm(),
	}
	votes := 1
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVoteL(i, args, &votes)
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	//DPrintf("%d become leader\n", rf.me)
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
}
func (rf *Raft) sendRequestVoteL(server int, args *RequestVoteArgs, vote *int) {

	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.newTerm(reply.Term)
			rf.persist()
			DPrintf("change role to Follower\n")
		}
		if reply.VoteGranted {

			*vote += 1
			//DPrintf("receive %d reply %v, vote = %d, rf.role = %d\n", server, reply, *vote, rf.role)
			if *vote == len(rf.peers)/2+1 && rf.role == Candidate {
				if rf.currentTerm == args.Term {
					//fmt.Printf("%d become leader with term %d\n", rf.me, rf.currentTerm)

					rf.becomeLeader()
					//rf.resetElection()
					rf.sendAppendL(true)
					rf.persist()
				}
			}
		}

	}
}
