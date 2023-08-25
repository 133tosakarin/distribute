package raft

import (
	"log"
)

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) sendAppendL(heartsbeats bool) {
	if heartsbeats {
		//DPrintf("send heartbeats with commitIdx = %d, appliedIndex = %d, lastInclude = %d, lastTerm = %d, log = %v\n", rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)
	}

	for i := range rf.peers {
		if i != rf.me {

			prevIndex := rf.nextIndex[i] - 1
			if prevIndex < rf.lastIncludedIndex {
				rf.sendSnapshotL(i)
			} else {
				rf.sendAppendSL(i, heartsbeats)
			}
		}
	}

}

func (rf *Raft) sendAppendSL(server int, heartbeats bool) {

	next := rf.nextIndex[server]
	if next-1 > rf.lastLogIndex() {
		next = rf.lastLogIndex()
	}
	DPrintf("send server[%d]next = %d, rf.lastInclude = %d, rf.lastLogIndex = %d, rf.lastTerm = %d\n", server, next, rf.lastIncludedIndex, rf.lastLogIndex(), rf.lastIncludedTerm)
	prevLog := rf.entry(next - 1)
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: next - 1,
		PrevLogTerm:  prevLog.Term,
		Entries:      make([]Entry, rf.lastLogIndex()-next+1),
	}
	copy(args.Entries, rf.slice(next))
	DPrintf("leader %d send appendEntry to follower %d , [%d]log is: %v, with arg %v\n", rf.me, server, rf.me, rf.log, args)
	go func() {
		var reply AppendEntryReply
		//DPrintf("rf.log send [%d] is %v..........................\n", server, rf.log.Entries)
		ok := rf.sendAppendEntry(server, args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//DPrintf("rf.role = %d\n", rf.role)
			rf.processReplyTermL(server, args, &reply)
		}

	}()

}

func (rf *Raft) processReplyTermL(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.currentTerm < args.Term {
		rf.newTerm(reply.Term)
		rf.persist()
	} else if rf.currentTerm == args.Term {
		rf.processReply(server, args, reply)
	}
}

func (rf *Raft) findLastConflictIndex(cterm int) int {
	index := rf.lastLogIndex()

	for index > rf.lastIncludedIndex {
		term := rf.entry(index).Term
		if term == cterm {
			return index
		} else if term < cterm {
			break
		}
		index--
	}
	return -1
}
func (rf *Raft) processReply(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if reply.Success {
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := newNext - 1

		if newNext > rf.nextIndex[server] {
			rf.nextIndex[server] = newNext
		}

		if newMatch > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatch
		}
		//DPrintf("update rf[%d].nextIndex[%d] = %d, .matchIndex[%d] = %d\n", rf.me, server, rf.nextIndex[server], server, rf.matchIndex[server])
	} else if reply.ConflictIndex != -1 {
		DPrintf("follower[%d] with Conflict reply %v\n", server, reply)
		if reply.ConflictTerm != -1 {
			index := rf.findLastConflictIndex(reply.ConflictTerm)
			if index != -1 {
				DPrintf(" find last index = %d, term = %d\n", index, rf.entry(index).Term)
			}
			if index != -1 {
				rf.nextIndex[server] = index + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}

		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	} else if rf.nextIndex[server] > 1 {
		DPrintf("nerver to here?\n")
		rf.nextIndex[server]--
		if rf.nextIndex[server] < rf.lastIncludedIndex+1 {
			DPrintf("why to here? rf.nextIndex[%d] = %d, rf.lastIncludeIndex = %d\n", server, rf.nextIndex[server], rf.lastIncludedIndex)
			rf.sendSnapshotL(server)
		}
	}
	rf.advanceCommit()
}

func (rf *Raft) advanceCommit() {
	if rf.role != Leader {
		return
	}
	//DPrintf("advanceCommit ......................................................")
	for N := rf.lastLogIndex(); N > rf.commitIndex && N > rf.lastIncludedIndex; N-- {
		vote := 1
		//DPrintf("advanceCommit: rf.lastLogIndex = %d, rf.log.len = %d, rf.commitIndex = %d, rf.lastIncludeIndex = %d\n", rf.lastLogIndex(), rf.log.len(), rf.commitIndex, rf.lastIncludedIndex)
		if rf.entry(N).Term == rf.currentTerm {

			for i := range rf.peers {
				if i != rf.me {
					if rf.matchIndex[i] >= N {
						vote++
					}
				}
			}

			if vote > len(rf.peers)/2 {
				//fmt.Printf("change ..........[%d]rf.commitIndex = %d\n", rf.me, rf.commitIndex)
				if N < rf.commitIndex {
					log.Fatal("some error happen here")
				}
				rf.commitIndex = N
				//fmt.Printf("change [%d]rf.commitIndex = %d\n", rf.me, rf.commitIndex)
				DPrintf("...............................................................set commit %d\n", N)
				break
			}
		}
	}

	rf.signalApplier()
}

func (rf *Raft) signalApplier() {
	//DPrintf("%d broadcast with role = %d\n", rf.me, rf.role)
	rf.cond.Broadcast()
}
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEnties", args, reply)
	return ok
}

func (rf *Raft) AppendEnties(args *AppendEntryArgs, reply *AppendEntryReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]follower receive leader[%d]'s rpc with args{%v}\n", rf.me, args.LeaderId, *args)
	reply.Success = false

	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("args.Term < follower[%d].Term \n", rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		rf.newTerm(args.Term)
		rf.persist()
	}
	if rf.role == Candidate {
		rf.role = Follower
	}
	reply.Term = rf.currentTerm
	rf.resetElection()
	if rf.lastIncludedIndex > args.PrevLogIndex {
		return
	}
	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		DPrintf("[%d]out-of-date\n", rf.me)
		return
	}
	//DPrintf("[%d]appendEntries judge match rf.lastIncludeIndex = %d, args.PrevLogIndex = %d, rf.log.len = %d\n", rf.me, rf.lastIncludedIndex, args.PrevLogIndex, rf.log.len())
	if rf.entry(args.PrevLogIndex).Term != args.PrevLogTerm {

		reply.ConflictTerm = rf.entry(args.PrevLogIndex).Term
		xterm := reply.ConflictTerm
		i := args.PrevLogIndex - 1
		for i > rf.lastIncludedIndex && rf.entry(i).Term == xterm {
			i--
		}
		//DPrintf("[%d]not match with ConflictIndex = %d, ConflictTerm = %d, my log = %v, args = %v, rf.lastInclude = %d, rf.lastTerm = %d\n", rf.me, i+1, reply.ConflictTerm, rf.log, args, rf.lastIncludedIndex, rf.lastIncludedTerm)
		reply.ConflictIndex = i + 1
		return
	}
	//DPrintf("append leader's log to my log, before append %d have length %d'log\n", rf.me, rf.log.len())
	reply.Success = true
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		if idx > rf.lastLogIndex() || rf.entry(idx).Term != entry.Term {
			rf.truncate(idx)
			rf.log.append(args.Entries[i:]...)
			break
		}
	}
	DPrintf("[%d]after append log %v\n", rf.me, rf.log.Entries)

	//DPrintf("append leader's log to my log, %d after append  %d'slen have length %d'log\n", rf.me, len(args.Entries), rf.log.len())
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Printf("change [%d].commitIndex = %d, lastInclud = %d, args.PrevLogIndex = %d\n", rf.me, rf.commitIndex, rf.lastIncludedIndex, args.PrevLogIndex)
		rf.commitIndex = Min(args.LeaderCommit, rf.lastLogIndex())
		//fmt.Printf(" change [%d].commitIndex = %d,lastApplied = %d, lastInclud = %d, rf.lastLogIndex = %d\n", rf.me, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.lastLogIndex())
		rf.signalApplier()
	}
	rf.persist()
	//DPrintf("rf.commitIndex = %d\n", rf.commitIndex)

}

func Min(a int, b int) int {
	if a > b {

		return b
	} else {
		return a
	}
}
