package raft

import "log"

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
		DPrintf("send heartbeats\n")
	}

	for i := range rf.peers {
		if i != rf.me {
			if rf.log.lastLogIndex() >= rf.nextIndex[i] || heartsbeats {
				rf.sendAppendSL(i, heartsbeats)
			}
		}
	}

}

func (rf *Raft) sendAppendSL(server int, heartbeats bool) {

	next := rf.nextIndex[server]

	if next <= rf.lastIncludedIndex {
		next = rf.lastIncludedIndex + 1
	}
	if next-1 > rf.log.lastLogIndex() {
		next = rf.log.lastLogIndex()
	}

	prevLog := rf.entry(next - 1)
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: next - 1,
		PrevLogTerm:  prevLog.Term,
		Entries:      make([]Entry, rf.log.lastLogIndex()-next+1),
	}
	copy(args.Entries, rf.slice(next-rf.lastIncludedIndex))
	//DPrintf("leader %d send appendEntry to follower %d with arg %v\n", rf.me, server, args)
	go func() {
		var reply AppendEntryReply
		DPrintf("rf.log send [%d] is %v..........................\n", server, rf.log.Entries)
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
	index := rf.log.lastLogIndex()

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
		rf.nextIndex[server]--
	}
	rf.advanceCommit()
}

func (rf *Raft) advanceCommit() {
	if rf.role != Leader {
		log.Fatal("not leader\n")
	}
	//DPrintf("advanceCommit ......................................................")
	for N := rf.log.lastLogIndex(); N >= rf.commitIndex; N-- {
		vote := 1
		if rf.entry(N).Term == rf.currentTerm {

			for i := range rf.peers {
				if i != rf.me {
					if rf.matchIndex[i] >= N {
						vote++
					}
				}
			}

			if vote > len(rf.peers)/2 {
				rf.commitIndex = N
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

	reply.Success = false

	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
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
	if rf.log.lastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.log.len()
		DPrintf("out-of-date\n")
		return
	}

	if rf.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		DPrintf("not match\n")
		reply.ConflictTerm = rf.entry(args.PrevLogIndex).Term
		xterm := reply.ConflictTerm
		i := args.PrevLogIndex - 1
		for i > rf.lastIncludedIndex && rf.entry(i).Term == xterm {
			i--
		}
		reply.ConflictIndex = i + 1
		return
	}
	//DPrintf("append leader's log to my log, before append %d have length %d'log\n", rf.me, rf.log.len())
	reply.Success = true
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		if idx > rf.log.lastLogIndex() || rf.entry(idx).Term != entry.Term {
			rf.truncate(idx)
			rf.log.append(args.Entries[i:]...)
		}
	}
	DPrintf("after append log %v..........................\n", rf.log.Entries)
	//DPrintf("append leader's log to my log, %d after append  %d'slen have length %d'log\n", rf.me, len(args.Entries), rf.log.len())
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.log.lastLogIndex())
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
