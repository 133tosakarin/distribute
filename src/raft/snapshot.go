package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapshotL(server int) {
	//DPrintf(" send snapshot................................\n")
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	DPrintf("leader[%d] sendSnapshot with args: %v\n", rf.me, args)
	rf.sendSnapshotSL(server, args)
}

func (rf *Raft) sendSnapshotSL(server int, args InstallSnapshotArgs) {

	go func() {
		var reply InstallSnapshotReply
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processSnapshotReply(server, &args, &reply)
		}

	}()

}

func (rf *Raft) processSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.newTerm(reply.Term)
		rf.persist()
	} else {
		DPrintf("[%d]send snapshot success\n", rf.me)
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.advanceCommit()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	DPrintf("[%d]install snapshot rf.lastIcnluded = %d, rf.log.len = %d, args.lstInclude = %d, args.lastTerm = %d\n", rf.me, rf.lastIncludedIndex, rf.log.len(), args.LastIncludedIndex, args.LastIncludedTerm)

	if rf.currentTerm < args.Term {
		rf.newTerm(args.Term)
		rf.persist()
	}
	rf.resetElection()
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	rf.snapshot = args.Data
	defer func() {
		rf.persistWithSnapshot(args.Data)
		am := ApplyMsg{
			SnapshotValid: true,
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
			Snapshot:      rf.snapshot,
		}
		rf.mu.Unlock()
		go func() {
			rf.applyCh <- am
		}()

		//rf.snapshot = args.Data

		//rf.signalApplier()
	}()

	for i := 1; i < rf.log.len(); i++ {
		if rf.realIndex(i) == args.LastIncludedIndex && rf.log.at(i).Term == args.LastIncludedTerm {
			//DPrintf("find follower[%d]'s match lastIncludeIndex = %d\n", rf.me, args.LastIncludedIndex)
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.log.Entries = append([]Entry{{Term: args.LastIncludedTerm}}, rf.log.Entries[i+1:]...)
			//fmt.Printf("[%d]update commit = %d, applied = %d\n", rf.me, rf.commitIndex, rf.lastApplied)
			if args.LastIncludedIndex > rf.commitIndex {
				rf.commitIndex = args.LastIncludedIndex
			}

			if args.LastIncludedIndex > rf.lastApplied {
				rf.lastApplied = args.LastIncludedIndex
			}
			//fmt.Printf("[%d]update after commit = %d, applied = %d\n", rf.me, rf.commitIndex, rf.lastApplied)
			return
		}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	//fmt.Printf(".............[%d]update commit = %d, applied = %d\n", rf.me, rf.commitIndex, rf.lastApplied)

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	//rf.commitIndex = Max(rf.commitIndex, args.LastIncludedIndex)
	//fmt.Printf("................[%d]update after commit = %d, applied = %d\n", rf.me, rf.commitIndex, rf.lastApplied)
	rf.log.Entries = []Entry{{Term: rf.lastIncludedTerm}}
	//DPrintf("rf[%d] all snapshot with rf.lastInclude = %d, rf.lastTerm = %d, \n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	//rf.persist()
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
