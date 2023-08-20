package raft

type Entry struct {
	Command interface{}
	Term    int
}

type Log struct {
	Entries []Entry
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) lastLogIndex() int {
	return l.len() - 1
}

func (l *Log) lastLogTerm() int {
	return l.Entries[l.lastLogIndex()].Term
}

func NewLog() Log {
	log := Log{}
	log.Entries = make([]Entry, 1)
	return log
}

func (l *Log) at(index int) *Entry {
	return &l.Entries[index]
}
func (rf *Raft) entryIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) entry(index int) *Entry {
	return rf.log.at(rf.entryIndex(index))
}

func (rf *Raft) truncate(index int) {
	idx := rf.entryIndex(index)
	rf.log.Entries = rf.log.Entries[:idx]
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

func (rf *Raft) slice(index int) []Entry {
	idx := rf.entryIndex(index)
	return rf.log.Entries[idx:]
}
