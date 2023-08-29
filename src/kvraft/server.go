package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// handle client args that include PUT, Get
	SeqId    int64
	ClientId int64
	Key      string
	Value    string
	OpType   string
	Index    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	//mp           map[int64]map[int64]bool
	data  map[string]string
	opCh  map[int]chan Op //
	seqMp map[int64]int64
	//persister         *raft.Persister
	lastIncludedIndex int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		DPrintf("GetReq with key: %v\n", args.Key)
		kv.processGetReq(args, reply)
	}

}

func (kv *KVServer) processGetReq(args *GetArgs, reply *GetReply) {

	op := Op{
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		OpType:   GET,
	}

	index, _, _ := kv.rf.Start(op)
	// create a chan by index if index is not exits, else return the chan
	ch := kv.getOpChan(index)
	// ensure every client just only send one request
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opCh, index)
	}()
	// some one crash mayby cause forever blocked at <-ch
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case msg := <-ch:
		{
			//msg must reach agreement  op
			if msg.ClientId != op.ClientId || msg.SeqId != op.SeqId {
				reply.Err = ErrWrongLeader
			} else {
				DPrintf("get success key%v msg.Value = %s\n", msg.Key, msg.Value)
				kv.mu.Lock()
				defer kv.mu.Unlock()
				reply.Value = kv.data[args.Key]
				reply.Err = OK
			}
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("putAppend key: %v, value: %v\n", args.Key, args.Value)
	if isLeader {
		kv.processPutAppendCommand(args, reply)
	} else {
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) processPutAppendCommand(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	index, _, _ := kv.rf.Start(op)
	if index == -1 {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getOpChan(index)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opCh, index)
	}()
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case msg := <-ch:
		{
			if msg.ClientId != op.ClientId || msg.SeqId != op.SeqId {
				reply.Err = ErrWrongLeader
				return
			} else {
				DPrintf("success putAppend key: %v, value: %v\n", args.Key, args.Value)
				reply.Err = OK
				return
			}
		}
	case <-timer.C:
		{
			reply.Err = ErrWrongLeader
		}
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) SaveRaftState() []byte {
	w := new(bytes.Buffer)
	ec := labgob.NewEncoder(w)
	ec.Encode(kv.data)
	ec.Encode(kv.seqMp)
	data := w.Bytes()
	return data
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func (kv *KVServer) apply() {
	for !kv.killed() {
		select {
		case am := <-kv.applyCh:
			{
				if kv.lastIncludedIndex >= am.CommandIndex {
					return
				}
				if am.CommandValid {
					index := am.CommandIndex
					op := am.Command.(Op)
					kv.mu.Lock()
					if !kv.isStale(op.ClientId, op.SeqId) {
						switch op.OpType {
						case PUT:
							kv.data[op.Key] = op.Value
							break
						case APPEND:
							kv.data[op.Key] += op.Value
							break
						}
						kv.seqMp[op.ClientId] = op.SeqId

					}
					kv.mu.Unlock()
					if kv.maxraftstate != -1 && kv.maxraftstate < kv.rf.GetRaftSize() {
						kv.mu.Lock()
						//defer kv.mu.Unlock()
						snapshot := kv.SaveRaftState()
						kv.rf.Snapshot(am.CommandIndex, snapshot)

						kv.mu.Unlock()
						//kv.snapshot = snapshot
					}
					kv.getOpChan(index) <- op

				}

				if am.SnapshotValid {
					kv.mu.Lock()
					//defer kv.mu.Unlock()
					kv.DecodePersister(am.Snapshot)
					kv.lastIncludedIndex = am.SnapshotIndex
					kv.mu.Unlock()
					kv.rf.Snapshot(am.SnapshotIndex, kv.SaveRaftState())
					//kv.snapshot = snapshot
				}

			}
		}

	}
}

func (kv *KVServer) getOpChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.opCh[index]
	if !exist {
		kv.opCh[index] = make(chan Op, 1)
		ch = kv.opCh[index]
	}

	return ch
}

func (kv *KVServer) isStale(cliendId int64, seqId int64) bool {

	lastSeqId, exsit := kv.seqMp[cliendId]
	if !exsit {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) DecodePersister(data []byte) {
	var kvdata map[string]string
	var seqMp map[int64]int64
	r := bytes.NewBuffer(data)
	dc := labgob.NewDecoder(r)
	if dc.Decode(&kvdata) != nil || dc.Decode(&seqMp) != nil {
		log.Fatal("readPersister error\n")
	}
	kv.data = kvdata
	kv.seqMp = seqMp

}
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.seqMp = make(map[int64]int64)
	kv.opCh = make(map[int]chan Op)
	//kv.persister = persister
	// You may need initialization code here.
	//kv.ReadPersister(persister.ReadRaftState())
	snapshot := persister.ReadSnapshot()
	kv.lastIncludedIndex = -1
	if len(snapshot) > 0 {
		kv.DecodePersister(snapshot)
	}
	go kv.apply()
	return kv
}
