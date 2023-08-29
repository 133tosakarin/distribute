package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

const (
	PUT      = "Put"
	APPEND   = "Append"
	GET      = "Get"
	KVSERVER = "KVServer."
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int64
	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = (nrand())
	ck.leaderId = int(nrand()) % len(servers)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	//DPrintf("Get key: %v\n", key)
	// You will have to modify this function.
	ck.seqId++ // ensure each comman is unique

	leaderId := ck.leaderId
	args := GetArgs{
		Key:      key,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}
	for {
		//DPrintf("leaderId = %d\n", leaderId)
		var reply GetReply
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = leaderId
				return ""
			} else if reply.Err == ErrWrongLeader {
				leaderId = (leaderId + 1) % len(ck.servers)
				continue
			} else {
				ck.leaderId = leaderId
				return reply.Value
			}
		}
		leaderId = (leaderId + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//DPrintf("%s key: %v, value: %v\n", op, key, value)
	//method := KVSERVER + op
	ck.seqId++ // ensure every command is unique
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}

	leaderId := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, &reply)
		if ok {
			if reply.Err != OK {
				leaderId = (leaderId + 1) % len(ck.servers)
				continue
			} else {
				ck.leaderId = leaderId
				break
			}
		}

		leaderId = (leaderId + 1) % len(ck.servers)

	}

}

func (ck *Clerk) processServerReply(peer int, args *PutAppendArgs, reply *PutAppendReply) {
	if reply.Err == OK {
		DPrintf("this[%d] is leader\n ", peer)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
