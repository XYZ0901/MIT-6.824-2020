package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cachedLeader int
	ClientId int64
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

	ck.cachedLeader = 0
	ck.ClientId = time.Now().UnixNano()
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
	res := ""
	args := GetArgs{key,ck.ClientId}
	for i := 0; ; i++ {
		//DPrintf("client send Get to %v",ck.cachedLeader)
		reply := GetReply{}
		ok := ck.servers[ck.cachedLeader].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.cachedLeader = (ck.cachedLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == OK {
			res = reply.Value
			//DPrintf("client Get %v:%v success from Leader %v",key,res,ck.cachedLeader)
		} else {
			DPrintf("Client Get %v error\n",key)
		}
		break
	}
	// You will have to modify this function.
	return res
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
	args := PutAppendArgs{key,value,op,time.Now().UnixNano(),ck.ClientId}
	for i := 0; ; i++ {
		reply := PutAppendReply{}
		//DPrintf("client send PutAppend to %v",ck.cachedLeader)
		ok := ck.servers[ck.cachedLeader].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.cachedLeader = (ck.cachedLeader + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			//DPrintf("client %v success from Leader %v",op,ck.cachedLeader)
		} else {
			//DPrintf("client %v ---- %v:%v error",op,key,value)
		}
		break
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
