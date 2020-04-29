package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"log"
	"os"
)
import "time"
import "crypto/rand"
import "math/big"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return

}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.cachedLeader = 0
	ck.ClientId = time.Now().UnixNano()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Src = ck.ClientId

	res := Config{}
	for i := 0; ; i++ {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply QueryReply
		//	ok := srv.Call("ShardMaster.Query", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return reply.Config
		//	}
		//}
		reply := QueryReply{}
		ok := ck.servers[ck.cachedLeader].Call("ShardMaster.Query", args, &reply)
		if !ok || (reply.Err == WrongLeader && reply.WrongLeader == true) {
			ck.cachedLeader = (ck.cachedLeader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == OK {
			res = reply.Config
		} else {
			os.Exit(-1)
		}
		return  res
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Time = time.Now().UnixNano()
	args.Src = ck.ClientId
	for {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply JoinReply
		//	ok := srv.Call("ShardMaster.Join", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return
		//	}
		//}
		//time.Sleep(100 * time.Millisecond)
		reply := JoinReply{}
		ok := ck.servers[ck.cachedLeader].Call("ShardMaster.Join", args, &reply)
		if !ok || (reply.Err == WrongLeader && reply.WrongLeader == true) {
			ck.cachedLeader = (ck.cachedLeader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == OK {

		} else {
			os.Exit(-1)
		}
		break
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Src = ck.ClientId
	args.Time = time.Now().UnixNano()
	for i := 0; ; i++ {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply LeaveReply
		//	ok := srv.Call("ShardMaster.Leave", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return
		//	}
		//}
		//time.Sleep(100 * time.Millisecond)
		reply := LeaveReply{}
		ok := ck.servers[ck.cachedLeader].Call("ShardMaster.Leave", args, &reply)
		if !ok || (reply.Err == WrongLeader && reply.WrongLeader == true) {
			ck.cachedLeader = (ck.cachedLeader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == OK {

		} else {
			os.Exit(-1)
		}
		break

	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Time = time.Now().UnixNano()
	args.Src = ck.ClientId
	for i := 0; ; i++ {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply MoveReply
		//	ok := srv.Call("ShardMaster.Move", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return
		//	}
		//}
		//time.Sleep(100 * time.Millisecond)
		reply := MoveReply{}
		ok := ck.servers[ck.cachedLeader].Call("ShardMaster.Move", args, &reply)
		if !ok || (reply.Err == WrongLeader && reply.WrongLeader == true) {
			ck.cachedLeader = (ck.cachedLeader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == OK {

		} else {
			os.Exit(-1)
		}

		break
	}
}
