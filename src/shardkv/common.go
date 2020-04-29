package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return

}

const (
	OK             		= "OK"
	ErrNoKey       		= "ErrNoKey"
	ErrWrongGroup  		= "ErrWrongGroup"
	ErrWrongLeader 		= "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	LastRequestId int64
	RequestId     int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Src int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrationData struct {
	Storage      map[string]string
	ClientRequestCache map[int64]string
}

type AskShardsRequest struct {
	Shards []int
	ConfigNum int
}

type AskShardsReply struct {
	Err   Err
	Storage map[string]string
	ClientTimeStamp map[int64]string
}
