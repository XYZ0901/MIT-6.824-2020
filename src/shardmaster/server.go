package shardmaster


import (
	"../raft"
	"fmt"
	"os"
	"sort"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

const (
	JOIN  			= "JOIN"
	LEAVE 			= "LEAVE"
	MOVE 			= "MOVE"
	QUERY 			= "QUERY"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	ClientTimeStamp map[int64]int64
	//when finished the op,
	//notify the waiting client with the res
	clientChannels map[int]chan Result
	LastAppliedIndex int
}

type Result struct {
	Err Err
	WrongLeader bool
	Config Config
}

type Op struct {
	// Your data here.
	OpType string
	Servers map[int][]string //for join
	GIDs []int //for leave
	Shard int//for move
	GID   int//for move
	Num int //for query

	ClientId int64
	Time int64
}

func (sm* ShardMaster) recordClient(index int64) {
	sm.mu.Lock()
	_,find := sm.ClientTimeStamp[index]
	if find == false {
		sm.ClientTimeStamp[index] = 0
	}
	sm.mu.Unlock()
}
//when we are not leader send res to waiting rpc handler
func (sm* ShardMaster) updateState(isLeader bool) {
	if !isLeader {
		sm.mu.Lock()
		if len(sm.clientChannels) != 0{

		}
		sm.mu.Unlock()
	}
}
func (sm* ShardMaster) getState() bool {
	_,isLeader := sm.rf.GetState()
	return isLeader
}

func (sm* ShardMaster) GetClientChannel(index int) (bool,chan Result) {
	var ok bool
	var ch chan Result
	sm.mu.Lock()
	ch,ok = sm.clientChannels[index]
	sm.mu.Unlock()
	return ok,ch
}

func (sm* ShardMaster) InsertClientChannel(index int,ch chan Result) {
	sm.mu.Lock()
	_,ok := sm.clientChannels[index]
	if ok {
		//DPrintf("insert ch error : ch exits \n")
		os.Exit(-1)
	}
	sm.clientChannels[index] = ch
	sm.mu.Unlock()
}
func (sm* ShardMaster) RemoveClientChannel(index int) {
	sm.mu.Lock()
	delete(sm.clientChannels,index)
	sm.mu.Unlock()
}

func (sm* ShardMaster) doJoin(op* Op,index int) Result {
	sm.mu.Lock()
	res := Result{OK,false,Config{}}
	time,find := sm.ClientTimeStamp[op.ClientId]
	if !find || (find && time < op.Time) {
		newGroup := make(map[int][]string)
		lastGroup := sm.configs[len(sm.configs) - 1].Groups
		for key, value := range lastGroup {
			newGroup[key] = value
		}
		for key, value := range op.Servers {
			newGroup[key] = value
		}
		shards := sm.balanceShards(newGroup)
		Num := len(sm.configs)
		newConfig := Config{Num,shards,newGroup}
		sm.configs = append(sm.configs, newConfig)
		sm.LastAppliedIndex = index
	}
	sm.mu.Unlock()
	return res
}

func (sm* ShardMaster) doLeave(op* Op,index int) Result {
	sm.mu.Lock()
	res := Result{OK,false,Config{}}
	time,find := sm.ClientTimeStamp[op.ClientId]
	if !find || (find && time < op.Time) {
		newGroup := make(map[int][]string)
		lastGroup := sm.configs[len(sm.configs) - 1].Groups
		for key, value := range lastGroup {
			newGroup[key] = value
		}
		for _, gid := range op.GIDs {
			delete(newGroup,gid)
		}
		shards := sm.balanceShards(newGroup)
		Num := len(sm.configs)
		newConfig := Config{Num,shards,newGroup}
		sm.configs = append(sm.configs, newConfig)
		sm.LastAppliedIndex = index
	}
	sm.mu.Unlock()
	return res
}

func (sm* ShardMaster) doMove(op* Op,index int) Result {
	sm.mu.Lock()
	res := Result{OK,false,Config{}}
	time,find := sm.ClientTimeStamp[op.ClientId]
	if !find || (find && time < op.Time) {
		newGroup := make(map[int][]string)
		lastGroup := sm.configs[len(sm.configs) - 1].Groups
		for key, value := range lastGroup {
			newGroup[key] = value
		}
		shards := sm.configs[len(sm.configs) - 1].Shards
		shards[op.Shard] = op.GID
		Num := len(sm.configs)
		newConfig := Config{Num,shards,newGroup}
		sm.configs = append(sm.configs, newConfig)
		sm.LastAppliedIndex = index
	}
	sm.mu.Unlock()
	return res
}

func (sm* ShardMaster) doQuery(op* Op,index int) Result {
	sm.mu.Lock()
	res := Result{OK,false,Config{}}
	//fmt.Printf("%v !!!!!!!!!!",op.Num)
	if op.Num < 0 || op.Num >= len(sm.configs) {
		res.Config = sm.configs[len(sm.configs) - 1]
	} else {
		res.Config = sm.configs[op.Num]
	}
	sm.LastAppliedIndex = index
	sm.mu.Unlock()
	return res
}

func (sm* ShardMaster) Run(op* Op,index int) Result {
	if op.OpType == JOIN {
		return sm.doJoin(op,index)
	} else if op.OpType == LEAVE {
		return sm.doLeave(op,index)
	} else if op.OpType == MOVE {
		return sm.doMove(op,index)
	} else if op.OpType == QUERY {
		return sm.doQuery(op,index)
	} else {
		fmt.Printf("invalid op")
		os.Exit(-1)
		return Result{}
	}
}

func (sm* ShardMaster) balanceShards(group map[int][]string) [NShards]int {
	var shards [NShards]int
	var gids []int
	for k, _ := range group {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	if len(gids) == 0 {
		return shards
	}
	divid := NShards / len(gids)
	for i := 0; i < len(gids);i++ {
		for j := 0; j < divid; j++ {
			shards[i * divid + j] = gids[i]
		}

		if (i == len(gids) - 1) && (i + 1) * divid < NShards {
			for j := (i + 1) * divid; j < NShards; j++ {
				shards[j] = gids[i]
			}
		}
	}
	return shards
}



func (sm* ShardMaster) doOp() {
	for {
		applyMsg := <-sm.applyCh
		op := applyMsg.Command.(Op)
		res := sm.Run(&op, applyMsg.CommandIndex)
		isLeader := sm.getState()
		sm.updateState(isLeader)
		if isLeader {
			ok, ch := sm.GetClientChannel(applyMsg.CommandIndex)
			sm.RemoveClientChannel(applyMsg.CommandIndex)
			if ok {
				ch <- res
			} else {
				//DPrintf("error : could not find notify channel\n")
				//os.Exit(-1)
			}
		} else {

		}
	}
}

func (sm* ShardMaster) waitComplete(index int) {
	time.Sleep(time.Millisecond * 700)
	ok,ch := sm.GetClientChannel(index)
	sm.RemoveClientChannel(index)
	if ok {
		res := Result{WrongLeader,true,Config{}}
		ch <- res
	} else {
		//has finished
		return
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.recordClient(args.Src)
	op := Op{JOIN,args.Servers,nil,-1,-1,-1,args.Src,args.Time}
	index,_,isLeader := sm.rf.Start(op)
	if !isLeader{
		reply.Err = WrongLeader
		reply.WrongLeader = true
	} else {
		ch := make(chan Result)
		sm.InsertClientChannel(index,ch)
		go sm.waitComplete(index)
		res,ok := <- ch
		if ok {
			reply.Err = res.Err
			reply.WrongLeader = res.WrongLeader
		} else {
			reply.Err = WrongLeader
			reply.WrongLeader = true
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.recordClient(args.Src)
	op := Op{LEAVE,nil,args.GIDs,-1,-1,-1,args.Src,args.Time}
	index,_,isLeader := sm.rf.Start(op)
	if !isLeader{
		reply.Err = WrongLeader
		reply.WrongLeader = true
	} else {
		ch := make(chan Result)
		sm.InsertClientChannel(index,ch)
		go sm.waitComplete(index)
		res,ok := <- ch
		if ok {
			reply.Err = res.Err
			reply.WrongLeader = res.WrongLeader
		} else {
			reply.Err = WrongLeader
			reply.WrongLeader = true
		}
	}

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.recordClient(args.Src)
	op := Op{MOVE,nil,nil,args.Shard,args.GID,-1,args.Src,args.Time}
	index,_,isLeader := sm.rf.Start(op)
	if !isLeader{
		reply.Err = WrongLeader
		reply.WrongLeader = true
	} else {
		ch := make(chan Result)
		sm.InsertClientChannel(index,ch)
		go sm.waitComplete(index)
		res,ok := <- ch
		if ok {
			reply.Err = res.Err
			reply.WrongLeader = res.WrongLeader
		} else {
			reply.Err = WrongLeader
			reply.WrongLeader = true
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.recordClient(args.Src)
	op := Op{QUERY,nil,nil,-1,-1,args.Num,args.Src,0}
	index,_,isLeader := sm.rf.Start(op)
	if !isLeader{
		reply.Err = WrongLeader
		reply.WrongLeader = true
	} else {
		ch := make(chan Result)
		sm.InsertClientChannel(index,ch)
		go sm.waitComplete(index)
		res,ok := <- ch
		if ok {
			reply.Err = res.Err
			reply.WrongLeader = res.WrongLeader
			reply.Config = res.Config
		} else {
			reply.Err = WrongLeader
			reply.WrongLeader = true
		}
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.ClientTimeStamp = make(map[int64]int64)
	sm.LastAppliedIndex = 0
	sm.clientChannels = make(map[int]chan Result)
	go sm.doOp()

	return sm
}
