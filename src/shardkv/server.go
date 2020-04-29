package shardkv

import (
	"../shardmaster"
	"bytes"
	"fmt"
	"os"
	"time"
)
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"

const (
	PUT             = "Put"
	APPEND			= "Append"
	GET             = "Get"
	CONFIG          = "CONFIG"
	SHARDS          = "SHARDS"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string
	ConfigNum int

	RequestId int64
	LastRequestId int64

	Config shardmaster.Config

	Storage map[string]string
	Cache   map[int64]string
	Src     int
}

type Result struct {
	Err Err
	Res string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big


	mck*         shardmaster.Clerk
	// Your definitions here.
	Storage      map[string]string
	ClientRequestCache map[int64]string
	clientChannels map[int]chan Result
	LastAppliedIndex int

	Config       shardmaster.Config

	WaitingShards   map[int][]int
	HistoryConfigs  []shardmaster.Config
	MigratingShards map[int]map[int]MigrationData

	IsMigrating_     bool
	Cond             *sync.Cond
}

func (kv* ShardKV) WaitingShardsNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.WaitingShards)
}

func (kv* ShardKV) IsMigrating() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.IsMigrating_
}

func (kv* ShardKV) Configuration() shardmaster.Config {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Config
}
func (kv* ShardKV) Shards() [shardmaster.NShards]int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Config.Shards
}


func (kv* ShardKV) checkStateAndConfig(key string,configNum int) (Err,bool) {
	if configNum != kv.Configuration().Num {
		return ErrWrongGroup,false
	}
	if kv.IsMigrating() {
		return ErrWrongGroup,false
	}
	shard := key2shard(key)
	if kv.Configuration().Shards[shard] != kv.gid {
		return ErrWrongGroup,false
	}
	isLeader := kv.getState()
	if isLeader {
		return OK,true
	} else {
		return ErrWrongLeader,false
	}
}


func (kv* ShardKV) getState() bool {
	_,isLeader := kv.rf.GetState()
	return isLeader
}

func (kv* ShardKV) detectConfig() {
	for  {
		time.Sleep(100 * time.Millisecond)
		if !kv.getState() || kv.IsMigrating() == true  {
			continue
		} else {
			go func() {
				nextConfigNum := kv.Configuration().Num + 1
				config := kv.mck.Query(nextConfigNum)
				if config.Num == nextConfigNum {
					op := Op{CONFIG,"","",nextConfigNum,0,0,config,nil,nil,0}
					kv.rf.Start(op)
					DPrintf("Config Log %v",nextConfigNum)
				}
			}()
			time.Sleep(100 * time.Millisecond)
		}
	}

}


func (kv* ShardKV) askForShards(gid int,shards []int,configNum int)  {
	kv.mu.Lock()
	servers := kv.HistoryConfigs[configNum].Groups[gid]
	kv.mu.Unlock()
	request := AskShardsRequest{shards,configNum}
	for  {
		for si := 0; si < len(servers); si++ {
			reply := AskShardsReply{}
			srv := kv.make_end(servers[si])
			ok := srv.Call("ShardKV.AskShards", &request, &reply)
			if ok && reply.Err == OK {
				//DPrintf(" ask shards success ")
				//op := Op{SHARDS,"","",configNum,0,0,shardmaster.Config{},reply.Storage,reply.ClientTimeStamp,gid}
				//kv.rf.Start(op)
				kv.mu.Lock()
				delete(kv.WaitingShards,gid)
				for k, v := range reply.Storage {
					kv.Storage[k] = v
				}
				for k, v := range reply.ClientTimeStamp {
					kv.ClientRequestCache[k] = v
				}
				kv.Cond.Signal()
				kv.mu.Unlock()
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				time.Sleep(time.Millisecond * 200)
				continue
			}
			// ... not ok, or ErrWrongLeader
		}
		time.Sleep(time.Millisecond * 200)
	}
}
func findList(shards []int,shard int) bool {
	res := false
	for i := 0; i < len(shards); i++ {
		if shards[i] == shard {
			res = true
			break
		}
	}
	return res
}

func (kv* ShardKV) doMigrating() {
	for  {
		time.Sleep(time.Millisecond * 150)
		if kv.WaitingShardsNum() == 0 || kv.getState() == false {
			continue
		}
		kv.mu.Lock()
		oldConfig := kv.HistoryConfigs[len(kv.HistoryConfigs) - 1]
		wg := sync.WaitGroup{}
		for gid, shards := range kv.WaitingShards {
			wg.Add(1)
				go func(gid int,shards []int) {
					kv.askForShards(gid,shards,oldConfig.Num)
					wg.Done()
				}(gid,shards)
		}
		kv.mu.Unlock()
		wg.Wait()
	}

}


func (kv* ShardKV) doConfig(op* Op,index int) {
	if op.Config.Num == kv.Configuration().Num + 1 {
		kv.mu.Lock()
		if len(kv.WaitingShards) != 0 || kv.IsMigrating_ {
			os.Exit(-1)
		}
		kv.IsMigrating_ = true
		oldConfig, oldShards := kv.Config, kv.Config.Shards
		kv.HistoryConfigs = append(kv.HistoryConfigs, kv.Config)
		kv.Config = op.Config
		newShards := kv.Config.Shards
		kv.WaitingShards = make(map[int][]int)
		var deleteSrc []int
		for i := 0; i < shardmaster.NShards; i++ {
			if newShards[i] == kv.gid && oldShards[i] != kv.gid && oldShards[i] != -1 {
				kv.WaitingShards[oldShards[i]] = append(kv.WaitingShards[oldShards[i]], i)
			} else if oldShards[i] == kv.gid && newShards[i] != kv.gid {
				deleteSrc = append(deleteSrc, i)
			}
		}
		if len(deleteSrc) != 0 {
			DPrintf("%v--%v before delete config to %v   %v",kv.gid,kv.me,kv.Config.Num,kv.Storage)
			v := make(map[int]MigrationData)
			for _, shard := range deleteSrc {
				data := MigrationData{make(map[string]string), make(map[int64]string)}
				for k, v := range kv.Storage {
					if key2shard(k) == shard {
						data.Storage[k] = v
						delete(kv.Storage, k)
					}
				}
				for k, v := range kv.ClientRequestCache {
					if key2shard(v) == shard {
						data.ClientRequestCache[k] = v
						delete(kv.ClientRequestCache, k)
					}
				}
				v[shard] = data
			}
			kv.MigratingShards[oldConfig.Num] = v
			DPrintf("%v--%v after delete config to %v   %v",kv.gid,kv.me,kv.Config.Num,kv.Storage)
		}

		kv.mu.Unlock()
		//ask sharded
		if kv.WaitingShardsNum() == 0 {
			kv.mu.Lock()
			kv.LastAppliedIndex = index
			kv.IsMigrating_ = false
			kv.mu.Unlock()
			DPrintf("%v--%v update config to %v   %v",kv.gid,kv.me,kv.Configuration().Num,kv.Storage)
			return
		} else {
			kv.mu.Lock()
			for gid, shards := range kv.WaitingShards {
				go func(gid int,shards []int,configNum int) {
					kv.askForShards(gid,shards,configNum)
				}(gid,shards,oldConfig.Num)
			}
			for len(kv.WaitingShards) != 0 {
				kv.Cond.Wait()
			}
			kv.LastAppliedIndex = index
			kv.IsMigrating_ = false
			kv.mu.Unlock()
			DPrintf("%v--%v update config to %v   %v",kv.gid,kv.me,kv.Configuration().Num,kv.Storage)
		}
	}
}

func (kv* ShardKV) applyShards(op* Op,index int) {
	if op.ConfigNum == kv.Configuration().Num - 1 {
		//DPrintf("apply hsards %v",op.Storage)
		kv.mu.Lock()
		delete(kv.WaitingShards,op.Src)
		for k, v := range op.Storage {
			kv.Storage[k] = v
		}
		for k, v := range op.Cache {
			kv.ClientRequestCache[k] = v
		}
		if len(kv.WaitingShards) == 0 {
			DPrintf("%v--%v end update config from %v to %v   %v",kv.gid,kv.me,op.ConfigNum,kv.Config.Num,kv.WaitingShards)

		}
		kv.LastAppliedIndex = index
		kv.mu.Unlock()
	}
}

func (kv* ShardKV) Run(op *Op,index int) Result {
	res := Result{}
	res.Err = OK
	kv.mu.Lock()
	shard := key2shard(op.Key)
	if kv.Config.Shards[shard] != kv.gid || op.ConfigNum != kv.Config.Num || kv.IsMigrating_ {
		res.Err = ErrWrongGroup
		//DPrintf("wrong group %v %v %v",op.OpType,op.Key,op.Value)
		kv.mu.Unlock()
		return res
	}
	if op.OpType == GET {
		_,ok := kv.Storage[op.Key]
		if ok {
			res.Res = kv.Storage[op.Key]
		} else {
			res.Err = ErrNoKey
		}
		kv.LastAppliedIndex = index
	} else {
			_,find := kv.ClientRequestCache[op.RequestId]
			if !find {
				if op.OpType == PUT {
					//DPrintf("put %v %v !!!!!!!!!!!! %v",op.Key,op.Value,kv.Config.Num)
					kv.Storage[op.Key] = op.Value
				} else {
					//DPrintf("append %v %v!!!!!!!!!! %v",op.Key,op.Value,kv.Config.Num)
					kv.Storage[op.Key] += op.Value
				}
				delete(kv.ClientRequestCache,op.LastRequestId)
				kv.ClientRequestCache[op.RequestId] = op.Key
				kv.LastAppliedIndex = index
				//DPrintf("%v update %v",kv.gid,kv.ClientRequestCache)
			} else {
				//DPrintf("%v ignore --- could not find %v %v",kv.gid,op.LastRequestId,kv.ClientRequestCache)
			}
		}
	kv.mu.Unlock()
	return res
}

func (kv* ShardKV) GetClientChannel(index int) (bool,chan Result) {
	var ok bool
	var ch chan Result
	kv.mu.Lock()
	ch,ok = kv.clientChannels[index]
	kv.mu.Unlock()
	return ok,ch
}

func (kv* ShardKV) InsertClientChannel(index int,ch chan Result) {
	kv.mu.Lock()
	_,ok := kv.clientChannels[index]
	if ok {
		DPrintf("insert ch error : ch exits \n")
		os.Exit(-1)
	}
	kv.clientChannels[index] = ch
	kv.mu.Unlock()
}

func (kv* ShardKV) RemoveClientChannel(index int) {
	kv.mu.Lock()
	delete(kv.clientChannels,index)
	kv.mu.Unlock()
}

func (kv* ShardKV) doOp() {

	for  {
		applyMsg := <- kv.applyCh
		if applyMsg.DoSnapshot {
			kv.readSnapshot()
			continue
		}
		op := applyMsg.Command.(Op)
		if op.OpType == CONFIG {
			kv.doConfig(&op,applyMsg.CommandIndex)
			go kv.saveSnapshot()
			continue
		}
		if op.OpType == SHARDS {
			//DPrintf("apply shards")
			kv.applyShards(&op,applyMsg.CommandIndex)
			go kv.saveSnapshot()
			continue
		}
		res := kv.Run(&op,applyMsg.CommandIndex)
		go kv.saveSnapshot()
		isLeader := kv.getState()
		if isLeader {
			ok,ch := kv.GetClientChannel(applyMsg.CommandIndex)
			kv.RemoveClientChannel(applyMsg.CommandIndex)
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	err,isOK := kv.checkStateAndConfig(args.Key,args.ConfigNum)
	if !isOK {
		reply.Err = err
		return
	}
	op := Op{GET,args.Key,"",args.ConfigNum,-1,-1,shardmaster.Config{},nil,nil,-1}
	index,_,isLeader := kv.rf.Start(op)

	if !isLeader {
		//DPrintf("received Get %v not leader ",kv.me)
		reply.Err = ErrWrongLeader
		return
	} else {
		ch := make(chan Result)
		kv.InsertClientChannel(index,ch)
		go kv.waitComplete(index)
		res,ok := <- ch
		kv.RemoveClientChannel(index)
		if ok {
			reply.Err = res.Err
			reply.Value = res.Res
		} else {
			//DPrintf("server is not leader now")
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	err,isOK := kv.checkStateAndConfig(args.Key,args.ConfigNum)
	if !isOK {
		reply.Err = err
		return
	}
	var op Op
	if args.Op == PUT {
		op = Op{PUT,args.Key,args.Value,args.ConfigNum,args.RequestId,args.LastRequestId,shardmaster.Config{},nil,nil,0}
	} else if args.Op == APPEND {
		op = Op{APPEND,args.Key,args.Value,args.ConfigNum,args.RequestId,args.LastRequestId,shardmaster.Config{},nil,nil,0}
	} else {
		//DPrintf("invalid op in PutAppend\n")
	}
	//op := Op{GET,args.Key,""}
	index,_,isLeader := kv.rf.Start(op)

	if !isLeader {
		//DPrintf("received PutAppend %v not leader ",kv.me)
		reply.Err = ErrWrongLeader
		return
	} else {
		ch := make(chan Result)
		kv.InsertClientChannel(index,ch)
		go kv.waitComplete(index)
		res,ok := <- ch
		kv.RemoveClientChannel(index)
		if ok {
			reply.Err = res.Err
		} else {
			//DPrintf("server is not leader now")
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *ShardKV) AskShards(args* AskShardsRequest,reply* AskShardsReply) {
	isLeader := kv.getState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else if args.ConfigNum >= kv.Configuration().Num {
		reply.Err = ErrWrongGroup
		return
	}

	reply.Err = OK
	reply.Storage,reply.ClientTimeStamp = make(map[string]string),make(map[int64]string)
	kv.mu.Lock()
	if v, ok := kv.MigratingShards[args.ConfigNum]; ok {
		for _, shard := range args.Shards {
			if migrationData, ok := v[shard]; ok {
				for k, v := range migrationData.Storage {
					reply.Storage[k] = v
				}
				for k, v := range migrationData.ClientRequestCache {
					reply.ClientTimeStamp[k] = v
				}
			} else {
				DPrintf("could not find shard")
				os.Exit(-1)
			}
		}

	} else {
		DPrintf("could not find configNum")
		os.Exit(-1)
	}
	kv.mu.Unlock()
	//DPrintf("reply shards %v ",reply.Storage)
}

func (kv* ShardKV) waitComplete(index int) {
	time.Sleep(time.Millisecond * 700)
	ok,ch := kv.GetClientChannel(index)
	kv.RemoveClientChannel(index)
	if ok {
		res := Result{ErrWrongLeader,""}
		ch <- res
	} else {
		//has finished
		return
	}
}

func (kv* ShardKV) saveSnapshot() {
	if kv.rf.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		kv.mu.Lock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.Storage)
		e.Encode(kv.ClientRequestCache)
		e.Encode(kv.Config)
		e.Encode(kv.WaitingShards)
		e.Encode(kv.HistoryConfigs)
		e.Encode(kv.MigratingShards)
		index := kv.LastAppliedIndex
		data := w.Bytes()
		kv.mu.Unlock()
		kv.rf.SaveStateAndSnapShot(data,index)
	}
}

func (kv* ShardKV) readSnapshot() {
	data,index := kv.rf.ReadSnapshot()
	if index <= 0 || data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	Storage := make(map[string]string)
	ClientRequestCache := make(map[int64]string)
	Config := shardmaster.Config{}
	WaitingShards := make(map[int][]int)
	var HistoryConfigs []shardmaster.Config
	MigratingShards := make(map[int]map[int]MigrationData)
	if d.Decode(&Storage) != nil ||
		d.Decode(&ClientRequestCache) != nil ||
		d.Decode(&Config) != nil ||
		d.Decode(&WaitingShards) != nil ||
		d.Decode(&HistoryConfigs) != nil ||
		d.Decode(&MigratingShards) != nil  {
		fmt.Printf("decode error")
		os.Exit(-1)
	} else {
		kv.mu.Lock()
		kv.Storage = Storage
		kv.ClientRequestCache = ClientRequestCache
		kv.LastAppliedIndex = index
		kv.Config = Config
		kv.WaitingShards = WaitingShards
		kv.HistoryConfigs = HistoryConfigs
		kv.MigratingShards = MigratingShards
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.Storage = make(map[string]string)
	kv.clientChannels = make(map[int]chan Result)
	kv.ClientRequestCache = make(map[int64]string)
	kv.Config = shardmaster.Config{}
	for i := 0; i < shardmaster.NShards; i++ {
		kv.Config.Shards[i] = -1
	}
	kv.Config.Num = 0
	kv.LastAppliedIndex = 0
	kv.WaitingShards = make(map[int][]int)
	kv.MigratingShards = make(map[int]map[int]MigrationData)
	kv.IsMigrating_ = false
	kv.Cond = sync.NewCond(&kv.mu)
	kv.readSnapshot()

	go kv.detectConfig()
	go kv.doOp()

	return kv
}
