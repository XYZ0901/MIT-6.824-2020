package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return

}

const (
	PUT             = "Put"
	APPEND			= "Append"
	GET             = "Get"

)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string
	Time int64
	ClientId int64
}


type Result struct {
	Err Err
	Res string
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.

	Storage map[string]string
	ClientTimeStamp map[int64]int64
	//key is the index of LogEntry,
	//when finished the op,
	//notify the waiting client with the res
	clientChannels map[int]chan Result
	isLeader bool
	LastAppliedIndex int

}

func (kv* KVServer) recordClient(index int64) {
	kv.mu.Lock()
	_,find := kv.ClientTimeStamp[index]
	if find == false {
		kv.ClientTimeStamp[index] = 0
	}
	kv.mu.Unlock()
}
//when we are not leader send res to waiting rpc handler
func (kv* KVServer) updateState(isLeader bool) {
	if !isLeader {
		kv.mu.Lock()
		if len(kv.clientChannels) != 0{

		}
		kv.mu.Unlock()
	}
}

func (kv* KVServer) getState() bool {
	_,isLeader := kv.rf.GetState()
	return isLeader
}

func (kv* KVServer) Run(op *Op,index int) Result {
	res := Result{}
	res.Err = OK
	kv.mu.Lock()
	if op.OpType == GET {
		_,ok := kv.Storage[op.Key]
		if ok {
			res.Res = kv.Storage[op.Key]
		} else {
			res.Err = ErrNoKey
		}
		kv.LastAppliedIndex = index
	} else {
		if op.OpType == PUT {
			time,find := kv.ClientTimeStamp[op.ClientId]
			if !find || (find && time < op.Time) {
				kv.Storage[op.Key] = op.Value
				kv.ClientTimeStamp[op.ClientId] = op.Time
			}
		} else if op.OpType == APPEND {
			time,find := kv.ClientTimeStamp[op.ClientId]
			if !find || (find && time < op.Time) {
				kv.ClientTimeStamp[op.ClientId] = op.Time
				_,ok := kv.Storage[op.Key]
				if ok {
					kv.Storage[op.Key] += op.Value
				} else {
					kv.Storage[op.Key] = op.Value
				}
			}

		}  else {
			os.Exit(-1)
			DPrintf("invalid op in Run() ")
		}
		kv.LastAppliedIndex = index
	}
	kv.mu.Unlock()
	return res
}

func (kv* KVServer) GetClientChannel(index int) (bool,chan Result) {
	var ok bool
	var ch chan Result
	kv.mu.Lock()
	ch,ok = kv.clientChannels[index]
	kv.mu.Unlock()
	return ok,ch
}

func (kv* KVServer) InsertClientChannel(index int,ch chan Result) {
	kv.mu.Lock()
	_,ok := kv.clientChannels[index]
	if ok {
		DPrintf("insert ch error : ch exits \n")
		os.Exit(-1)
	}
	kv.clientChannels[index] = ch
	kv.mu.Unlock()
}
func (kv* KVServer) RemoveClientChannel(index int) {
	kv.mu.Lock()
	delete(kv.clientChannels,index)
	kv.mu.Unlock()
}
func (kv* KVServer) doOp() {

	for  {
		applyMsg := <- kv.applyCh
		if applyMsg.DoSnapshot {
			kv.readSnapshot()
			continue
		}
		op := applyMsg.Command.(Op)
		res := kv.Run(&op,applyMsg.CommandIndex)
		go kv.saveSnapshot()
		isLeader := kv.getState()
		kv.updateState(isLeader)
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.recordClient(args.Src)
	op := Op{GET,args.Key,"",0,args.Src}
	index,_,isLeader := kv.rf.Start(op)
	kv.updateState(isLeader)
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
			DPrintf("server is not leader now")
			reply.Err = ErrWrongLeader
		}
	}
}


func (kv* KVServer) waitComplete(index int) {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.recordClient(args.Src)
	var op Op
	if args.Op == PUT {
		op = Op{PUT,args.Key,args.Value,args.Time,args.Src}
	} else if args.Op == APPEND {
		op = Op{APPEND,args.Key,args.Value,args.Time,args.Src}
	} else {
		DPrintf("invalid op in PutAppend\n")
	}
	//op := Op{GET,args.Key,""}
	index,_,isLeader := kv.rf.Start(op)
	kv.updateState(isLeader)
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
			DPrintf("server is not leader now")
			reply.Err = ErrWrongLeader
		}
	}
}



func (kv* KVServer) saveSnapshot() {
	if kv.rf.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		kv.mu.Lock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.Storage)
		e.Encode(kv.ClientTimeStamp)
		index := kv.LastAppliedIndex
		data := w.Bytes()
		kv.mu.Unlock()
		kv.rf.SaveStateAndSnapShot(data,index)
	}

}

func (kv* KVServer) readSnapshot() {
	data,index := kv.rf.ReadSnapshot()
	if index <= 0 || data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	Storage := make(map[string]string)
	ClientTimeStamp := make(map[int64]int64)

	if d.Decode(&Storage) != nil ||
	   d.Decode(&ClientTimeStamp) != nil {
		fmt.Printf("decode error")
		os.Exit(-1)
	} else {
		kv.mu.Lock()
		kv.Storage = Storage
		kv.ClientTimeStamp = ClientTimeStamp
		kv.LastAppliedIndex = index
		kv.mu.Unlock()
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

	// You may need initialization code here.
	kv.Storage = make(map[string]string)
	kv.clientChannels = make(map[int]chan Result)
	kv.isLeader = false
	kv.ClientTimeStamp = make(map[int64]int64)
	kv.readSnapshot()
	go kv.doOp()
	return kv
}
