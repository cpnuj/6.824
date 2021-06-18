package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string // Put or Append
	Key      string
	Value    string
	ClientId int64
	Seq      int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// applied is a condition variable to record
	// the current applied command index
	c       *sync.Cond
	applied int
	log     map[int]Op    // track index -> op
	cliseq  map[int64]int // track client's max seq
	data    map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if isLead := kv.rf.CheckQuorum(); !isLead {
		reply.Err = NotLead
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value
		reply.Err = NoErr
	} else {
		reply.Err = Fail
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, ok := kv.rf.Start(Op{Key: args.Key, Value: args.Value,
		Type: args.Op, ClientId: args.ClientID, Seq: args.Seq})
	if !ok {
		reply.Err = NotLead
		return
	}
	// wait for the command to be applied
	kv.c.L.Lock()
	for kv.applied < index {
		kv.c.Wait()
	}
	kv.c.L.Unlock()

	kv.mu.Lock()
	aop := kv.log[index]
	kv.mu.Unlock()

	cid, seq := aop.ClientId, aop.Seq
	_, isLead := kv.rf.GetState()
	if !isLead || cid != args.ClientID || seq != args.Seq {
		reply.Err = Fail
	} else {
		reply.Err = NoErr
	}
}

func (kv *KVServer) processApplyMsg(am raft.ApplyMsg) {
	kv.mu.Lock()
	op := am.Command.(Op)

	if _, ok := kv.cliseq[op.ClientId]; !ok {
		kv.cliseq[op.ClientId] = -1
	}

	if op.Seq > kv.cliseq[op.ClientId] {
		switch op.Type {
		case "Put":
			kv.data[op.Key] = op.Value
		case "Append":
			kv.data[op.Key] = kv.data[op.Key] + op.Value
		}
		kv.cliseq[op.ClientId] = op.Seq
	}
	kv.mu.Unlock()

	// update applied index
	kv.c.L.Lock()
	if am.CommandIndex <= kv.applied {
		log.Fatalf("%d attempt to apply smaller index %d applied index: %d",
			kv.me, am.CommandIndex, kv.applied)
	}
	kv.applied = am.CommandIndex

	kv.mu.Lock()
	kv.log[am.CommandIndex] = op
	kv.mu.Unlock()

	kv.c.Broadcast()
	kv.c.L.Unlock()
}

func (kv *KVServer) run() {
	for !kv.killed() {
		am := <-kv.applyCh
		kv.processApplyMsg(am)
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
	DPrintf("[debug kill] server %d got killed\n", kv.me)
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
	// condition lock
	kv.c = sync.NewCond(&sync.Mutex{})
	kv.log = make(map[int]Op)
	kv.cliseq = make(map[int64]int)
	kv.data = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.run()
	// We should run server goroutine first, then the
	// initialization of raft can apply the previous log
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
