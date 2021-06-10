package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func min(x, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

func max(x, y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string // Put or Append
	Key      string
	Value    string
	ClientId int
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
	applied int
	c       *sync.Cond
	log  []Op
	data map[string]string

	done chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLead := kv.rf.GetState(); isLead {
		if value, ok := kv.data[args.Key]; ok {
			reply.Value = value
			reply.Err = NoErr
		} else {
			reply.Err = Fail
		}
	} else {
		reply.Err = NotLead
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, ok := kv.rf.Start(Op{Key: args.Key, Value: args.Value, Type: args.Op, Seq: args.Seq})
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
}

func (kv *KVServer) processApplyMsg(am raft.ApplyMsg) {
	kv.mu.Lock()
	cmd := am.Command.(Op)
	switch cmd.Type {
	case "Put":
		kv.data[cmd.Key] = cmd.Value
	case "Append":
		kv.data[cmd.Key] = kv.data[cmd.Key] + cmd.Value
	}
	kv.mu.Unlock()
	// update applied index
	kv.c.L.Lock()
	kv.applied = max(am.CommandIndex, kv.applied)
	kv.c.Broadcast()
	kv.c.L.Unlock()
}

func (kv *KVServer) run() {
	for {
		select {
		case <-kv.done:
			return
		case am := <-kv.applyCh:
			kv.processApplyMsg(am)
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
	DPrintf("[debug kill] server %d got killed\n", kv.me)
	kv.done <- struct {}{}
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
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.c = sync.NewCond(&sync.Mutex{})
	kv.data = make(map[string]string)
	go kv.run()

	return kv
}
