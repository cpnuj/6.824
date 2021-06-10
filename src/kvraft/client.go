package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	sync.Mutex
	leadCache int
	seq				int
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
	// You will have to modify this function.
	args := &GetArgs{Key: key}
	DPrintf("[debug app] get %s\n", key)
	for i, _ := range ck.servers {
		reply := &GetReply{}
		if ok := ck.servers[i].Call("KVServer.Get", args, reply); ok {
			switch reply.Err {
			case NoErr:
				return reply.Value
			case NotLead:
				continue
			case KeyNotExist:
				break
			}
		}
	}
	return ""
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
	args := &PutAppendArgs{Key: key, Value: value, Op: op}
	ck.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.Unlock()
	reply := &PutAppendReply{}
	for ; ; ck.leadCache = (ck.leadCache + 1) % len(ck.servers) {
		ok := ck.sendPutAppendAndWait(ck.leadCache, args, reply)
		if ok {
			return
		}
		// slow down request
		time.Sleep(time.Second)
	}
}

// sendPutAppendAndWait would init a goroutine to send PutAppend rpc
// and wait for the result within timeout.
func (ck *Clerk) sendPutAppendAndWait(to int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ch := make(chan PutAppendReply, 1)

	// goroutine for sending PutAppend rpc
	// if client timeout, this goroutine would recv ctx.Done()
	// and close the res channel
	go func() {
		if ok := ck.servers[to].Call("KVServer.PutAppend", args, reply); ok {
		  ch <- *reply
		}
	}()

	select {
	case reply := <-ch:
		return reply.Err == NoErr
	case <-time.After(time.Second):
		return false
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("[debug app] Clerk put key %s value %s\n", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("[debug app] Clerk append key %s value %s\n", key, value)
	ck.PutAppend(key, value, "Append")
}
