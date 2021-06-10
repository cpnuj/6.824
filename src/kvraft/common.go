package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	KeyNotExist Err = "key not exist"
	NotLead         = "is not lead"
	TimeOut					= "timeout"
	NoErr           = "success"
	Fail            = "fail"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Seq   int
}

type PutAppendReply struct {
	Err  Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
