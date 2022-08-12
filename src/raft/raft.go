package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Leader int = iota
	Candidate
	Follower
)

const nonVote = -1

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	term  int
	state int

	voteFor  int
	votesAck map[int]struct{}

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	heartbeatTimeout int

	requestVoteArgsCh    chan *RequestVoteArgs
	requestVoteReplyCh   chan *RequestVoteReply
	appendEntriesArgsCh  chan *AppendEntriesArgs
	appendEntriesReplyCh chan *AppendEntriesReply

	getStateReqCh chan *GetStateRequest
}

type GetStateRequest struct {
	respCh chan *GetStateResponse
}

type GetStateResponse struct {
	Term     int
	IsLeader bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	req := &GetStateRequest{make(chan *GetStateResponse)}
	rf.getStateReqCh <- req
	resp := <-req.respCh
	term, isleader = resp.Term, resp.IsLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From int
	Term int

	ReplyCh chan *RequestVoteReply
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	From int
	Term int
	Succ bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	args.ReplyCh = make(chan *RequestVoteReply)
	rf.requestVoteArgsCh <- args
	*reply = *(<-args.ReplyCh)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	From int
	Term int

	ReplyCh chan *AppendEntriesReply
}

type AppendEntriesReply struct {
	From int
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	args.ReplyCh = make(chan *AppendEntriesReply)
	rf.appendEntriesArgsCh <- args
	*reply = *(<-args.ReplyCh)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) becomeFollower() {
	rf.state = Follower
	rf.voteFor = nonVote
	rf.electionElapsed = 0
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.electionElapsed = 0

	rf.votesAck = make(map[int]struct{})
	rf.votesAck[rf.me] = struct{}{}

	// broadcast vote request
	args := &RequestVoteArgs{From: rf.me, Term: rf.term}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		i := i
		go func() {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, args, reply); ok {
				rf.requestVoteReplyCh <- reply
			}
		}()
	}

	DPrintf("[peer %d] become candidate in term %d", rf.me, rf.term)
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.heartbeatTimeout = 0

	rf.sendHeartbeats()

	DPrintf("[peer %d] become leader in term %d", rf.me, rf.term)
}

func (rf *Raft) sendHeartbeats() {
	args := &AppendEntriesArgs{
		From: rf.me,
		Term: rf.term,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		i := i
		go func() {
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(i, args, reply); ok {
				rf.appendEntriesReplyCh <- reply
			}
		}()
	}
}

func (rf *Raft) tick() {
	switch rf.state {
	case Follower, Candidate:
		rf.electionElapsed++
		if rf.electionElapsed > rf.electionTimeout {
			rf.term++
			rf.becomeCandidate()
		}
	case Leader:
		rf.heartbeatElapsed++
		if rf.heartbeatElapsed > rf.heartbeatTimeout {
			rf.sendHeartbeats()
			rf.heartbeatElapsed = 0
		}
	}
}

func (rf *Raft) handleRequestVoteArgs(args *RequestVoteArgs) {
	reply := &RequestVoteReply{From: rf.me}
	defer func() {
		args.ReplyCh <- reply
	}()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Succ = false
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.becomeFollower()
	}
	// args.Term == rf.term
	reply.Term = rf.term
	if rf.state == Follower && rf.voteFor == nonVote {
		rf.voteFor = args.From
		reply.Succ = true
	} else {
		reply.Succ = false
	}
}

func (rf *Raft) handleRequestVoteReply(reply *RequestVoteReply) {
	if reply.Term < rf.term {
		return
	}
	if reply.Term > rf.term {
		rf.term = reply.Term
		rf.becomeFollower()
		return
	}
	if rf.state == Candidate && reply.Succ {
		rf.votesAck[reply.From] = struct{}{}
		if len(rf.votesAck) > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) handleAppendEntriesArgs(args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{From: rf.me}
	defer func() {
		args.ReplyCh <- reply
	}()

	if args.Term < rf.term {
		reply.Term = rf.term
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.becomeFollower()
	}
	reply.Term = rf.term
	switch rf.state {
	case Follower:
		rf.electionElapsed = 0
	case Candidate:
		rf.becomeFollower()
	case Leader:
		log.Fatalf("[peer %d] two leaders in term %d", rf.me, rf.term)
	}
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term < rf.term {
		return
	}
	if reply.Term > rf.term {
		rf.becomeFollower()
	}
}

func (rf *Raft) run() {
	tickDuration := 10 * time.Millisecond
	timer := time.NewTimer(tickDuration)

	for {
		select {
		// time module
		case <-timer.C:
			rf.tick()
			timer.Reset(tickDuration)

		// rpc request and reply handler
		case args := <-rf.requestVoteArgsCh:
			rf.handleRequestVoteArgs(args)
		case reply := <-rf.requestVoteReplyCh:
			rf.handleRequestVoteReply(reply)

		case args := <-rf.appendEntriesArgsCh:
			rf.handleAppendEntriesArgs(args)
		case reply := <-rf.appendEntriesReplyCh:
			rf.handleAppendEntriesReply(reply)

		// api msg handler
		case req := <-rf.getStateReqCh:
			req.respCh <- &GetStateResponse{
				Term:     rf.term,
				IsLeader: rf.state == Leader,
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.term = 0
	rf.becomeFollower()

	// election timeout range 200ms to 400ms
	// our ticker should tick every 10ms
	rf.electionElapsed = 0
	rf.electionTimeout = rand.Intn(20) + 20
	// heartbeat timeout every 100ms
	rf.heartbeatTimeout = 10

	rf.requestVoteArgsCh = make(chan *RequestVoteArgs, 100)
	rf.requestVoteReplyCh = make(chan *RequestVoteReply, 100)
	rf.appendEntriesArgsCh = make(chan *AppendEntriesArgs, 100)
	rf.appendEntriesReplyCh = make(chan *AppendEntriesReply, 100)

	rf.getStateReqCh = make(chan *GetStateRequest, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
