package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

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

// import "fmt"
// import "time"
// import "sync/atomic"
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

type Entry struct {
	Command interface{}
	Term    int
}

// timeout settings (ms)
const (
	MaxTimeout int = 1000
	MinTimeout int = 500
)

// indicate Raft node's role
const (
	LEADER    = 1
	FOLLOWER  = 2
	CANDIDATE = 3
)

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
	applyCh chan ApplyMsg

	role int // peer's role. leader, follower or candidate.
	term int // peer's current term

	hbch chan int // channel to receive heartbreak

	// fileds for election timeout
	// timeout is must between 250 - 500 ms
	timeout time.Duration
	timer   *time.Timer

	// log entries
	log []Entry
	// number of agreements on log index
	nagree []int

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	if rf.killed() {
		return rf.term, false
	}
	term, isleader := rf.term, rf.role == LEADER
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
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("node: %d, term: %d receive vote request from node: %d, term: %d\n", rf.me, rf.term, args.CandidateID, args.Term)
	reply.Term = rf.term

	// candidate's term <= peer's term
	// NOTE: if candidate's term == peer's term,
	// means this peer has voted in this term
	if rf.term >= args.Term {
		reply.VoteGranted = -1
		return
	}

	// DPrintf("node last term: %d arg last term: %d, node last applied: %d arg last applied %d", rf.log[rf.lastApplied].Term, args.LastLogTerm, rf.lastApplied, args.LastLogIndex)

	// check if the candidate's log is more up-to-date
	if rf.log[rf.lastApplied].Term != args.LastLogTerm {
		if rf.log[rf.lastApplied].Term > args.LastLogTerm {
			reply.VoteGranted = -1
		}
	} else if rf.lastApplied > args.LastLogIndex {
		reply.VoteGranted = -1
	} else {
		reply.VoteGranted = 1
	}

	// the candidate's term > peer's term
	// update the peer's
	// although the candidate's log is not more up-to-date
	rf.term = args.Term
	reply.Term = args.Term

	// If the peer receives the RequestVote RPC,
	// then trigger the heartbreak channel
	rf.hbch <- 1
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

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Succ int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("node%d term %d log len: %d", rf.me, rf.term, len(rf.log))
	// DPrintf("node%d receives append entries %d", rf.me, len(args.Entries))
	reply.Term = rf.term

	// leader term < peer term
	if args.Term < rf.term {
		DPrintf("node%d term%d, leader term%d", rf.me, rf.term, args.Term)
		reply.Succ = -1
		return
	}

	// Update peer's term
	rf.term = args.Term

	// DPrintf("args: prev log index: %d, prev log term: %d", args.PrevLogIndex, args.Term)
	if args.PrevLogIndex <= rf.lastApplied {
		// DPrintf("node%d term: %d", rf.me, rf.log[args.PrevLogIndex].Term)
	}

	// peer's log doesn't contain entry at prevLogIndex whose term
	// matches prevLogTerm
	if args.PrevLogIndex > rf.lastApplied {
		DPrintf("arg prev log index: %d, node%d last apply", args.PrevLogIndex, rf.lastApplied)
		reply.Succ = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("In index %d: node%d term %d, leader term %d", args.PrevLogIndex, rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.Succ = -1
		return
	}

	if len(args.Entries) != 0 {
		// delete entries after prevLogIndex
		// apply new entries received from leader
		rf.mu.Lock()
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		rf.lastApplied = rf.lastApplied + len(args.Entries)
		rf.mu.Unlock()
	}

	// update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.mu.Lock()
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.lastApplied)
		// send applyMsg with new commit
		for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = rf.log[i].Command
			applyMsg.CommandIndex = i
			rf.applyCh <- applyMsg
		}
		rf.mu.Unlock()
	}

	reply.Succ = 1

	// If the peer receives this AppendEntries RPC,
	// then trigger its heartbreak channel
	rf.hbch <- 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("node%d send heartbreak to node%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// Your code here (2B).

	// fmt.Println(command)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term := rf.lastApplied+1, rf.term
	_, isLeader := rf.GetState()

	if isLeader == true {
		// DPrintf("node%d receives command %d", rf.me, len(rf.log))

		rf.lastApplied++

		rf.nagree = append(rf.nagree, 1)
		// DPrintf("nagree:%d", len(rf.nagree))

		entry := Entry{command, rf.term}
		rf.log = append(rf.log, entry)
		// DPrintf("log:%d", len(rf.log))
	}

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
	// DPrintf("node%d has been killed.\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runAsFollower() {
	// reset timer
	rf.timer = time.NewTimer(rf.timeout)
	defer rf.timer.Stop()

	// if receive heartbeat, reset timer
	// if timeout, return
	for {
		if rf.killed() {
			return
		}
		select {
		case <-rf.hbch:
			if !rf.timer.Stop() {
				<-rf.timer.C
			}
			// DPrintf("node%d receives heartbreak\n", rf.me)
			rf.timer.Reset(rf.timeout)
		case <-rf.timer.C:
			// DPrintf("node%d timeout\n", rf.me)
			rf.role = CANDIDATE
			return
		}
	}
}

func (rf *Raft) runAsCandidate() {
	rf.mu.Lock()
	rf.term++
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastApplied,
		LastLogTerm:  rf.log[rf.lastApplied].Term,
	}
	DPrintf("node%d becomes candidate in term%d", rf.me, rf.term)
	rf.mu.Unlock()

	reply := make([]RequestVoteReply, len(rf.peers))
	vch, votes := make(chan int, len(rf.peers)), 1
	// send request vote concurrently
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			if ok := rf.sendRequestVote(server, &args, &reply[server]); ok {
				vch <- server
			} else {
				vch <- -1
			}
		}(i)
	}

	// reset timer
	rf.timer = time.NewTimer(rf.timeout)
	defer rf.timer.Stop()

	for {
		select {
		// receive vote
		case server := <-vch:
			if server < 0 {
				continue
			}
			if reply[server].VoteGranted == 1 {
				votes++
			} else if reply[server].Term > rf.term {
				rf.term = reply[server].Term
				rf.role = FOLLOWER
				return
			}
			if votes >= (len(rf.peers)+1)/2 {
				rf.role = LEADER
				return
			}
		// receive heartbreak
		case <-rf.hbch:
			rf.role = FOLLOWER
			// DPrintf("node%d lose the election in term%da", rf.me, rf.term)
			return
		// timer elapse
		case <-rf.timer.C:
			rf.role = CANDIDATE
			// DPrintf("node%d term%d election timeout", rf.me, rf.term)
			return
		}
	}
}

const HB_PERIOD = 100 // heartbreak interval

// goroutine for Leader to send append entries RPC
func (rf *Raft) appendEntriesSender(i int) {
	args, reply := AppendEntriesArgs{}, AppendEntriesReply{}
	args.Term = rf.term
	args.LeaderID = rf.me

	for {
		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}

		rf.mu.Lock()
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		if rf.lastApplied >= rf.nextIndex[i] {
			args.Entries = rf.log[rf.nextIndex[i]:]
		} else {
			args.Entries = args.Entries[:0]
		}
		rf.mu.Unlock()

		if ok := rf.sendAppendEntries(i, &args, &reply); ok {
			if reply.Succ == 1 {
				begin := rf.nextIndex[i]
				rf.nextIndex[i] += len(args.Entries)
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				end := rf.matchIndex[i]
				// Update nagree asyn
				// TODO remove this goroutine, checkCommit is responsible for this job
				go func(begin, end int) {
					// TODO use rwlocker to replace mutex
					rf.mu.Lock()
					defer rf.mu.Unlock()
					for i := begin; i <= end; i++ {
						rf.nagree[i]++
					}
				}(begin, end)
			} else {
				// the reason here why not use leader's term is that
				// another sender goroutine may has modify the currentTerm
				// that would result the rf.nextIndex[i]-- continue
				if reply.Term > args.Term && reply.Term > rf.term {
					// DPrintf("node%d's term%d > leader's term%d", i, reply.Term, rf.term)
					rf.term = reply.Term
				} else {
					rf.nextIndex[i]--
				}
			}
		} else {
			// DPrintf("append entries to node%d fail", i)
		}

		time.Sleep(HB_PERIOD * time.Millisecond)
	}

}

func (rf *Raft) checkCommit(quit chan bool) {
	const checkInterval = 500               // check commit period
	var nMajority = (len(rf.peers) + 1) / 2 // n to achieve majority agreement

	applyMsg := ApplyMsg{}
	applyMsg.CommandValid = true

	timer := time.NewTimer(checkInterval * time.Millisecond)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		timer.Reset(checkInterval * time.Millisecond)

		select {
		case <-quit:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			// TODO use rwlocker to replace mutex
			rf.mu.Lock()
			// If current leader has not served any request
			// it can not commit the log in the previous term
			if rf.log[rf.lastApplied].Term == rf.term {
				for i := rf.lastApplied; i > rf.commitIndex; i-- {
					if rf.nagree[i] >= nMajority {
						for j := rf.commitIndex + 1; j <= i; j++ {
							applyMsg.Command = rf.log[j].Command
							applyMsg.CommandIndex = j
							rf.applyCh <- applyMsg
						}
						rf.commitIndex = i
						break
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) runAsLeader() {
	DPrintf("node%d becomes leader in term%d.\n", rf.me, rf.term)

	// init nextIndex and matchIndex
	rf.mu.Lock()
	currentTerm := rf.term
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastApplied + 1
	}
	// reinitialize nagree
	rf.nagree = make([]int, rf.lastApplied+1)
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.appendEntriesSender(i)
	}

	// goroutine for checking commit status
	quitCheck := make(chan bool)
	go rf.checkCommit(quitCheck)

	for rf.term == currentTerm {
		time.Sleep(2 * HB_PERIOD * time.Millisecond)
	}

	quitCheck <- true

	rf.role = FOLLOWER

	DPrintf("node%d stops to be leader in term%d.\n", rf.me, currentTerm)
	return
}

// Raft node main goroutine
func (rf *Raft) begin() {
	// fmt.Println(rf.me, "begin")
	for {
		if rf.killed() {
			return
		}
		switch rf.role {
		case FOLLOWER:
			rf.runAsFollower()
		case CANDIDATE:
			rf.runAsCandidate()
		case LEADER:
			rf.runAsLeader()
		}
	}
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
	rf.applyCh = applyCh

	rf.term = 0
	rf.role = FOLLOWER
	rf.hbch = make(chan int)
	rf.timeout = time.Duration(rangeRand(MaxTimeout, MinTimeout)) * time.Millisecond

	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{Term: 0}
	rf.commitIndex, rf.lastApplied = 0, 0
	rf.nagree = make([]int, 1)

	DPrintf("Init node%d with timeout %dms\n", rf.me, rf.timeout/time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.begin()

	return rf
}
