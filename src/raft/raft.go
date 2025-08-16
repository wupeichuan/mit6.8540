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
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Entry interface{}
	Term  int
}

type stateType int
type ServerState struct {
	follower  stateType
	candidate stateType
	leader    stateType
}

var serverstate ServerState = ServerState{
	follower:  0,
	candidate: 1,
	leader:    2,
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg
	dead      int32 // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastReceiveTime time.Time
	timeout         time.Duration
	serverstate     stateType
	ballot          int
	timeCh          chan int
	quickreCh       chan []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.serverstate == serverstate.leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	Debug(dPersist, "S%d rf.currentTerm = %d rf.votedFor = %d len(rf.log) - 1 = %d rf.log[len - 1] = %v", rf.me, rf.currentTerm, rf.votedFor, len(rf.log)-1, rf.log[len(rf.log)-1].Entry)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("d.Decode() != nil")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	haschange := false
	defer rf.persistHandle(&haschange)
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d Not Vote For S%d (args.Term < rf.currentTerm)", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		Debug(dVote, "S%d Receive From S%d (args.Term > rf.currentTerm)", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.serverstate = serverstate.follower
		haschange = true
	}

	if rf.serverstate != serverstate.follower {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			Debug(dVote, "S%d Vote For S%d (rf.votedFor == -1 || rf.votedFor == args.CandidateId)", rf.me, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			if rf.votedFor == -1 {
				rf.votedFor = args.CandidateId
				haschange = true
			}
			rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
			rf.lastReceiveTime = time.Now()
		} else {
			Debug(dVote, "S%d Not Vote For S%d (rf.votedFor == -1 || rf.votedFor == args.CandidateId)", rf.me, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	haschange := false
	defer rf.persistHandle(&haschange)
	Debug(dLog2, "S%d Got AppendEntries From S%d, rf.currentTerm = %d", rf.me, args.LeaderId, rf.currentTerm)
	if rf.serverstate == serverstate.candidate && args.Term >= rf.currentTerm {
		Debug(dLog2, "S%d Got AppendEntries From S%d (rf.serverstate == serverstate.candidate && args.Term >= rf.currentTerm)", rf.me, args.LeaderId)
		rf.serverstate = serverstate.follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		haschange = true
	} else if args.Term > rf.currentTerm {
		Debug(dLog2, "S%d Got AppendEntries From S%d (args.Term > rf.currentTerm)", rf.me, args.LeaderId)
		rf.serverstate = serverstate.follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		haschange = true
	}

	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Got AppendEntries From S%d (args.Term < rf.currentTerm)", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
	rf.lastReceiveTime = time.Now()
	if args.PrevLogIndex > len(rf.log)-1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.PrevLogIndex > len(rf.log)-1 {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			reply.ConflictIndex = rf.conflictHandle2(args.PrevLogIndex, reply.ConflictTerm)
		}
		return
	}

	Debug(dLog2, "S%d Got AppendEntries From S%d len(rf.log)-1=%d", rf.me, args.LeaderId, len(rf.log)-1)
	i := 0
	for ; i < len(args.Entries); i++ {
		if args.PrevLogIndex+1+i > len(rf.log)-1 {
			break
		}
		if args.Entries[i].Term != rf.log[args.PrevLogIndex+1+i].Term {
			rf.log = rf.log[:args.PrevLogIndex+1+i]
			haschange = true
			break
		}
	}

	if len(args.Entries[i:]) > 0 {
		rf.log = append(rf.log, args.Entries[i:]...)
		haschange = true
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Signal()
	}
}

func (rf *Raft) conflictHandle2(prevLogIndex int, conflictTerm int) int {
	low := 0
	high := prevLogIndex
	middle := (low + high) / 2
	var answer int
	for {
		if high-low > 1 {
			if rf.log[middle].Term < conflictTerm {
				low = middle
			} else if rf.log[middle].Term == conflictTerm {
				high = middle
			} else {
				high = middle - 1
			}
			middle = (low + high) / 2
		} else {
			if rf.log[high].Term >= conflictTerm {
				answer = low
				break
			} else {
				answer = high
				break
			}
		}
	}
	answer = answer + 1
	return answer
}

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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs) {
	reply := RequestVoteReply{}
	Debug(dVote, "S%d -> S%d, Sending RequestVote", rf.me, server)

	if ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	haschange := false
	if rf.serverstate != serverstate.candidate {
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if args.Term != rf.currentTerm {
		Debug(dVote, "S%d <- S%d, Not Got Vote (args.Term != rf.currentTerm)", rf.me, server)
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		Debug(dVote, "S%d <- S%d, Not Got Vote (reply.Term > rf.currentTerm)", rf.me, server)
		rf.currentTerm = reply.Term
		rf.serverstate = serverstate.follower
		rf.votedFor = -1
		haschange = true
		rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
		rf.lastReceiveTime = time.Now()
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if reply.VoteGranted {
		Debug(dVote, "S%d <- S%d, Got Vote", rf.me, server)
		rf.ballot++
		if rf.ballot >= len(rf.peers)/2+1 {
			Debug(dVote, "S%d Candidate, Achieved Majority for T%d (%d), converting to Leader", rf.me, rf.currentTerm, rf.ballot)
			rf.serverstate = serverstate.leader
			rf.nextIndex = make([]int, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					rf.nextIndex[server] = len(rf.log)
				}
			}
			rf.matchIndex = make([]int, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					rf.matchIndex[server] = 0
				}
			}
			rf.timeout = time.Duration(150) * time.Millisecond
			rf.lastReceiveTime = time.Now()
			rf.persistHandle(&haschange)
			rf.mu.Unlock()
			servers := []int{}
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					servers = append(servers, server)
				}
			}
			rf.quickreCh <- servers
		} else {
			rf.persistHandle(&haschange)
			rf.mu.Unlock()
		}
	} else {
		Debug(dVote, "S%d <- S%d, Not Got Vote", rf.me, server)
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	haschange := false
	if rf.serverstate != serverstate.leader {
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if args.Term != rf.currentTerm {
		Debug(dLog2, "S%d <- S%d (args.Term != rf.currentTerm)", rf.me, server)
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		Debug(dLog2, "S%d <- S%d (reply.Term > rf.currentTerm)", rf.me, server)
		rf.currentTerm = reply.Term
		rf.serverstate = serverstate.follower
		rf.votedFor = -1
		haschange = true
		rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
		rf.lastReceiveTime = time.Now()
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
		rf.commitHandle(rf.matchIndex[server])
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
	} else {
		rf.nextIndex[server] = rf.conflictHandle1(reply.ConflictIndex, reply.ConflictTerm)
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		servers := []int{server}
		rf.quickreCh <- servers
	}
}

func (rf *Raft) sendHeartBeat(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	haschange := false
	if rf.serverstate != serverstate.leader {
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if args.Term != rf.currentTerm {
		Debug(dLeader, "S%d <- S%d (args.Term != rf.currentTerm)", rf.me, server)
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		Debug(dLeader, "S%d <- S%d (reply.Term > rf.currentTerm)", rf.me, server)
		rf.currentTerm = reply.Term
		rf.serverstate = serverstate.follower
		rf.votedFor = -1
		haschange = true
		rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
		rf.lastReceiveTime = time.Now()
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = Max(args.PrevLogIndex, rf.matchIndex[server])
		rf.nextIndex[server] = Max(args.PrevLogIndex + 1, rf.nextIndex[server])
		rf.commitHandle(rf.matchIndex[server])
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
	} else {
		rf.nextIndex[server] = rf.conflictHandle1(reply.ConflictIndex, reply.ConflictTerm)
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		servers := []int{server}
		rf.quickreCh <- servers
	}
}

func (rf *Raft) commitHandle(matchIndex int) {
	for index := matchIndex; index > rf.commitIndex; index-- {
		if rf.log[index].Term < rf.currentTerm {
			break
		}
		if rf.log[index].Term > rf.currentTerm {
			panic("rf.log[index].Term > rf.currentTerm")
		}
		majority := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				if rf.matchIndex[i] >= index {
					majority++
				}
			}
		}
		if majority >= len(rf.peers)/2+1 {
			rf.commitIndex = index
			rf.cond.Signal()
			Debug(dCommit, "S%d commitIndex %d %v", rf.me, rf.commitIndex, rf.log[rf.commitIndex].Entry)
			break
		}
	}
}

func (rf *Raft) conflictHandle1(conflictIndex int, conflictTerm int) int {
	if conflictTerm == -1 {
		return conflictIndex
	}

	low := 1
	high := len(rf.log)
	middle := high / 2
	answer := -1
	for {
		if high-low > 1 {
			if rf.log[middle].Term < conflictTerm {
				low = middle + 1
			} else if rf.log[middle].Term == conflictTerm {
				low = middle
			} else {
				high = middle
			}
			middle = (low + high) / 2
		} else {
			if rf.log[low].Term <= conflictTerm {
				answer = high
				break
			} else {
				answer = low
				break
			}
		}
	}

	if rf.log[answer-1].Term == conflictTerm {
		return answer
	} else {
		return conflictIndex
	}
}

func (rf *Raft) persistHandle(haschange *bool) {
	if *haschange {
		rf.persist()
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	haschange := false
	if rf.serverstate != serverstate.leader {
		rf.persistHandle(&haschange)
		rf.mu.Unlock()
		return -1, -1, false
	}

	rf.log = append(rf.log, Log{
		Entry: command,
		Term:  rf.currentTerm,
	})

	length := len(rf.log) - 1
	currentTerm := rf.currentTerm
	haschange = true
	rf.persistHandle(&haschange)
	rf.mu.Unlock()
	servers := []int{}
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			servers = append(servers, server)
		}
	}
	rf.quickreCh <- servers
	return length, currentTerm, true
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait()
			if rf.killed() {
				return
			}
		}
		m := make([]ApplyMsg, rf.commitIndex-rf.lastApplied)
		for i := 0; rf.lastApplied < rf.commitIndex; i++ {
			rf.lastApplied++
			m[i] = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Entry,
				CommandIndex: rf.lastApplied,
			}
			Debug(dCommit, "S%d Apply %d %v", rf.me, rf.lastApplied, rf.log[rf.lastApplied].Entry)
		}
		rf.mu.Unlock()
		for _, i := range m {
			rf.applyCh <- i
		}
	}

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) timeHandle() {
	for rf.killed() == false {
		time.Sleep(time.Duration(2) * time.Millisecond)
		rf.mu.Lock()
		if time.Since(rf.lastReceiveTime).Milliseconds() > rf.timeout.Milliseconds() {
			rf.mu.Unlock()
			rf.timeCh <- -1
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) timeoutHandle() {
	rf.mu.Lock()
	if rf.serverstate == serverstate.follower {
		Debug(dVote, "S%d Follower, Starting Election", rf.me)
		rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
		rf.lastReceiveTime = time.Now()
		rf.serverstate = serverstate.candidate
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.ballot = 0
		rf.ballot++
		haschange := true
		rf.persistHandle(&haschange)

		Term := rf.currentTerm
		LastLogIndex := len(rf.log) - 1
		LastLogTerm := rf.log[len(rf.log)-1].Term
		rf.mu.Unlock()
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				go rf.sendRequestVote(server, RequestVoteArgs{
					Term:         Term,
					CandidateId:  rf.me,
					LastLogIndex: LastLogIndex,
					LastLogTerm:  LastLogTerm,
				})
			}
		}
	} else if rf.serverstate == serverstate.candidate {
		Debug(dVote, "S%d Candidate, Not Achieved Majority for T%d (%d) continuing to elect", rf.me, rf.currentTerm, rf.ballot)
		rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
		rf.lastReceiveTime = time.Now()
		rf.currentTerm++
		rf.ballot = 0
		rf.ballot++
		haschange := true
		rf.persistHandle(&haschange)

		Term := rf.currentTerm
		LastLogIndex := len(rf.log) - 1
		LastLogTerm := rf.log[len(rf.log)-1].Term
		rf.mu.Unlock()
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				go rf.sendRequestVote(server, RequestVoteArgs{
					Term:         Term,
					CandidateId:  rf.me,
					LastLogIndex: LastLogIndex,
					LastLogTerm:  LastLogTerm,
				})
			}
		}
	} else {
		Debug(dLeader, "S%d Leader, sendAppendEntries in T%d", rf.me, rf.currentTerm)
		rf.timeout = time.Duration(150) * time.Millisecond
		rf.lastReceiveTime = time.Now()

		length := len(rf.log) - 1
		Entries := make([]Log, len(rf.log))
		copy(Entries, rf.log)
		Term := rf.currentTerm
		PrevLogIndex := make([]int, len(rf.peers))
		PrevLogTerm := make([]int, len(rf.peers))
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				PrevLogIndex[server] = rf.nextIndex[server] - 1
				PrevLogTerm[server] = rf.log[rf.nextIndex[server]-1].Term
			}
		}
		LeaderCommit := rf.commitIndex
		rf.mu.Unlock()
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				if length >= PrevLogIndex[server] {
					Debug(dLeader, "S%d Leader, sendAppendEntries in T%d to S%d PrevLogIndex=%d", rf.me, rf.currentTerm, server, PrevLogIndex[server])
					go rf.sendAppendEntries(server, AppendEntriesArgs{
						Term:         Term,
						LeaderId:     rf.me,
						PrevLogIndex: PrevLogIndex[server],
						PrevLogTerm:  PrevLogTerm[server],
						Entries:      Entries[PrevLogIndex[server]+1:],
						LeaderCommit: LeaderCommit,
					})
				} else {
					go rf.sendHeartBeat(server, AppendEntriesArgs{
						Term:         Term,
						LeaderId:     rf.me,
						PrevLogIndex: PrevLogIndex[server],
						PrevLogTerm:  PrevLogTerm[server],
						LeaderCommit: LeaderCommit,
					})
				}
			}
		}
	}
}

func (rf *Raft) quickreHandle(servers []int) {
	rf.mu.Lock()
	Debug(dLeader, "S%d Leader, sendAppendEntries(quickreHandle) in T%d", rf.me, rf.currentTerm)
	length := len(rf.log) - 1
	Entries := make([]Log, len(rf.log))
	copy(Entries, rf.log)
	Term := rf.currentTerm
	PrevLogIndex := make([]int, len(servers))
	PrevLogTerm := make([]int, len(servers))
	for server := 0; server < len(servers); server++ {
		PrevLogIndex[server] = rf.nextIndex[servers[server]] - 1
		PrevLogTerm[server] = rf.log[rf.nextIndex[servers[server]]-1].Term
	}
	LeaderCommit := rf.commitIndex
	rf.mu.Unlock()
	for server := 0; server < len(servers); server++ {
		if servers[server] != rf.me {
			if length >= PrevLogIndex[server] {
				Debug(dLeader, "S%d Leader, sendAppendEntries(quickreHandle) in T%d to S%d PrevLogIndex=%d", rf.me, rf.currentTerm, servers[server], PrevLogIndex[server])
				go rf.sendAppendEntries(servers[server], AppendEntriesArgs{
					Term:         Term,
					LeaderId:     rf.me,
					PrevLogIndex: PrevLogIndex[server],
					PrevLogTerm:  PrevLogTerm[server],
					Entries:      Entries[PrevLogIndex[server]+1:],
					LeaderCommit: LeaderCommit,
				})
			} else {
				go rf.sendHeartBeat(servers[server], AppendEntriesArgs{
					Term:         Term,
					LeaderId:     rf.me,
					PrevLogIndex: PrevLogIndex[server],
					PrevLogTerm:  PrevLogTerm[server],
					LeaderCommit: LeaderCommit,
				})
			}
		}
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.cond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// Your code here (2A)
	// Check if a leader election should be started.
	for rf.killed() == false {
		select {
		case <-rf.timeCh : 
			go rf.timeoutHandle()
		case servers := <-rf.quickreCh :
			go rf.quickreHandle(servers)
		case <-time.After(time.Duration(10) * time.Millisecond) :
		}
	}
	if len(rf.timeCh) > 0 {
		<-rf.timeCh
	}
	for len(rf.quickreCh) > 0 {
		<-rf.quickreCh
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.serverstate = serverstate.follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastReceiveTime = debugStart
	rf.timeout = time.Duration(180+(rand.Int63()%100)) * time.Millisecond
	rf.ballot = 0
	rf.timeCh = make(chan int)
	rf.quickreCh = make(chan []int, len(rf.peers))

	// start ticker goroutine to start elections
	go rf.timeHandle()
	go rf.ticker()
	go rf.apply()

	return rf
}
