package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// some const value for implementation
//
const (
	DefaultSliceLength = 100
	DefaultTermBottom  = 300
	DefaultTermTop     = 500
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A log entry implementation
//
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg
	cond      *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	state       int
	votedFor    *labrpc.ClientEnd
	log         []LogEntry
	commitIndex int
	lastApplied int
	votes       int
	sh          chan int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

//
// Raft's server state
//
const (
	FOLLOWER = iota
	LEADER
	CANDIDATE
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isLeader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// as specialized by paper Figure 2
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// as specialized by paper Figure 2
	Term        int
	VoteGranted bool
}

// TODO:
// Election restriction implementation
// paper 5.4.1
//
func (rf *Raft) electionRestriction(args *RequestVoteArgs) bool {
	if args.LastLogTerm == rf.currentTerm {
		return args.LastLogIndex >= len(rf.log)-1
	}
	return args.LastLogTerm > rf.currentTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("node %d(term: %d) receive request votes from %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	// TODO:
	//    better lock statement
	// 		make more clean code

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// refuse outdated request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// we should enhance once we found we are outdated

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = nil
	}
	rf.sh <- 1

	if rf.votedFor == nil || rf.votedFor == rf.peers[args.CandidateId] && rf.electionRestriction(args) {
		rf.votedFor = rf.peers[args.CandidateId]
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
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

//
// Invoked by leader to replicate log entries, also used as heartbeat
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO
	//   clean code

	// DPrintf("node %d(%d) receive append entry from leader %d(%d)\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	// refuse outdated request
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.state = FOLLOWER
		rf.votes = 0
	}
	rf.sh <- 1

	// return false if log doesn't matches PrevLogTerm and PrevLogIndex
	if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if an existing entry conflicts with a new one, delete the existing entry and all those follow it
	// according to paper 5.3
	var i int
	for i = 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i+1 < len(rf.log) && rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
			break
		}
	}
	if i != len(args.Entries) {
		rf.log = rf.log[:args.PrevLogTerm+i+1]
	}
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Signal()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

// send Append entries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if !isLeader {
		return index, term, isLeader
	}

	// Your code here (2B).

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.mu = sync.Mutex{}
	rf.votedFor = nil
	rf.votes = 0
	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rand.Seed(time.Now().UnixNano() + int64(me))

	// ensure the first index is 1
	rf.log = make([]LogEntry, 1, DefaultSliceLength)
	rf.sh = make(chan int, 10)

	srv := labrpc.MakeService(rf)
	rpcs := labrpc.MakeServer()
	rpcs.AddService(srv)

	// apply the command when commitIndex > lastA
	go func() {
		for {
			rf.mu.Lock()
			for rf.commitIndex == rf.lastApplied {
				rf.cond.Wait()
			}

			if rf.commitIndex < rf.lastApplied {
				DPrintf("some error happened for rf.commitIndex < rf.lastApplied")
			}

			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				// we can go routine, client can tell the difference through msg.Index
				rf.lastApplied++
				index := rf.lastApplied
				command := rf.log[rf.lastApplied].Command
				go func() {
					app := ApplyMsg{Index: index, Command: command}
					rf.applyCh <- app
				}()
			}

			rf.mu.Unlock()
		}
	}()

	go func() {
		for {
			rf.mu.Lock()
			// must guarantee the lock is closed in the following do* function
			switch rf.state {
			case FOLLOWER:
				rf.doFollower()
			case LEADER:
				rf.doLeader()
			case CANDIDATE:
				rf.doCandidate()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) doFollower() {
	rf.mu.Unlock()
	select {
	case <-time.After(time.Duration(rand.Intn(DefaultTermTop-DefaultTermBottom)+DefaultTermBottom) * time.Millisecond):
		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.mu.Unlock()
		return
	case <-rf.sh:
		return
	}
}

func (rf *Raft) doLeader() {
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	// send heartbeat or heartbeat to all peers

	go func() {
		for {
			// not the leader, don't send out append
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				break
			}
			for i := 0; i < len(rf.peers) && rf.state == LEADER; i++ {
				rf.mu.Unlock()
				if i == rf.me {
					rf.mu.Lock()
					continue
				}
				i := i
				go func() {
					// TODO:
					//    now only support heartbeat, it's likely to combine heartbeat and AppendEntries together
					rf.mu.Lock()
					var entries []LogEntry
					if rf.nextIndex[i] < len(rf.log) {
						entries = rf.log[rf.nextIndex[i]:]
					} else {
						entries = make([]LogEntry, 0)
					}
					args := &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[i] - 1,
						rf.log[rf.nextIndex[i]-1].Term, entries, rf.commitIndex}
					reply := &AppendEntriesReply{}
					rf.mu.Unlock()

					ok := rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if ok {
						if !reply.Success {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = FOLLOWER
								rf.sh <- 1
								rf.votedFor = nil
								return
							} else {
								rf.nextIndex[i]--
							}
						} else {
							rf.nextIndex[i] = rf.nextIndex[i] + len(args.Entries)
							rf.matchIndex[i] = rf.nextIndex[i] - 1
							k := len(rf.log) - 1
							for rf.log[k].Term == rf.currentTerm {
								sum := 0
								for j := 0; j < len(rf.peers); j++ {
									if rf.matchIndex[j] >= k {
										sum++
										if sum >= len(rf.peers)/2 {
											goto COMMIT_IT
										}
									}
								}
								k--
							}
							return

						COMMIT_IT:
							rf.commitIndex = k
							rf.cond.Signal()
							return
						}
					}
				}()
				rf.mu.Lock()
			}
			rf.mu.Unlock()
			time.Sleep(150 * time.Millisecond)
		}
	}()

	select {
	case <-rf.sh:
		return
	}
}

// STATE: Candidate
func (rf *Raft) doCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.peers[rf.me]
	rf.votes = 1

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {

		i := i
		if i == rf.me {
			continue
		}
		go func() {
			rf.mu.Lock()
			args := &RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
			reply := &RequestVoteReply{}
			rf.mu.Unlock()

			ok := rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = nil
					rf.sh <- 1
					return
				} else if reply.Term < rf.currentTerm {
					return
				}

				if reply.VoteGranted && rf.state == CANDIDATE {
					// DPrintf("ok for %d to %d\n", i, rf.me)
					rf.votes++
					if rf.votes >= len(rf.peers)/2+1 {
						// DPrintf("%d(%d) becomes leader", rf.me, rf.currentTerm)
						rf.state = LEADER
						rf.sh <- 1
					}

					return
				}
			}
		}()
	}

	select {
	case <-time.After(time.Duration(rand.Intn(DefaultTermTop-DefaultTermBottom)+DefaultTermBottom) * time.Millisecond):
		return
	case <-rf.sh:
		return
	}
}
