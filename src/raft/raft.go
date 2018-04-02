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
	"bytes"
	"encoding/gob"
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
	UseSnapshot bool
	Snapshot    []byte
	Term        int
}

//
// A log entry implementation
//
type LogEntry struct {
	Index   int
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
	random    *rand.Rand

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	state       int
	votedFor    int
	Log         []LogEntry
	commitIndex int
	lastApplied int
	votes       int
	sh          chan int

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == LEADER

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.Log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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
	// as specialized by paper Figure 2
	Term        int
	VoteGranted bool
}

// Election restriction implementation
// paper 5.4.1
func (rf *Raft) electionRestriction(args *RequestVoteArgs) bool {
	if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term {
		return args.LastLogIndex >= len(rf.Log)-1
	}
	return args.LastLogTerm > rf.Log[len(rf.Log)-1].Term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	needPersist := false
	rf.mu.Lock()
	defer func() {
		if needPersist {
			rf.persist()
		}
		rf.mu.Unlock()
	}()

	// refuse outdated request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// we should become follower and become up to date
	// once we found we are outdated
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		needPersist = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.electionRestriction(args) {
		rf.sh <- 1
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		needPersist = true
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

	// accelerated log backtracking optimization
	// paper page8 upper left
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	needPersist := false
	rf.mu.Lock()
	defer func() {
		if needPersist {
			rf.persist()
		}
		rf.mu.Unlock()
	}()

	// refuse out of date request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// wake up and reset the timer
	rf.sh <- 1

	// we should become follower and become up to date
	// once we found we are outdated
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		needPersist = true
	}

	// return false if log doesn't matches PrevLogTerm and PrevLogIndex
	if len(rf.Log) == 1 || rf.Log[len(rf.Log)-1].Index < args.PrevLogIndex {
		if len(rf.Log) == 1 {
			reply.ConflictIndex = 1
		} else {
			reply.ConflictIndex = rf.Log[len(rf.Log)-1].Index
		}
		reply.ConflictTerm = -1
		reply.Term = rf.currentTerm
		reply.Success = false
		needPersist = true
		return
	}

	var baseIndex int
	if len(rf.Log) > 1 {
		baseIndex = rf.Log[1].Index
	} else {
		baseIndex = 1
	}
	if rf.Log[args.PrevLogIndex-baseIndex+1].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.Log[args.PrevLogIndex-baseIndex+1].Term
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.Log[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}

		rf.Log = rf.Log[:args.PrevLogIndex-baseIndex+1]
		reply.Term = rf.currentTerm
		reply.Success = false
		needPersist = true
		return
	}

	/*
		If the follower has all the entries the leader sent, the follower
		MUST NOT truncate its log. Any elements following the entries sent
		by the leader MUST be kept. This is because we could be receiving
		an outdated AppendEntries RPC from the leader, and truncating the
		log would mean “taking back” entries that we may have already told
		the leader that we have in our log.
	*/
	// according to student's advice
	var i int
	for i = 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex-baseIndex+i+2 >= len(rf.Log) || rf.Log[args.PrevLogIndex-baseIndex+i+2].Term != args.Entries[i].Term {
			break
		}
	}

	if args.PrevLogIndex-baseIndex+i+2 < len(rf.Log) {
		rf.Log = rf.Log[:args.PrevLogIndex-baseIndex+i+2]
		needPersist = true
	}

	for ; i < len(args.Entries); i++ {
		rf.Log = append(rf.Log, args.Entries[i])
		needPersist = true
	}

	// commit according to args.LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		/*
			The min in the final step (#5) of AppendEntries is necessary, and it needs to be computed
			with the index of the last new entry. It is not sufficient to simply have the function
			that applies things from your log between lastApplied and commitIndex stop when it reaches
			the end of your log. This is because you may have entries in your log that differ from the
			leader’s log after the entries that the leader sent you (which all match the ones in your log).
			 Because #3 dictates that you only truncate your log if you have conflicting entries, those
			 won’t be removed, and if leaderCommit is beyond the entries the leader sent you,
			 you may apply incorrect entries.

			 from students-guide-to-raft
		*/
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.cond.Signal()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	return
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

	index := rf.Log[len(rf.Log)-1].Index + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if !isLeader {
		return index, term, isLeader
	}

	rf.Log = append(rf.Log, LogEntry{Index: index, Term: rf.currentTerm, Command: command})
	rf.persist()
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
// Raft can be given a log index, discard the entries before that index,
// and continue operating while storing only log entries after that
// index
//
func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// prepend a 0 index
	rf.Log = append([]LogEntry{rf.Log[0]}, rf.Log[index:]...)
	rf.persister.SaveSnapshot(snapshot)
	rf.persist()
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

	rf.currentTerm = 0
	rf.mu = sync.Mutex{}
	rf.votedFor = -1
	rf.votes = 0
	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	s := rand.NewSource(time.Now().UnixNano() + int64(me)*100)
	rf.random = rand.New(s)

	// ensure the first index is 1
	rf.Log = make([]LogEntry, 0, DefaultSliceLength)
	rf.Log = append(rf.Log, LogEntry{Index: 0, Term: 0, Command: nil})
	rf.sh = make(chan int, 10)

	// apply the command when commitIndex > lastA
	go func() {
		for {
			rf.mu.Lock()
			for rf.commitIndex == rf.lastApplied {
				rf.cond.Wait()
			}

			var baseIndex int
			if len(rf.Log) > 1 {
				baseIndex = rf.Log[1].Index
			} else {
				baseIndex = 1
			}
			for {
				i := rf.lastApplied + 1
				if i > rf.commitIndex {
					break
				}
				rf.lastApplied++
				log := rf.Log[rf.lastApplied-baseIndex+1]
				// assert(log.Index == rf.lastApplied)
				app := ApplyMsg{Index: log.Index, Command: log.Command, Term: log.Term}
				// TODO
				//   how to make sure the applyCh is no-blocking
				rf.applyCh <- app
			}

			rf.mu.Unlock()
		}
	}()

	// state machine
	go func() {
		for {
			rf.mu.Lock()
			rf.sh = make(chan int, 10)
			// must guarantee the lock is closed in the following do_* function
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

// state: follower
func (rf *Raft) doFollower() {
	rf.mu.Unlock()
	select {
	case <-time.After(time.Duration(rf.random.Intn(DefaultTermTop-DefaultTermBottom)+DefaultTermBottom) * time.Millisecond):
		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.mu.Unlock()
		return
	case <-rf.sh:
		return
	}
}

// state: leader
func (rf *Raft) doLeader() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.Log[len(rf.Log)-1].Index + 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	// send heartbeat or append to all peers
	for {
		rf.mu.Lock()
		// don't send out append if you are not leader again
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
				for {
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					var entries []LogEntry

					var baseIndex int
					if len(rf.Log) > 1 {
						baseIndex = rf.Log[1].Index
					} else {
						baseIndex = 1
					}
					nextIndex := rf.nextIndex[i]
					if nextIndex <= rf.Log[len(rf.Log)-1].Index {
						entries = rf.Log[nextIndex-baseIndex+1:]
					} else {
						entries = make([]LogEntry, 0)
					}
					args := &AppendEntriesArgs{rf.currentTerm, rf.me, nextIndex - 1,
						rf.Log[nextIndex-baseIndex].Term, entries, rf.commitIndex}
					reply := &AppendEntriesReply{}

					rf.mu.Unlock()
					ok := rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()

					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}

					if ok {
						if reply.Success == false {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = FOLLOWER
								rf.sh <- 1
								rf.votedFor = -1
								rf.persist()
								rf.mu.Unlock()
								return
							} else if reply.Term < rf.currentTerm {
								/*
								  If a leader sends out an AppendEntries RPC, and it is rejected, but not because of log
								  inconsistency (this can only happen if our term has passed), then you should immediately
								  step down, and not update nextIndex. If you do, you could race with the resetting of nextIndex
								  if you are re-elected immediately.

								  from students-guide-to-raft
								*/
								rf.mu.Unlock()
								return
							} else {
								var j int

								var baseIndex int
								if len(rf.Log) > 1 {
									baseIndex = rf.Log[1].Index
								} else {
									baseIndex = 1
								}
								for j = len(rf.Log) - 1; j >= 0; j-- {
									if rf.Log[j].Term < reply.ConflictTerm {
										rf.nextIndex[i] = reply.ConflictIndex
										break
									}
									if reply.ConflictTerm == rf.Log[j].Term {
										rf.nextIndex[i] = j + baseIndex
										break
									}
								}

								if j < 0 {
									rf.nextIndex[i] = reply.ConflictIndex
								}
							}
						} else {
							if reply.Term < rf.currentTerm {
								rf.mu.Unlock()
								return
							}
							if len(entries) == 0 {
								rf.mu.Unlock()
								return
							}
							if rf.nextIndex[i] < nextIndex+len(entries) {
								rf.nextIndex[i] = nextIndex + len(entries)
							}
							rf.matchIndex[i] = nextIndex - 1 + len(entries)
							/*
								A leader is not allowed to update commitIndex to somewhere in
								a previous term (or, for that matter, a future term). Thus,
								as the rule says, you specifically need to check that
								log[N].term == currentTerm. This is because Raft leaders cannot
								be sure an entry is actually committed (and will not ever be
								changed in the future) if it’s not from their current term.
								This is illustrated by Figure 8 in the paper.

								from students-guide-to-raft
							*/
							k := len(rf.Log) - 1
							for rf.Log[k].Term == rf.currentTerm {
								sum := 0
								for j := 0; j < len(rf.peers); j++ {
									if rf.matchIndex[j] >= k {
										sum++
										if sum >= len(rf.peers)/2 {
											rf.commitIndex = k
											rf.cond.Signal()
											rf.mu.Unlock()
											return
										}
									}
								}
								k--
							}
							rf.mu.Unlock()
							return
						}
					}
					rf.mu.Unlock()
					return
				}
			}()
			rf.mu.Lock()
		}
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// STATE: Candidate
func (rf *Raft) doCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votes = 1
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		i := i
		if i == rf.me {
			continue
		}
		go func() {
			rf.mu.Lock()
			args := &RequestVoteArgs{rf.currentTerm, rf.me, len(rf.Log) - 1, rf.Log[len(rf.Log)-1].Term}
			reply := &RequestVoteReply{}

			rf.mu.Unlock()
			ok := rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()

			needPersist := false
			defer func() {
				if needPersist {
					rf.persist()
				}
				rf.mu.Unlock()
			}()

			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.sh <- 1
					needPersist = true
					return
				} else if reply.Term < rf.currentTerm {
					return
				}

				if reply.VoteGranted && rf.state == CANDIDATE {
					rf.votes++
					if rf.votes >= len(rf.peers)/2+1 {
						rf.state = LEADER
						rf.sh <- 1
					}
				}
			}
		}()
	}

	select {
	case <-time.After(time.Duration(rf.random.Intn(DefaultTermTop-DefaultTermBottom)+DefaultTermBottom) * time.Millisecond):
		return
	case <-rf.sh:
		return
	}
}
