package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	ROLE_LEADER = iota + 1
	ROLE_CANDIDATE
	ROLE_FOLLOWER
)

const BATCH_SIZE = 20

type LogEntry struct {
	Term  int
	Value any
}

func (le LogEntry) String() string {
	return fmt.Sprintf("[%d]=%v", le.Term, le.Value)
	//return fmt.Sprintf("[%d]", le.Term)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	peerCnt     int
	role        int
	currentTerm int
	// used for redirect requests
	leaderId int
	// first index for log is 1, not 0
	log []LogEntry
	// vote for which peer in current term
	votedFor struct {
		Term        int
		CandidateId int
	}
	// index of highest log entry known to be commited
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int
	// for each server, index of the next log entry to send to that server
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	matchIndex []int
	hbVisited  bool
	applyCh    chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor.CandidateId)
	e.Encode(rf.votedFor.Term)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedForId, votedForTerm int
	var log []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("%d decode currentTerm err %s\n", rf.me, err)
		return
	}
	rf.currentTerm = currentTerm
	if err := d.Decode(&votedForId); err != nil {
		fmt.Printf("%d decode votedFor.CandidateId err %s\n", rf.me, err)
		return
	}
	rf.votedFor.CandidateId = votedForId
	if err := d.Decode(&votedForTerm); err != nil {
		fmt.Printf("%d decode votedFor.Term err %s\n", rf.me, err)
		return
	}
	rf.votedFor.Term = votedForTerm
	if err := d.Decode(&log); err != nil {
		fmt.Printf("%d decode log err %s\n", rf.me, err)
		return
	}
	rf.log = log
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	// index of candidate's last log entry
	LastLogIndex int
	// term of candidate's last log entry
	LastLogTerm int
}

func (rv RequestVoteArgs) String() string {
	return fmt.Sprintf("from %d term %d LastLogIndex %d LastLogTerm %d",
		rv.CandidateId, rv.Term, rv.LastLogIndex, rv.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// currentTerm from server for candidate to update itself if candidate's currentTerm is stale
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	stateChanged := false
	rf.mu.Lock()
	defer func() {
		if stateChanged {
			rf.persist()
		}
		rf.mu.Unlock()
	}()
	// fmt.Printf("%d vote request %s\n", rf.me, args)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		rf.role = ROLE_FOLLOWER
		rf.currentTerm = args.Term
		// currentTerm changed
		stateChanged = true
	}
	if args.Term == rf.votedFor.Term {
		if args.CandidateId == rf.votedFor.CandidateId {
			// already voted for this candidate but requestVote retried
			reply.VoteGranted = true
		} else if rf.votedFor.CandidateId > -1 {
			// already voted for another candidate
			reply.VoteGranted = false
		}
		return
	}
	lastEntry := rf.log[len(rf.log)-1]
	if lastEntry.Term < args.LastLogTerm ||
		(lastEntry.Term == args.LastLogTerm && len(rf.log)-1 <= args.LastLogIndex) {
		rf.votedFor.CandidateId = args.CandidateId
		rf.votedFor.Term = args.Term
		stateChanged = true
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// empty for heartbeat
	Entries []LogEntry
	// leader's commitIndex
	LeaderCommit int
}

func (ae AppendEntriesArgs) String() string {
	return fmt.Sprintf("from %d in term %d prevLogIndex %d prevLogTerm %d entry: %s leaderCommit %d",
		ae.LeaderId, ae.Term, ae.PrevLogIndex, ae.PrevLogTerm, ae.Entries, ae.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (aer *AppendEntriesReply) String() string {
	return fmt.Sprintf("[Term %d Success %t XTerm %d XIndex %d XLen %d",
		aer.Term, aer.Success, aer.XTerm, aer.XIndex, aer.XLen)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//	fmt.Printf("%d appendEntries %s\n", rf.me, args)
	stateChanged := false
	rf.mu.Lock()
	//fmt.Printf("%d args %s log len %d commit %d\n", rf.me, args, len(rf.log), rf.commitIndex)
	defer func() {
		if stateChanged {
			rf.persist()
		}
		rf.mu.Unlock()
	}()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if !rf.hbVisited {
		rf.hbVisited = true
	}
	rf.leaderId = args.LeaderId
	if rf.role != ROLE_FOLLOWER {
		rf.role = ROLE_FOLLOWER
	}
	if args.Term > rf.currentTerm {
		// update currentTerm
		rf.currentTerm = args.Term
		stateChanged = true
	}
	reply.Term = args.Term

	lastIndex := len(rf.log) - 1
	// new entry only could be appended when prevLogIndex and prevLogTerm matched
	// otherwise, return false to make leader decrement the next Index for this follower
	if args.PrevLogIndex <= lastIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// delete the conflict existing entries
		rf.log = rf.log[0 : args.PrevLogIndex+1]
		if lastIndex > args.PrevLogIndex {
			stateChanged = true
		}
		if len(args.Entries) > 0 {
			rf.log = append(rf.log, args.Entries...)
			stateChanged = true
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		reply.Success = true
	} else {
		// optimization for nextIndex
		reply.XLen = len(rf.log)
		if args.PrevLogIndex <= lastIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.log[i].Term != reply.XTerm {
					break
				}
				reply.XIndex = i
			}
		}
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != ROLE_LEADER {
		isLeader = false
		return index, term, isLeader
	}
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Value: command})
	index = len(rf.log) - 1
	rf.persist()
	return index, term, isLeader
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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			for range BATCH_SIZE {
				if rf.lastApplied < rf.commitIndex && !rf.killed() {
					rf.lastApplied++
					rf.applyCh <- raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.log[rf.lastApplied].Value,
						CommandIndex: rf.lastApplied,
					}
					//fmt.Printf("%d try to apply %d\n", rf.me, rf.lastApplied)
				} else {
					break
				}
			}
			rf.mu.Unlock()
			ms := 30
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}()
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 150 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.role == ROLE_LEADER {
			// leader do nothing
			rf.mu.Unlock()
			continue
		}
		if rf.role == ROLE_FOLLOWER {
			if rf.hbVisited {
				// reset mark
				rf.hbVisited = false
				rf.mu.Unlock()
				continue
			}
			rf.role = ROLE_CANDIDATE
		}
		// candidate should start a leader election
		rf.currentTerm++
		rf.votedFor.CandidateId = rf.me
		rf.votedFor.Term = rf.currentTerm

		//fmt.Printf("%d start election term is %d\n", rf.me, rf.currentTerm)
		term, lastLogIndex, lastLogTerm := rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term
		rf.persist()
		rf.mu.Unlock()
		rf.launchVote(term, lastLogIndex, lastLogTerm)
	}
}

func (rf *Raft) launchVote(term, lastLogIndex, lastLogTerm int) {
	doneCh := make(chan struct{})
	newTermCh := make(chan int)
	voteCnt := int32(1)
	for i := range rf.peerCnt {
		if i == rf.me {
			continue
		}
		go func(srv int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(srv, args, reply); ok {
				// ignore the stale reply for previous vote request because of network delay
				if reply.Term >= args.Term {
					if reply.VoteGranted {
						if atomic.AddInt32(&voteCnt, 1) > int32(rf.peerCnt/2) {
							doneCh <- struct{}{}
						}
					} else if reply.Term > args.Term {
						newTermCh <- reply.Term
					}
				}
			}
		}(i)
	}
	waiting, hasMajority := true, false
	waitMs := 50 + (rand.Int63() % 300)
	timerTimout := time.After(time.Duration(waitMs) * time.Millisecond)
	for waiting {
		select {
		case <-doneCh:
			hasMajority = true
			waiting = false
		case t := <-newTermCh:
			rf.mu.Lock()
			if rf.currentTerm < t {
				rf.role = ROLE_FOLLOWER
				rf.currentTerm = t
				// currentTerm changed
				rf.persist()
			}
			rf.mu.Unlock()
			waiting = false
		case <-timerTimout:
			waiting = false
		}
	}
	if hasMajority {
		//fmt.Printf("try to become leader %d in term %d\n", rf.me, rf.currentTerm)
		rf.mu.Lock()
		if rf.role == ROLE_CANDIDATE {
			rf.role = ROLE_LEADER
			rf.leaderId = rf.me
			for k := range rf.nextIndex {
				rf.nextIndex[k] = len(rf.log)
				rf.matchIndex[k] = 0
			}

			lastIndex := len(rf.log) - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				Entries:      make([]LogEntry, 0),
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: lastIndex,
				PrevLogTerm:  rf.log[lastIndex].Term,
			}
			for i := range rf.peerCnt {
				if i == rf.me {
					continue
				}
				go func(aeArgs AppendEntriesArgs, srv int) {
					reply := &AppendEntriesReply{}
					_ = rf.sendAppendEntries(i, &aeArgs, reply)
				}(args, i)
			}
			go func() {
				rf.syncLog()
			}()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) syncLog() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != ROLE_LEADER {
			rf.mu.Unlock()
			break
		}
		argsList := make([]*AppendEntriesArgs, rf.peerCnt)
		for srv := range rf.peerCnt {
			if srv == rf.me {
				continue
			}
			prevLogIndex := rf.nextIndex[srv] - 1
			prevLogTerm, entries := rf.log[prevLogIndex].Term, []LogEntry{}
			if prevLogIndex+1 < len(rf.log) {
				begin, end := prevLogIndex+1, min(prevLogIndex+1+BATCH_SIZE, len(rf.log))
				entries = append(entries, rf.log[begin:end]...)
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				LeaderCommit: rf.commitIndex,
				Entries:      entries,
			}
			argsList[srv] = args
		}
		rf.mu.Unlock()

		failCnt := int32(0)
		for i := range rf.peerCnt {
			if i == rf.me {
				continue
			}
			go func(srv int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(srv, args, reply)
				stateChanged := false
				rf.mu.Lock()
				//fmt.Printf("%d reply %s ok %t content %s\n", srv, args, ok, reply)
				//fmt.Printf("%d logLen %d matchIndex %v nextIndex %v\n",
				// rf.me, len(rf.log), rf.matchIndex, rf.nextIndex)
				defer func() {
					if stateChanged {
						rf.persist()
					}
					//fmt.Printf("%d logLen %d commitIndex %d matchIndex %v nextIndex %v\n",
					//	rf.me, len(rf.log), rf.commitIndex, rf.matchIndex, rf.nextIndex)
					rf.mu.Unlock()
				}()
				if rf.role != ROLE_LEADER {
					return
				}
				// network delay!!!
				if reply.Term < rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					// discover new term, change to be follower
					rf.role = ROLE_FOLLOWER
					rf.currentTerm = reply.Term
					stateChanged = true
					return
				}
				if !ok {
					if atomic.AddInt32(&failCnt, 1) > int32(rf.peerCnt)/2 {
						// a majority fail to reply, change to be follower
						rf.role = ROLE_FOLLOWER
					}
					return
				}
				if reply.Success {
					// log maybe append new entries before reply return
					if len(args.Entries) > 0 {
						newMatchIndex := args.PrevLogIndex + len(args.Entries)
						newNextIndex := args.PrevLogIndex + 1 + len(args.Entries)
						// only updated when new value is greater
						if newNextIndex <= len(rf.log) {
							rf.nextIndex[srv] = max(newNextIndex, rf.nextIndex[srv])
						}
						rf.matchIndex[srv] = max(newMatchIndex, rf.matchIndex[srv])
					} else {
						// matchIndex for srv should be at least args.PrevLogIndex
						// because success return true when rf.log[args.prevLogIndex] are the same
						rf.matchIndex[srv] = max(rf.matchIndex[srv], args.PrevLogIndex)
					}
					// try to find N to increase commitIndex
				} else {
					newNextIndex := len(rf.log)
					if reply.XIndex > 0 && reply.XTerm > 0 {
						// case 2, leader has XTerm
						if rf.log[reply.XIndex].Term == reply.XTerm {
							// the same index and term, to make the same command
							for i := reply.XIndex; i < len(rf.log); i++ {
								if rf.log[i].Term != reply.XTerm {
									break
								}
								newNextIndex = i + 1
							}
							// matchIndex at least shoud be XIndex
							rf.matchIndex[srv] = max(reply.XIndex, rf.matchIndex[srv])
						} else {
							// case 1, leader does not have xterm
							newNextIndex = reply.XIndex
						}
					} else {
						// case 3, follower's log is too short
						newNextIndex = reply.XLen
					}
					if newNextIndex < rf.nextIndex[srv] && newNextIndex > rf.matchIndex[srv] {
						rf.nextIndex[srv] = newNextIndex
					}
				}
				rf.incrCommitIndex()
			}(i, argsList[i])
		}
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// need to get lock at first
func (rf *Raft) incrCommitIndex() {
	if rf.commitIndex >= len(rf.log)-1 {
		return
	}
	flag := false
	// find the min of rf.matchIndex
	minMatchIndex := math.MaxInt32
	maxMatchIndex := 0
	for k, v := range rf.matchIndex {
		if k == rf.me {
			continue
		}
		minMatchIndex = min(v, minMatchIndex)
		maxMatchIndex = max(v, maxMatchIndex)
	}
	for n := maxMatchIndex; n >= minMatchIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			// only commit logEntry in currentTerm
			break
		}
		cnt := 1
		for i := range rf.peerCnt {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= n {
				cnt++
				if cnt > rf.peerCnt/2 {
					flag = true
					break
				}
			}
		}
		if flag {
			rf.commitIndex = max(n, rf.commitIndex)
			break
		}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.peerCnt = len(peers)
	rf.role = ROLE_FOLLOWER
	rf.currentTerm = 0
	rf.log = append([]LogEntry{}, LogEntry{0, 0})
	// vote for which peer in current term
	rf.votedFor = struct {
		Term        int
		CandidateId int
	}{0, -1}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.peerCnt)
	rf.matchIndex = make([]int, rf.peerCnt)

	rf.hbVisited = false
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
