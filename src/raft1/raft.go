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
	matchIndex    []int
	hbVisited     bool
	applyCh       chan raftapi.ApplyMsg
	baseIndex     int
	snapshotBytes []byte
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
	e.Encode(rf.baseIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshotBytes)
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
	var currentTerm, votedForId, votedForTerm, baseIndex int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedForId) != nil ||
		d.Decode(&votedForTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&baseIndex) != nil {
		fmt.Println("decode error")
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor.CandidateId = votedForId
	rf.votedFor.Term = votedForTerm
	rf.log = log
	rf.baseIndex = baseIndex
	rf.lastApplied = baseIndex
	rf.commitIndex = baseIndex
}

func (rf *Raft) lastLog() LogEntry {
	i := len(rf.log) - 1
	return rf.log[i]
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1 + rf.baseIndex
}

func (rf *Raft) shorter(index int) int {
	return index - rf.baseIndex
}

func (rf *Raft) longer(index int) int {
	return index + rf.baseIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.baseIndex {
		return
	}
	newLog := make([]LogEntry, len(rf.log)+rf.baseIndex-index)
	copy(newLog, rf.log[(index-rf.baseIndex):])
	rf.log = newLog
	rf.baseIndex = index
	rf.snapshotBytes = snapshot
	rf.persist()
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
	//fmt.Printf("%d vote request %s\n", rf.me, args)
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
	lastEntry := rf.lastLog()
	if lastEntry.Term < args.LastLogTerm ||
		(lastEntry.Term == args.LastLogTerm && rf.lastLogIndex() <= args.LastLogIndex) {
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
	//fmt.Printf("%d appendEntries %s\n", rf.me, args)
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

	// new entry only could be appended when prevLogIndex and prevLogTerm matched
	// otherwise, return false to make leader decrement the next Index for this follower
	prevIndex := rf.shorter(args.PrevLogIndex)
	if prevIndex < len(rf.log) && rf.log[prevIndex].Term == args.PrevLogTerm {
		// delete the conflict existing entries
		diff := false
		a, b := prevIndex+1, 0
		for a < len(rf.log) && b < len(args.Entries) {
			if rf.log[a].Term != args.Entries[b].Term {
				diff = true
				break
			}
			a++
			b++
		}
		if diff {
			rf.log = rf.log[:a]
			stateChanged = true
		}
		if b < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[b:]...)
			stateChanged = true
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		}
		reply.Success = true
	} else {
		// optimization for nextIndex
		reply.XLen = rf.lastLogIndex() + 1
		if prevIndex < len(rf.log) && rf.log[prevIndex].Term != args.PrevLogTerm {
			reply.XTerm = rf.log[prevIndex].Term
			reply.XIndex = args.PrevLogIndex
			for i := prevIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != reply.XTerm {
					break
				}
				reply.XIndex = rf.longer(i)
			}
		}
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	stateChanged := false
	defer func() {
		if stateChanged {
			rf.persist()
		}
		rf.mu.Unlock()
	}()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	if !rf.hbVisited {
		rf.hbVisited = true
	}
	reply.Term = args.Term
	rf.leaderId = args.LeaderId
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		stateChanged = true
	}
	if rf.role != ROLE_FOLLOWER {
		rf.role = ROLE_FOLLOWER
	}
	firstLog := rf.log[0]
	// existing log entry has same index and term as snapshot’s last included entry
	if args.LastIncludedIndex == rf.baseIndex && args.LastIncludedTerm == firstLog.Term {
		return
	}
	if rf.baseIndex > args.LastIncludedIndex {
		return
	}
	rf.baseIndex = args.LastIncludedIndex
	rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	stateChanged = true
	rf.snapshotBytes = args.Data
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
}

type requestArgs struct {
	AE   bool
	Args any
}

func (rf *Raft) collectArgs() map[int]requestArgs {
	ret := map[int]requestArgs{}
	for srv := range rf.peerCnt {
		if srv == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[srv] - 1
		if prevLogIndex < rf.baseIndex {
			// installSnapshot
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.baseIndex,
				LastIncludedTerm:  rf.log[0].Term,
				Offset:            0,
				Data:              rf.snapshotBytes,
				Done:              true,
			}
			ret[srv] = requestArgs{AE: false, Args: args}
		} else {
			// appendEntries
			prevIndex := rf.shorter(prevLogIndex)
			entries := []LogEntry{}
			if prevIndex < len(rf.log)-1 {
				begin := prevIndex + 1
				end := min(begin+BATCH_SIZE, len(rf.log))
				entries = append(entries, rf.log[begin:end]...)
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogTerm:  rf.log[prevIndex].Term,
				PrevLogIndex: prevLogIndex,
				LeaderCommit: rf.commitIndex,
				Entries:      entries,
			}
			ret[srv] = requestArgs{AE: true, Args: args}
		}
	}
	return ret
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
	index = rf.lastLogIndex()
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

func (rf *Raft) applyMsg() {
	for !rf.killed() {
		msgList := []raftapi.ApplyMsg{}
		rf.mu.Lock()
		if rf.role != ROLE_LEADER && rf.lastApplied < rf.baseIndex {
			rf.lastApplied = rf.baseIndex
			msgList = append(msgList, raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshotBytes,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.baseIndex,
			})
		} else {
			for range BATCH_SIZE {
				if rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.lastLogIndex() {
					rf.lastApplied++
					shorter := rf.shorter(rf.lastApplied)
					msgList = append(msgList, raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.log[shorter].Value,
						CommandIndex: rf.lastApplied,
					})
				} else {
					break
				}
			}
		}
		rf.mu.Unlock()
		for _, v := range msgList {
			rf.applyCh <- v
		}
		ms := 30
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) ticker() {
	go rf.applyMsg()
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
		term, lastLogIndex, lastLogTerm := rf.currentTerm, rf.lastLogIndex(), rf.lastLog().Term
		rf.persist()
		rf.mu.Unlock()
		if rf.launchVote(term, lastLogIndex, lastLogTerm) {
			rf.mu.Lock()
			if rf.role == ROLE_CANDIDATE {
				rf.role = ROLE_LEADER
				rf.leaderId = rf.me
				for k := range rf.nextIndex {
					rf.nextIndex[k] = rf.lastLogIndex() + 1
					rf.matchIndex[k] = 0
				}
			}
			rf.mu.Unlock()
			go func() {
				for !rf.killed() && rf.syncLog() {
					ms := 60
					time.Sleep(time.Duration(ms) * time.Millisecond)
				}
			}()
		}
	}
}

func (rf *Raft) launchVote(term, lastLogIndex, lastLogTerm int) bool {
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
	waitMs := 50 + (rand.Int63() % 300)
	timerTimout := time.After(time.Duration(waitMs) * time.Millisecond)
	for {
		select {
		case <-doneCh:
			return true
		case t := <-newTermCh:
			rf.mu.Lock()
			if rf.currentTerm < t {
				rf.role = ROLE_FOLLOWER
				rf.currentTerm = t
				// currentTerm changed
				rf.persist()
			}
			rf.mu.Unlock()
			return false
		case <-timerTimout:
			return false
		}
	}
}

func (rf *Raft) syncLog() bool {
	rf.mu.Lock()
	if rf.role != ROLE_LEADER {
		rf.mu.Unlock()
		return false
	}
	argsMap := rf.collectArgs()
	rf.mu.Unlock()

	failCnt := int32(0)
	for k, v := range argsMap {
		go func(srv int, request requestArgs) {
			ok, replyTerm := false, 0
			aeArgs := AppendEntriesArgs{}
			snapArgs := InstallSnapshotArgs{}
			aeReply, snapReply := &AppendEntriesReply{}, &InstallSnapshotReply{}
			if request.AE {
				aeArgs = request.Args.(AppendEntriesArgs)
				ok = rf.sendAppendEntries(srv, &aeArgs, aeReply)
				replyTerm = aeReply.Term
			} else {
				snapArgs = request.Args.(InstallSnapshotArgs)
				ok = rf.sendInstallSnapshot(srv, &snapArgs, snapReply)
				replyTerm = snapReply.Term
			}
			if !ok {
				if atomic.AddInt32(&failCnt, 1) > int32(rf.peerCnt)/2 {
					// a majority fail to reply, change to be follower
					rf.mu.Lock()
					rf.role = ROLE_FOLLOWER
					rf.mu.Unlock()
				}
				return
			}
			stateChanged := false
			rf.mu.Lock()
			//fmt.Printf("%d reply %s ok %t content %s\n", srv, args, ok, reply)
			//fmt.Printf("%d logLen %d matchIndex %v nextIndex %v\n",
			//rf.me, len(rf.log), rf.matchIndex, rf.nextIndex)
			defer func() {
				if stateChanged {
					rf.persist()
				}
				//fmt.Printf("%d logLen %d commitIndex %d matchIndex %v nextIndex %v\n",
				//	rf.me, len(rf.log), rf.commitIndex, rf.matchIndex, rf.nextIndex)
				rf.mu.Unlock()
			}()
			// network delay!!!
			if rf.role != ROLE_LEADER || replyTerm < rf.currentTerm {
				return
			}
			if replyTerm > rf.currentTerm {
				// discover new term, change to be follower
				rf.role = ROLE_FOLLOWER
				rf.currentTerm = replyTerm
				stateChanged = true
				return
			}
			if request.AE {
				rf.handleAEReply(srv, request.Args.(AppendEntriesArgs), aeReply)
			} else {
				newNextIndex := snapArgs.LastIncludedIndex + 1
				newMatchIndex := snapArgs.LastIncludedIndex
				if newNextIndex > rf.nextIndex[srv] {
					rf.nextIndex[srv] = min(newNextIndex, rf.lastLogIndex()+1)
				}
				if newMatchIndex > rf.matchIndex[srv] {
					rf.matchIndex[srv] = newMatchIndex
				}
			}
			rf.incrCommitIndex()
		}(k, v)
	}
	return true
}

func (rf *Raft) handleAEReply(srv int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		// matchIndex for srv should be at least args.PrevLogIndex
		// because success return true when rf.log[args.prevLogIndex] are the same
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		newNextIndex := newMatchIndex + 1
		// only updated when new value is greater
		if newNextIndex > rf.nextIndex[srv] && newNextIndex <= rf.lastLogIndex()+1 {
			rf.nextIndex[srv] = newNextIndex
		}
		if newMatchIndex > rf.matchIndex[srv] {
			rf.matchIndex[srv] = newMatchIndex
		}
	} else {
		var newNextIndex int
		if reply.XIndex > 0 && reply.XTerm > 0 {
			// case 1, leader does not have xterm
			// or shorter < 0 means the log slice has been compacted
			newNextIndex = reply.XIndex
			shorter := rf.shorter(reply.XIndex)
			// case 2, leader has XTerm.
			if shorter >= 0 && rf.log[shorter].Term == reply.XTerm {
				// the same index and term, to make the same command
				for i := shorter + 1; i < len(rf.log); i++ {
					if rf.log[i].Term != reply.XTerm {
						break
					}
					newNextIndex = rf.longer(i + 1)
				}
			}
		} else {
			// case 3, follower's log is too short
			newNextIndex = reply.XLen
		}
		if newNextIndex < rf.nextIndex[srv] && newNextIndex >= rf.matchIndex[srv] {
			rf.nextIndex[srv] = newNextIndex
		}
	}
}

// need to get lock at first
func (rf *Raft) incrCommitIndex() {
	if rf.commitIndex >= rf.lastLogIndex() {
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
		shorterN := rf.shorter(n)
		if shorterN < 0 || rf.log[shorterN].Term != rf.currentTerm {
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
			if n > rf.commitIndex {
				rf.commitIndex = n
			}
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
	rf.snapshotBytes = nil
	rf.baseIndex = 0

	// initialize from state persisted before a crash
	// server would turn to be follower and does not need to send snapshot to others
	// snapShot saved by raft layer, but read by application layer
	rf.readPersist(persister.ReadRaftState())
	rf.snapshotBytes = persister.ReadSnapshot()

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
