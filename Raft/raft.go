package raft

//
//   this is an outline of the API that raft must expose to
//   the service (or tester). see comments below for
//   each of these functions for more details.
//
//   rf = Make(...)
//   create a new Raft server.
//   rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
//   rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
//   ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math"
	"math/rand"
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	// 	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// when send other kinds of messages (e.g. snapshots) on the applyCh,
// but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	peerNum      int                 // server number in peers[]
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	identity     string              // including "leader" "candidate" "follower"
	currentTerm  int                 // lasted term server has seen
	votedFor     int32               // candidateId that received vote in current term
	hasVoted     bool                // show that there has voted one peer in this given term
	log          *logInfo            // log entries;and term when entry was received by leader
	commitIndex  int                 // index of the highest log entry known to be committed
	lastApplied  int                 // index os the highest log entry applied to state machine
	nextIndex    []int               // index of the next log entry to send to that server
	matchIndex   []int               // index of the highest log entry known to be replicated on server
	electionDone chan interface{}    // election timeout sign in electing
	idleDone     chan interface{}    // idle timeout elapses without receiving any RPC
	interrupt    bool                // find higher term while sending RPC
	logApplySign chan bool           // check whether log entry can be applied
	timer        *time.Timer
}

type logInfo struct {
	logs   []*logEntry
	logNum int
}

type logEntry struct {
	currentTerm int
	entry       interface{}
	commited    bool
}

// return currentTerm and whether this server
// believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	term = rf.currentTerm
	if rf.identity == "leader" {
		isLeader = true
	} else {
		isLeader = false
	}
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.log)
	encoder.Encode(rf.votedFor)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
	return
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	var term int
	var votedFor int32
	var tmpLog *logInfo
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	decoder.Decode(term)
	decoder.Decode(tmpLog)
	decoder.Decode(votedFor)
	if term != 0 {
		rf.currentTerm = term
	} else if tmpLog != nil {
		rf.log = tmpLog
	} else if votedFor != 0 {
		rf.votedFor = votedFor
	}
	return
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int32
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		if rf.identity == "candidate" || rf.identity == "leader" {
			rf.currentTerm = args.Term
			rf.mu.Unlock()
			rf.backToFollower()
			return
		}
	}
	if rf.hasVoted == false && args.LastLogIndex > rf.commitIndex {
		rf.votedFor = args.CandidateId
		rf.hasVoted = true
		reply.VoteGranted = true
		rf.timer.Reset(randomDuration())
		rf.mu.Unlock()
		return
	}
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
	if ok == true && reply.VoteGranted == true {
		return ok
	}
	if reply.VoteGranted == false && reply.Term > rf.currentTerm {
		rf.interrupt = true
		rf.backToFollower()
		return false
	}
	return false
}

//
//AppendEntries RPC handle
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.log.logNum-1)))
	}
	entryLen := len(args.Entries)
	if args.Entries == nil {
		if args.Term < rf.currentTerm {
			reply.Success = false
			return
		}
		for i := 0; i < rf.peerNum; i++ {
			rf.nextIndex[i] = args.PrevLogIndex + 1
		}

		if rf.identity == "follower" && args.Entries == nil {
			rf.timer.Reset(randomDuration())
			return
		}
		if rf.identity == "candidate" && args.Entries == nil {
			rf.backToFollower()
			return
		}
	} else {
		if rf.log.logs[args.PrevLogIndex].currentTerm != args.PrevLogTerm {
			// if log does not contain an entry at preLogIndex whose term matches preLogTerm
			reply.Success = false
			return
		} else if rf.log.logs[args.PrevLogIndex+1].currentTerm != args.Term {
			rf.log.logs = rf.log.logs[0:args.PrevLogIndex]
			return
		} else {
			for i := 0; i < entryLen; i++ {
				rf.log.logs[rf.log.logNum+i] = &args.Entries[i]
			}
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok == true {
		rf.nextIndex[server] = args.PrevLogIndex + 1
		return ok
	}
	return false
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
	var sign chan bool
	index := -1
	term := -1
	isLeader := true
	if rf.identity != "leader" {
		isLeader = false
		return index, term, isLeader
	} else {
		logNum := rf.log.logNum
		index = rf.commitIndex + 1
		term = rf.currentTerm
		rf.mu.Lock()
		rf.log.logs[logNum].currentTerm = term
		rf.log.logs[logNum].entry = command
		rf.log.logNum++
		go func() {
			defer rf.mu.Unlock()
			isTrue := rf.parallelReplicated()
			if isTrue == true {
				rf.log.logs[logNum].commited = true

				sign <- true
			}
		}()
		go func() {
			<-sign
			applySign := false
			for applySign != true {
				applySign = rf.appliesEntryToSM()
			}
		}()
		return index, term, isLeader
	}
}

// replicated command to other peer in parallel.
func (rf *Raft) parallelReplicated() bool {
	var wg sync.WaitGroup
	var tmpSign chan bool
	var i, checkApply int
	var commands []interface{}
	for _, command := range rf.log.logs[rf.lastApplied:len(rf.log.logs)] {
		commands[i] = command
		i++
	}
	for serverIndex, point := range rf.peers {
		if point != rf.peers[rf.me] {
			wg.Add(1)
			go func() {
				defer wg.Done()
				args := rf.initAppendArgs(commands)
				reply := new(AppendEntriesReply)
				if rf.sendAppendEntries(serverIndex, args, reply) == true {
					checkApply++
					if checkApply > (rf.peerNum-1)/2 {
						rf.logApplySign <- true
					}
				}
			}()
		}
	}
	go func() {
		wg.Wait()
		<-rf.logApplySign
		tmpSign <- true
	}()
	<-tmpSign
	return true
}

// replicated commited log to state machine.
func (rf *Raft) appliesEntryToSM() bool {

	return true
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
//AppendEntriesArgs example AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int
	Entries      []logEntry // log entries to store (empty for heartbeat;
	LeaderCommit int
}

//
//AppendEntriesReply example AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term    int  // for leader to update itself
	Success bool // true if follower contained entry matching prevlogindex and prevlogterm
}

func (rf *Raft) initRequestArgs() *RequestVoteArgs {
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = int32(rf.me)
	args.LastLogIndex = rf.log.logNum - 1
	args.LastLogTerm = rf.log.logs[args.LastLogIndex].currentTerm
	return args
}

func (rf *Raft) initAppendArgs(commands []interface{}) *AppendEntriesArgs {
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = rf.log.logNum - 1
	args.PrevLogTerm = rf.log.logs[rf.log.logNum].currentTerm
	for index, command := range commands {
		newLogEntry := new(logEntry)
		newLogEntry.entry = command
		newLogEntry.currentTerm = rf.currentTerm
		args.Entries[index] = *newLogEntry
	}
	args.LeaderCommit = rf.commitIndex
	return args
}

func (rf *Raft) checkVote(currentVote *int, reply *RequestVoteReply) {
	if reply.VoteGranted == true {
		*currentVote++
		if *currentVote == (rf.peerNum/2)+1 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) backToFollower() {
	rf.mu.Lock()
	rf.timer.Reset(randomDuration())
	go func() {
		<-rf.timer.C
		rf.idleDone <- 1
	}()
	rf.votedFor = -1
	rf.identity = "follower"
	rf.electionDone <- 1
	rf.mu.Unlock()
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.timer.Reset(randomDuration())
	rf.identity = "candidate"
	rf.currentTerm++
	rf.votedFor = int32(rf.me)
	rf.hasVoted = true
	rf.mu.Unlock()
	go func() {
		<-rf.timer.C
		rf.idleDone <- 1
	}()
}

func (rf *Raft) becomeLeader() {
	var wg sync.WaitGroup
	rf.mu.Lock()
	rf.identity = "leader"
	rf.mu.Unlock()
	go func() {
		for serverIndex, point := range rf.peers {
			if point != rf.peers[rf.me] {
				wg.Add(1)
				go func() {
					defer wg.Done()
					args := rf.initAppendArgs(nil)
					reply := new(AppendEntriesReply)
					rf.sendAppendEntries(serverIndex, args, reply)
					if reply.Success == false && reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.backToFollower()
					}
				}()
			}
		}
		wg.Wait()
		time.Sleep(randomDuration() / 4)
	}()
}

func (rf *Raft) startElection() {
	var currentVote *int
	var wg sync.WaitGroup
	rf.becomeCandidate()
	for serverIndex, point := range rf.peers {
		if point != rf.peers[rf.me] {
			wg.Add(1)
			go func() {
				defer wg.Done()
				args := rf.initRequestArgs()
				reply := new(RequestVoteReply)
				if rf.sendRequestVote(serverIndex, args, reply) == true {
					rf.checkVote(currentVote, reply)
				} else if rf.interrupt == true {
					return
				}
			}()
		}
	}
	wg.Wait()
}

// One thread(timer thread/ticker()) manages the election timer for both followers and candidates;
// it starts a new election once a randomized election timeout has elapsed.
// A second thread causes the server to return to the follower state if,
// as leader, it is unable to communicate with a majority of the cluster;
// clients are then able to retry their requests with another server.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.idleDone:
			go rf.startElection()
		case <-rf.electionDone:
			go rf.backToFollower()
		}
	}
}

// choose a random election/idle timeout for rf.timer
func randomDuration() time.Duration {
	initInt := rand.Intn(50)
	stringDuration := strconv.Itoa(initInt+200) + "ms"
	duration, _ := time.ParseDuration(stringDuration)
	return duration
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
	var num int
	// initialize a raft single peer state
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	for range rf.peers {
		num++
	}
	rf.peerNum = num
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	//rf.log.logs = make([]*logEntry,100)
	rf.electionDone = make(chan interface{})
	rf.identity = "follower"
	rf.interrupt = false
	rf.nextIndex = make([]int, rf.peerNum)
	rf.matchIndex = make([]int, rf.peerNum)
	rf.timer = time.NewTimer(randomDuration())

	// use a timer with randomDuration() for selection
	go func() {
		<-rf.timer.C
		rf.idleDone <- 1
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go func() {
		for {
			tmpMsg := <-applyCh
			if tmpMsg.CommandValid == true {
				rf.log.logs[tmpMsg.CommandIndex].entry = tmpMsg.Command
			}
		}
	}()
	return rf
}
