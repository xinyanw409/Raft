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
	// "fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cmu440/labrpc"
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
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term
	state       int // current state of the raft
	voteCnt     int // number of followers that vote for this candidate

	heartBeat   chan bool // receive heartbeat from leader in follower and candidate case
	setLeader   chan bool // signal to set a candidate to leader when majority vote granted
	setFollower chan bool // signal to reset election timer after voitng
	isKill      chan bool // Kill() function called
	commitChan  chan bool // signal to tell raft to apply msg to ApplyChan

	log         []LogEntry // log entries
	commitIndex int        // index of highest log entry known to be committed
	lastApplied int        // index of highest log entry applied to state machine
	nextIndex   []int      // for each server, index of the next log entry to send to that server
	matchIndex  []int      // for each server, index of highest log entry known to be replicated on server
}

type LogEntry struct {
	Term    int         // term for the entry
	Command interface{} // command for the entry
}

const (
	LEADER    = 1
	CANDIDATE = 2
	FOLLOWER  = 3
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate term
	CandidateId  int // candidate broadcasting the vote request
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	GrantVote bool // true means candidate received vote
	Term      int  // updated term
}

type AppendEntryArgs struct {
	Term         int        // leader term
	LeaderId     int        // leader that broadcasting the append entry request
	PreLogIndex  int        // index of log entry immediately preceding new ones
	PreLogTerm   int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntryReply struct {
	Term      int  // updated term
	Success   bool // true if follower contained entry macthing
	NextIndex int  // the correct nextIndex for follower, used to update leader's nextIndex[]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.GrantVote = false //reply false if term < currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// candidate's log should be at least as up-to-date as receiver's log
	reply.Term = rf.currentTerm
	thisTerm := rf.log[len(rf.log)-1].Term
	if args.LastLogTerm > thisTerm || args.LastLogTerm == thisTerm && args.LastLogIndex >= len(rf.log)-1 {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.GrantVote = true
			rf.state = FOLLOWER
			rf.setFollower <- true
			rf.votedFor = args.CandidateId
		}
	}
	return
}

// RPC AppendEntry handler
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	rf.heartBeat <- true
	if args.PreLogIndex > len(rf.log)-1 {
		reply.Success = false
		reply.NextIndex = len(rf.log) // reply the correct nextIndex to the leader
		return
	}
	term := rf.log[args.PreLogIndex].Term
	// delete all subsequence and overwrite the entry
	if term != args.PreLogTerm {
		i := args.PreLogIndex - 1
		for i >= 0 && rf.log[i].Term != term {
			i--
		}
		reply.NextIndex = i + 1
		return
	}
	// repair the missing entry
	rf.log = rf.log[:args.PreLogIndex+1]
	for j := 0; j < len(args.Entries); j++ {
		rf.log = append(rf.log, args.Entries[j])
	}
	reply.Success = true
	reply.NextIndex = len(rf.log)
	// set commitIndex = min(leaderCommit, index of last new entry)
	// fmt.Printf("the len of the log for raft %v is %v\n", rf.me, len(rf.log))
	if args.LeaderCommit > rf.commitIndex {
		new := len(rf.log) - 1
		// fmt.Printf("leadercommit %v rf.commitIndex %v new %v\n", args.LeaderCommit, rf.commitIndex, new)
		if new > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = new
		}
		rf.commitChan <- true
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		term = rf.currentTerm
		index = len(rf.log)
		logItem := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, logItem)
	} else {
		isLeader = false
	}
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
	rf.isKill <- true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (3A, 3B).
	rf := &Raft{
		peers:       peers,
		me:          me,
		mu:          sync.Mutex{},
		currentTerm: 0,
		state:       FOLLOWER,
		votedFor:    -1,
		voteCnt:     0,
		heartBeat:   make(chan bool, 1),
		setLeader:   make(chan bool, 1),
		setFollower: make(chan bool, 1),
		isKill:      make(chan bool, 1),
		commitChan:  make(chan bool, 1),
	}
	rf.log = append(rf.log, LogEntry{Term: 0}) // dummy node
	go rf.mainRoutine()
	go rf.commitRoutine(applyCh)
	return rf
}

// Commit routine for raft, each time commitIndex changes, apply msg
func (rf *Raft) commitRoutine(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitChan:
			rf.mu.Lock()
			k := rf.lastApplied + 1
			for k <= rf.commitIndex {
				msg := ApplyMsg{
					Index:   k,
					Command: rf.log[k].Command,
				}
				applyCh <- msg
				rf.lastApplied = k
				k++
			}
			rf.mu.Unlock()
		}
	}
}

// Main routine for raft, state switch among FOLLOWER, LEADER, CANDIDATE
func (rf *Raft) mainRoutine() {
	for {
		rf.mu.Lock()
		STATE := rf.state
		rf.mu.Unlock()
		if STATE == FOLLOWER {
			// fmt.Printf("raft %v become follower, curr term is %v\n", rf.me, rf.currentTerm)
			timeout := time.After(time.Duration(rand.Int63n(150)+400) * time.Millisecond) // reset election timeout
			select {
			case <-rf.heartBeat:
				// fmt.Printf("raft %v receive heartbeat\n", rf.me)
				continue
			case <-rf.setFollower: // if not voted in this term, vote for this candidate
				// fmt.Printf("raft %v vote for candidate\n", rf.me)
				continue
			case <-timeout: // election timeout
				// fmt.Printf("follower %v timeout\n", rf.me)
				rf.mu.Lock()
				rf.state = CANDIDATE // if doesn't hear from leader, become a candidate
				rf.mu.Unlock()
				continue
			case <-rf.isKill:
				return
			}
		}
		if STATE == CANDIDATE {
			// fmt.Printf("raft %v become candidate, curr term is %v\n", rf.me, rf.currentTerm)
			rf.mu.Lock()
			rf.updateCandidate()
			rf.mu.Unlock()
			go rf.broadcastVoteRequest()
			timeout := time.After(time.Duration(rand.Int63n(150)+400) * time.Millisecond) // reset election timeout
			select {
			case <-rf.heartBeat:
				// fmt.Printf("candidate %v recieve hb, become follower\n", rf.me)
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
				continue
			case <-timeout:
				// fmt.Printf("candidate %v timeout\n", rf.me)
				continue
			case <-rf.setLeader:
				rf.mu.Lock()
				rf.state = LEADER
				// reinitialized after election
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = len(rf.log) // initialized to leader lastlogindex + 1
					rf.matchIndex[i] = 0          // initilaized to 0
				}
				rf.mu.Unlock()
				continue
			case <-rf.isKill:
				return
			}
		}
		if STATE == LEADER { // Leader
			select {
			case <-rf.isKill:
				return
			default:
				go rf.broadcastAppendEntry()
				time.Sleep(100 * time.Millisecond) // heartbeat interval
			}
		}
	}
}

// Fuction to broadcast voterequest
func (rf *Raft) broadcastVoteRequest() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go rf.sendHelper(i, args)
		}
	}
}

func (rf *Raft) sendHelper(i int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{
		GrantVote: false,
		Term:      -1,
	}
	ok := rf.sendRequestVote(i, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// if RPC response contains term T > currTerm, set currT = T and convert to follower
		if reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
		if reply.GrantVote {
			rf.voteCnt++
			if rf.state == CANDIDATE && rf.voteCnt > len(rf.peers)/2 {
				rf.setLeader <- true // recieved majority of votes, switch to leader
			}
		}
	}
	return
}

// Fuction to broadcast appendentry
func (rf *Raft) broadcastAppendEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderCommitIndexUpdate()
	// consistent all logs on servers
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			tmp := rf.nextIndex[i] - 1
			args := &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  tmp, // index of log entry immediately preceding new ones for the follower
				PreLogTerm:   rf.log[tmp].Term,
				LeaderCommit: rf.commitIndex,
			}
			if len(rf.log)-1 >= rf.nextIndex[i] {
				entry := rf.log[rf.nextIndex[i]:]
				for j := 0; j < len(entry); j++ {
					args.Entries = append(args.Entries, entry[j])
				}
			} else {
				// if heartbeat, entry is empty
				args.Entries = make([]LogEntry, 0)
			}
			go func(i int, args *AppendEntryArgs) {
				rf.mu.Lock()
				reply := &AppendEntryReply{
					Term:      rf.currentTerm,
					Success:   false,
					NextIndex: tmp,
				}
				rf.mu.Unlock()
				ok := rf.peers[i].Call("Raft.AppendEntry", args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok {
					if rf.state != LEADER {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.state = FOLLOWER
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						return
					}
					if reply.Success {
						// if successful, update nextIndex and matchIndex for follower
						rf.nextIndex[i] += len(args.Entries)
						rf.matchIndex[i] = rf.nextIndex[i] - 1
					} else {
						// correct nextIndex for the follower
						rf.nextIndex[i] = reply.NextIndex
					}
				}
			}(i, args)
		}
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) leaderCommitIndexUpdate() {
	N := rf.commitIndex + 1
	for N <= len(rf.log)-1 {
		cnt := 1
		for k := range rf.peers {
			if k != rf.me && rf.matchIndex[k] >= N && rf.log[N].Term == rf.currentTerm {
				cnt++
			}
		}
		if cnt*2 > len(rf.peers) {
			rf.commitIndex = N
		}
		N++
	}
	rf.commitChan <- true
}

// Update candidate
func (rf *Raft) updateCandidate() {
	rf.voteCnt = 1
	rf.currentTerm++
	rf.votedFor = rf.me // vote for itself
	return
}
