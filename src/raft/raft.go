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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

func D(f string, a ...interface{}) {
	fmt.Printf(f+"\n", a...)
}

// ApplyMsg
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

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// the items defined in paper figure2
	currentTerm int           // latest term server has seen
	votedFor    int           // candidateId that received vote in current term
	log         []interface{} // log entries
	commitIndex int           // index of the highest log entry known to be committed
	lastApplied int           // index of the highest log entry applied to state machine
	nextIndex   []int         // (leaders) for each server, index of the next log entry to send to that server
	matchIndex  []int         // (leaders) for each server, index of the highest log entry known to be replicated on server

	// custom data
	applyMsg      ApplyMsg
	leaderId      int
	isRunning     bool
	lastHeartBeat time.Time
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.leaderId == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// RequestVoteArgs
// invoked by candidates to gather votes
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last entry
}

// RequestVoteReply
// the vote rpc result
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means the candidate received vote
}

// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		if rf.leaderId == rf.me {
			rf.leaderId = -1
		}
	} else if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.lastHeartBeat = time.Now()
}

// AppendEntriesArgs
// rpc argument for AppendEntries
//
type AppendEntriesArgs struct {
	Term         int    // leader's term
	LeaderId     int    // so follower can redirect clients
	PrevLogIndex int    // index of log entry immediately preceding new ones
	PrevLogTerm  int    // term of prevLogIndex entry
	Entries      []byte // log entries to store (empty for heartbeat, may send more than one for efficiency)
	LeaderCommit int    // leader's commitIndex
}

// AppendEntriesReply
// rpc reply for AppendEntries
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		if rf.leaderId == rf.me { // avoid multi leader
			rf.leaderId = -1
		}
		rf.mu.Unlock()
	}
	if args.Entries == nil { // heartbeat
		reply.Success = true
		rf.mu.Lock()
		rf.leaderId = args.LeaderId
		rf.lastHeartBeat = time.Now()
		rf.mu.Unlock()

	} else { // append entries
		// todo
	}
}

func (rf *Raft) heartBeat(heartBeatTimeOut time.Duration) {
	var args = AppendEntriesArgs{Entries: nil, LeaderId: rf.me, Term: rf.currentTerm}
	var reply = AppendEntriesReply{}
	for rf.leaderId == rf.me {
		for i := range rf.peers {
			if rf.peers[i].Call("Raft.AppendEntries", args, &reply) {
				// if other response term > currentTerm, convert to follower
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.leaderId = -1
					rf.mu.Unlock()
					return
				}
			}
		}
		time.Sleep(heartBeatTimeOut)
	}
}

func (rf *Raft) run() {
	for rf.isRunning {
		if rf.leaderId == rf.me {

		} else { // follower or candidate
			// 150-300 ms
			electionTimeout := int64(rand.Intn(150) + 150)
			if time.Since(rf.lastHeartBeat).Nanoseconds()/1e6 > electionTimeout { // convert to candidate
				rf.mu.Lock()
				rf.leaderId = -1              // reset the lead
				rf.currentTerm += 1           // increment current term
				rf.votedFor = rf.me           // vote for self
				rf.lastHeartBeat = time.Now() // reset election timer
				rf.mu.Unlock()

				args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: -1, LastLogTerm: -1}
				reply := &RequestVoteReply{}
				beOK := 0
				for i := range rf.peers { // send request vote rpc to all servers
					reply.VoteGranted = false
					if rf.peers[i].Call("Raft.RequestVote", args, reply) {
						// if received from new leader, convert to follower
						if reply.Term > rf.currentTerm || rf.currentTerm > args.Term || rf.leaderId != -1 {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.mu.Unlock()
							beOK = 0
							break
						}
						if reply.VoteGranted {
							beOK += 1
						}
					}
				}
				if beOK*2 > len(rf.peers) {
					rf.leaderId = rf.me
					D("%d(term=%d) become leader, has vote %d\n", rf.me, rf.currentTerm, beOK)
					go rf.heartBeat(50 * time.Millisecond)
				}
			}
		}
	}
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
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
	return index, rf.currentTerm, rf.leaderId == rf.me
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.leaderId = -1 // if leader, stop the heart beat
	rf.isRunning = false
}

// Make
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.leaderId = -1
	rf.lastHeartBeat = time.Now()
	rf.isRunning = true

	rand.Seed(time.Now().UnixNano())
	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
