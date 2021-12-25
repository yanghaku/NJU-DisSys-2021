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

func D(f string, a ...interface{}) { // for debug
	fmt.Printf(f+"\n", a...)
}

// global timeout config
const heartBeatInterval = 20 * time.Millisecond
const RPCTimeOut = 100 * time.Millisecond
const rangeTimeOutL = 150
const rangeTimeOutR = 300

func Min(a int, b int) int { // helper function
	if a > b {
		return b
	}
	return a
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type LogEntry struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                 sync.Mutex
	peers              []*labrpc.ClientEnd
	persister          *Persister
	me                 int           // index into peers[]
	currentTerm        int           // latest term server has seen
	votedFor           int           // candidateId that received vote in current term
	log                []LogEntry    // log entries
	commitIndex        int           // index of the highest log entry known to be committed
	lastApplied        int           // index of the highest log entry applied to state machine
	nextIndex          []int         // (for leaders) save each server's index of the next log entry to send
	matchIndex         []int         // (for leaders) save each server's index of the highest log entry known to be replicated
	applyMsg           chan ApplyMsg // if committed, send msg to it
	leaderId           int           // the leader's id
	waitHeartBeatTimer *time.Timer   // wait for heart beat timer, if timeout, become candidate
	sendHeartBeatTimer *time.Timer   // (for leaders), if timeout, send headBeat
	isRunning          bool
}

func (rf *Raft) timerRandomReset() {
	timeout := int64(rand.Intn(rangeTimeOutR-rangeTimeOutL) + rangeTimeOutR)
	rf.waitHeartBeatTimer.Reset(time.Duration(timeout * int64(time.Millisecond)))
}

// applyCommits apply from rf.lastApplied + 1 to applyEnd (include applyEnd)
func (rf *Raft) applyCommits(applyEnd int) { // real apply may spend a large time, so we use async func to apply it
	var apply ApplyMsg
	for {
		rf.mu.Lock() // get the applied entry
		if rf.lastApplied < applyEnd {
			rf.lastApplied += 1
			apply.Index = rf.lastApplied
			apply.Command = rf.log[rf.lastApplied].Command
		} else {
			apply.Command = nil
		}
		rf.mu.Unlock()
		if apply.Command == nil {
			return
		}
		rf.applyMsg <- apply // do apply, apply may take a long time, this implement is for portable
	}
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.leaderId == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//9
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

// RequestVoteArgs invoked by candidates to gather votes
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last entry
}

// RequestVoteReply the vote rpc result
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means the candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		if rf.leaderId == rf.me {
			rf.leaderId = -1
		}
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		} else if rf.votedFor == -1 {
			lastIndex := len(rf.log) - 1 // check safety (more update-to-date)
			if args.Term >= rf.log[lastIndex].Term || (args.Term == rf.log[lastIndex].Term && args.LastLogIndex >= lastIndex) {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			}
		}
	}
	if reply.VoteGranted { // if vote granted, reset the timer
		rf.timerRandomReset()
	}
	rf.mu.Unlock() // defer is a little slower than normal operation
}

// AppendEntriesArgs rpc argument for AppendEntries
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat, may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply rpc reply for AppendEntries
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.timerRandomReset()

	reply.Term = rf.currentTerm
	reply.Success = len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.leaderId == rf.me { // avoid multi leader
			rf.leaderId = -1 // become follower
		}
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.leaderId != args.LeaderId { // setup leader
		rf.leaderId = args.LeaderId
	}
	if args.Entries != nil && reply.Success { // not heart beat and match
		entryId := 0
		for args.PrevLogIndex++; args.PrevLogIndex < len(rf.log) && entryId < len(args.Entries); args.PrevLogIndex++ {
			rf.log[args.PrevLogIndex] = args.Entries[entryId] // replace old log item
			entryId++
		}
		for ; entryId < len(args.Entries); entryId++ {
			rf.log = append(rf.log, args.Entries[entryId]) // append the new item
		}
	}
	if reply.Success && args.LeaderCommit > rf.commitIndex { // update commit index, and match
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyCommits(rf.commitIndex) // apply these commits
	}
}

// sendAppendLogEntries send the updated log entries to others, if updateEnd==-1, send heart beat only
func (rf *Raft) sendAppendLogEntries(updateEnd int) { // if majority accept, update the commitId=updateEnd-1
	rf.sendHeartBeatTimer.Reset(heartBeatInterval)
	appendAllNum := len(rf.peers)
	appendResult := make(chan bool, appendAllNum)

	for i := 0; i < appendAllNum; i++ { // send to every peers
		if i == rf.me { // don't send to itself, just accept
			appendResult <- true
			rf.timerRandomReset()
			continue
		}
		go func(serverId int, logEnd int) { // send
			reply := &AppendEntriesReply{}
			var sendEntries []LogEntry
			thisTerm := rf.currentTerm
			for {
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[serverId] - 1
				if logEnd == -1 { // heart beat
					sendEntries = nil
				} else {
					if rf.leaderId != rf.me || rf.nextIndex[serverId] >= logEnd { // if other goroutine has sent, just return
						appendResult <- false
						rf.mu.Unlock()
						return
					}
					sendEntries = rf.log[rf.nextIndex[serverId]:logEnd]
				}
				args := AppendEntriesArgs{Entries: sendEntries,
					PrevLogIndex: prevLogIndex, PrevLogTerm: rf.log[prevLogIndex].Term,
					LeaderId: rf.me, Term: thisTerm, LeaderCommit: rf.commitIndex}
				rf.mu.Unlock()

				reply.Success = false
				if !rf.peers[serverId].Call("Raft.AppendEntries", args, reply) {
					appendResult <- false
					return
				}

				if reply.Term > rf.currentTerm || rf.leaderId != rf.me { // if other response term > currentTerm, convert to follower
					rf.mu.Lock()
					rf.leaderId = -1
					rf.timerRandomReset()
					rf.mu.Unlock()
					return
				}
				if logEnd == -1 {
					return
				}
				if reply.Success {
					appendResult <- reply.Success
					if logEnd != -1 {
						rf.nextIndex[serverId] = logEnd - 1  // todo: check overlap
						rf.matchIndex[serverId] = logEnd - 1 // update match index
					}
					return
				} else { // fail, but it does not because RPC
					rf.mu.Lock()
					logEnd = len(rf.log)            // the logEnd may be -1 (heart beat), if fail, we update the logEnd too.
					if rf.nextIndex[serverId] > 1 { // decrement, but must >=1
						rf.nextIndex[serverId] -= 1
					}
					rf.mu.Unlock()
				}
			}
		}(i, updateEnd)
	}

	if updateEnd == -1 { // just heartBeat
		return
	}
	// wait for result
	replyOk := 0
	for i := 0; i < appendAllNum && rf.leaderId == rf.me; i++ {
		if <-appendResult {
			replyOk += 2
			if replyOk > appendAllNum {
				newCommitIndex := updateEnd - 1
				rf.mu.Lock()
				if newCommitIndex > rf.commitIndex {
					rf.commitIndex = updateEnd - 1
				}
				rf.mu.Unlock()
				go rf.applyCommits(rf.commitIndex) // apply these commits
				break
			}
		}
	}
}

// run the main goroutine, for leader, follower and  candidate
func (rf *Raft) run() {
	for rf.isRunning {
		if rf.leaderId == rf.me { // leader
			select {
			case <-rf.sendHeartBeatTimer.C: // if timeout, send heartbeat
				rf.sendAppendLogEntries(-1)
			}
		} else { // follower or candidate
			select {
			case <-rf.waitHeartBeatTimer.C: // timeout! become candidate
				rf.mu.Lock()
				rf.leaderId = -1      // reset the lead
				rf.currentTerm += 1   // increment current term
				rf.votedFor = -1      // vote for reset (not for self to efficient
				rf.timerRandomReset() // reset election timer

				// start vote for self
				lastLogIndex := len(rf.log) - 1
				args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: rf.log[lastLogIndex].Term}
				rf.mu.Unlock()

				voteAllNum := len(rf.peers)
				voteResult := make(chan int, voteAllNum)
				for i := range rf.peers { // send request vote rpc to all servers
					go func(serverId int) {
						reply := &RequestVoteReply{VoteGranted: false}
						if rf.leaderId != -1 {
							voteResult <- 0
							return
						}
						if rf.peers[serverId].Call("Raft.RequestVote", args, reply) {
							// if received from new leader, convert to follower
							if reply.Term > rf.currentTerm || rf.currentTerm > args.Term || rf.leaderId != -1 {
								rf.mu.Lock()
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.mu.Unlock()
								voteResult <- -1 // -1 means fail
								return
							} else if reply.VoteGranted {
								voteResult <- 1 // 1 means get granted
								return
							}
						}
						voteResult <- 0 // 0 means be not granted
					}(i)
				}
				voteGrantedNum := 0 // collect vote results
				for i := 0; i < voteAllNum; i++ {
					select {
					case d := <-voteResult:
						if d == 1 { // get granted
							voteGrantedNum += 2
							if voteGrantedNum > voteAllNum { // stop early
								break
							}
						} else if d == -1 { // fail
							voteGrantedNum = 0
							break
						}
					case <-time.After(RPCTimeOut): // set RPC timeout to avoid RPC spending too long time
						break
					}
				}
				rf.mu.Lock()
				if voteGrantedNum > voteAllNum && rf.leaderId == -1 { // become leader
					rf.leaderId = rf.me
					go rf.sendAppendLogEntries(-1) // send heart beat immediately
					lastLogIndex := len(rf.log)
					for i := range rf.nextIndex { // reinitialized after election
						rf.nextIndex[i] = lastLogIndex
						rf.matchIndex[i] = 0
					}
				}
				rf.mu.Unlock()
			default:
				// follower
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	isLeader := rf.leaderId == rf.me
	term := rf.currentTerm
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command}) // save log to logEntries
		go rf.sendAppendLogEntries(len(rf.log))                         // send this logEntry to others, and commit
	}
	return index, term, isLeader // return immediately
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
	rf.applyMsg = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1) // the first index of log is 1
	rf.log[0].Term = 0
	rf.log[0].Command = nil
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.leaderId = -1
	rf.waitHeartBeatTimer = time.NewTimer(time.Second)
	rf.sendHeartBeatTimer = time.NewTimer(0)
	rf.timerRandomReset()
	rf.isRunning = true

	if rf.me == 0 { // set random seed only once
		rand.Seed(time.Now().UnixNano())
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run() // main goroutine
	return rf
}
