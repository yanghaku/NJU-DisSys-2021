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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// global timeout config
const heartBeatInterval = 100 * time.Millisecond
const electionTimeout = 300 * time.Millisecond
const RPCTimeOut = time.Second
const rangeTimeOutL = 300
const rangeTimeOutR = 600
const networkFailRetryTimes = 20
const ChannelBufSize = 1000

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
	lastSend           int           // (for leaders) last index to send AppendEntries to followers
	applyMsg           chan ApplyMsg // if committed, send msg to it
	leaderId           int           // the leader's id
	waitHeartBeatTimer chan bool     // wait for heart beat timer, if timeout, become candidate
	isKilled           chan bool
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	isLead := rf.me == rf.leaderId
	rf.mu.Unlock()
	return isLead
}

// applyCommits apply from rf.lastApplied + 1 to applyEnd (include applyEnd)
func (rf *Raft) applyCommits(applyEnd int) {
	var apply ApplyMsg
	rf.mu.Lock()
	rf.persist()
	for rf.lastApplied < applyEnd { // get apply entry
		rf.lastApplied += 1
		apply.Index = rf.lastApplied
		apply.Command = rf.log[apply.Index].Command
		rf.applyMsg <- apply // notify apply entry
	}
	rf.mu.Unlock()
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
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data != nil {
		d := gob.NewDecoder(bytes.NewBuffer(data))
		_ = d.Decode(&rf.currentTerm)
		_ = d.Decode(&rf.votedFor)
		_ = d.Decode(&rf.log)
	} else {
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 1) // the first index of log is 1
		rf.log[0] = LogEntry{Term: -1, Command: nil}
	}
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
	if args.Term > rf.currentTerm { // every server must check this condition, if true, become follower
		rf.currentTerm = args.Term // update its term
		rf.votedFor = -1           // if term changed, the voteFor must reset
		if rf.leaderId == rf.me {  // avoid multi leaders
			rf.leaderId = -1
		}
	}
	if rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
	} else if rf.votedFor == -1 {
		lastIndex := len(rf.log) - 1 // check safety (more update-to-date)
		reply.VoteGranted = args.LastLogTerm > rf.log[lastIndex].Term || (args.LastLogTerm == rf.log[lastIndex].Term && args.LastLogIndex >= lastIndex)
	}
	if reply.VoteGranted { // if vote granted, reset the timer
		rf.votedFor = args.CandidateId
		rf.waitHeartBeatTimer <- false
	}
	rf.mu.Unlock() // defer is a little slower than normal operation
}

// sendRequestVote become candidate, and send RequestVote RPC to all others, result will save to voteResult (only use for follower!!)
// return true if the candidate become leader
func (rf *Raft) sendRequestVote() bool {
	rf.mu.Lock()                   // conversion stage (convert to candidate)
	rf.leaderId = -1               // reset the lead
	rf.currentTerm += 1            // increment current term
	rf.votedFor = -1               // vote for reset (not for self to efficient
	rf.waitHeartBeatTimer <- false // reset election timer
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: rf.log[lastLogIndex].Term}
	rf.mu.Unlock()
	voteAllNum := len(rf.peers)
	voteResult := make(chan bool, voteAllNum)
	for i := 0; i < voteAllNum; i++ { // send request vote rpc to all servers
		go func(serverId int) {
			reply := &RequestVoteReply{VoteGranted: false}
			if rf.peers[serverId].Call("Raft.RequestVote", args, reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm { // change to new term and become follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1 // if term changed, the voteFor must reset
				}
				rf.mu.Unlock()
			}
			voteResult <- reply.VoteGranted // send result to the chan
		}(i)
	}

	voteGrantedNum := 0 // collect vote results
	voteNotGrantedNum := 0
	for i := 0; i < voteAllNum; i++ {
		select { // wait for vote results
		case d := <-voteResult:
			if d { // get granted
				voteGrantedNum += 2
			} else {
				voteNotGrantedNum += 2
			}
			if voteGrantedNum > voteAllNum || voteNotGrantedNum > voteAllNum { // stop early
				break
			}
		case <-time.After(electionTimeout): // set RPC timeout to avoid RPC spending too long time
			break
		}
	}
	return voteGrantedNum > voteAllNum
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
	rf.waitHeartBeatTimer <- false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm // must set reply term firstly
	if args.Term < rf.currentTerm || len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false // reply false if term < currentTerm or rf.log doesn't container the matched PrevLogTerm
		return
	}
	reply.Success = true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.leaderId == rf.me { // avoid multi leader
			rf.leaderId = -1 // become follower
		}
	}
	if rf.leaderId != args.LeaderId { // setup leader
		rf.leaderId = args.LeaderId
	}
	if args.Entries != nil { // not heart beat, append the args.Entries to logs
		entryId := 0
		for args.PrevLogIndex++; args.PrevLogIndex < len(rf.log) && entryId < len(args.Entries); args.PrevLogIndex++ {
			rf.log[args.PrevLogIndex] = args.Entries[entryId] // replace old log item
			entryId++
		}
		if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex-1].Term > rf.log[args.PrevLogIndex].Term {
			rf.log = rf.log[:args.PrevLogIndex] // remove the old entry
		}
		for ; entryId < len(args.Entries); entryId++ {
			rf.log = append(rf.log, args.Entries[entryId]) // append the new item
		}
	}
	if args.LeaderCommit > rf.commitIndex { // update commit index, and match
		newCommitIndex := args.LeaderCommit
		if newCommitIndex > len(rf.log)-1 { // not more than rf.log length
			newCommitIndex = len(rf.log) - 1
			reply.Success = false // if now log is less, return false to fetch new logs
		}
		if newCommitIndex != rf.commitIndex { // if commit index changed, apply it
			rf.commitIndex = newCommitIndex
			go rf.applyCommits(newCommitIndex) // apply these commits
		}
	}
}

// sendAppendEntries send the updated log entries to others, the heartbeat also contains entries
func (rf *Raft) sendAppendEntries() { // if majority accept, update the commitId=updateEnd-1
	rf.mu.Lock()
	updateEnd := len(rf.log)
	isHeartBeat := updateEnd == rf.lastSend // if lastSend==len(rf.log), just send heartbeat
	rf.lastSend = updateEnd
	thisTerm := rf.currentTerm
	rf.mu.Unlock()
	appendAllNum := len(rf.peers)
	appendResult := make(chan bool, appendAllNum)

	for i := 0; i < appendAllNum; i++ { // send to every peers
		if i == rf.me { // don't send to itself, just accept
			appendResult <- true
			rf.waitHeartBeatTimer <- false
			continue
		}
		go func(serverId int, logEnd int) { // send
			reply := &AppendEntriesReply{Success: false}
			var sendEntries []LogEntry = nil
			sendRollBack := 1 // if it doesn't matched, must decrement
			for {
				rf.mu.Lock()
				if rf.leaderId != rf.me {
					rf.mu.Unlock()
					break
				}
				prevLogIndex := rf.nextIndex[serverId] - sendRollBack
				if prevLogIndex < 0 { // prevLogIndex must >=0
					prevLogIndex = 0
				}
				next := prevLogIndex + 1
				if next < logEnd { // not heart beat
					sendEntries = rf.log[next:logEnd]
				}
				args := AppendEntriesArgs{Entries: sendEntries,
					PrevLogIndex: prevLogIndex, PrevLogTerm: rf.log[prevLogIndex].Term,
					LeaderId: rf.me, Term: thisTerm, LeaderCommit: rf.commitIndex}
				rf.mu.Unlock()

				retryTimes := networkFailRetryTimes
				retryWaitTime := time.Nanosecond
				for retryTimes >= 0 && rf.isLeader() && !rf.peers[serverId].Call("Raft.AppendEntries", args, reply) { // if rpc fail, retry
					retryTimes -= 1
					retryWaitTime <<= 1
					time.Sleep(retryWaitTime)
				}

				if reply.Term > thisTerm { // if other response term > currentTerm, convert to follower
					rf.mu.Lock()
					if rf.leaderId == rf.me {
						rf.leaderId = -1
						if reply.Term > rf.currentTerm { // if Term changed, reset the voteFor
							rf.currentTerm = reply.Term
							rf.votedFor = -1
						}
					}
					rf.mu.Unlock()
					reply.Success = false
					break
				}
				if reply.Success { // success
					if next < logEnd { // if not heart beat
						rf.mu.Lock()
						if rf.nextIndex[serverId] < logEnd-1 { // update next index and match index
							rf.nextIndex[serverId] = logEnd - 1
							rf.matchIndex[serverId] = logEnd - 1
						}
						rf.mu.Unlock()
					}
					break
				} else { // fail, but it does not because RPC, (maybe because of not match)
					sendRollBack <<= 3
				}
			}
			appendResult <- reply.Success
		}(i, updateEnd)
	}

	// wait for result
	replyOk := 0
	for i := 0; i < appendAllNum; i++ {
		select { // wait for send results
		case r := <-appendResult:
			if isHeartBeat && i > 1 { // heart beat
				return
			}
			if !isHeartBeat && r { // not heart beat
				replyOk += 2
				if replyOk > appendAllNum { // if the majority of servers accept, commit it
					newCommitIndex := updateEnd - 1
					rf.mu.Lock()
					if rf.leaderId == rf.me && newCommitIndex > rf.commitIndex && rf.log[newCommitIndex].Term == rf.currentTerm { // for fig 8
						rf.commitIndex = newCommitIndex
						go rf.applyCommits(newCommitIndex) // apply these commits
					}
					rf.mu.Unlock()
					return
				}
			}
		case <-time.After(RPCTimeOut):
			rf.mu.Lock()
			if i == 1 && rf.leaderId == rf.me { // only receive the response from itself, so the leader is disconnect from network
				rf.leaderId = -1 // convert to follower
			}
			rf.mu.Unlock()
			return
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
	rf.isKilled <- true
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.leaderId = -1
	rf.waitHeartBeatTimer = make(chan bool, ChannelBufSize)
	rf.waitHeartBeatTimer <- false
	rf.isKilled = make(chan bool)
	if rf.me == 0 { // set random seed only once
		rand.Seed(time.Now().UnixNano())
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() { // main goroutine
		for {
			if rf.isLeader() { // leader
				select {
				case <-rf.isKilled: // if is killed, stop the main goroutine
					return
				case <-time.After(heartBeatInterval): // leader only wait sendHeartBeatTimer, if timeout, send heartBeat
					go rf.sendAppendEntries() // if timeout, send heartbeat
				}
			} else { // follower or candidate
				select {
				case <-rf.isKilled: // if is killed, stop the main goroutine
					return
				case <-time.After(time.Duration(int64(rand.Intn(rangeTimeOutR-rangeTimeOutL)+rangeTimeOutR)) * time.Millisecond):
					// timeout! process conversion stage to convert to candidate, and send requestVotes, try to become leader
					becomeLeader := rf.sendRequestVote()
					rf.mu.Lock()
					if becomeLeader && rf.leaderId == -1 { // become leader
						rf.leaderId = rf.me
						lastLogIndex := len(rf.log)
						rf.lastSend = lastLogIndex
						for i := range rf.nextIndex { // reinitialized after election
							rf.nextIndex[i] = lastLogIndex
							rf.matchIndex[i] = 0
						}
						go rf.sendAppendEntries() // send heart beat immediately
					}
					rf.mu.Unlock()
				case <-rf.waitHeartBeatTimer: // do nothing
				}
			}
		}
	}()
	return rf
}
