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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"
import "sort"
import "bytes"
import "labgob"


// import "bytes"
// import "labgob"

const Leader = 0
const Candidate = 1
const Follower = 2

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg         // Used to apply logs
	state int                     // Indicates the status of this server 
	currentTerm int               // Current term this server is in   
	logs []Log                   // Sequence of logs 
	votedFor int                  // Server id that current server vote for within current term

	appendChan chan int           // Append channel used to inform appendRpc received
	voteChan chan int             // Vote channel used to inform request vote received
	becomeLeaderChan chan int     // Indicate the server now becomes the leader
	exitCh chan int               // Used to exit

	commitIndex int               // Commit index
	lastApplied int               // Index of highest log entry applied to state machine

	nextIndex []int               // for each server, index of the next log entry to send to that server
	matchIndex []int              // for each server, index of highest log entry known to be replicated on server
}

// Log structure to store operation
type Log struct {
	Term int
	Index int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
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
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil{
		fmt.Printf("Error\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}




// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int 
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

// AppendEntries RPC structure.
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

    // Grant lock to avoid race condition
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    fmt.Printf("Server %v receive vote request from server %v\n", rf.me, args.CandidateId)

    reply.Term = rf.currentTerm
	reply.VoteGranted = false

	signal := false

    // If current term is larger that of candidate's, the candidate is out of date, 
    // return false.
	if rf.currentTerm > args.Term {
		fmt.Printf("Server %v denied vote request from Server %v because of term lower issue\n", rf.me, args.CandidateId)
		return
	}

    // If term of args is larger than current term, this server is out-of-date, then it 
    // reset current term and its votedFor variable
	if rf.currentTerm < args.Term {
		if rf.state == Leader {
			fmt.Printf("Server %v received higher term num and backs to Follower\n", rf.me)
		}
		rf.backToFollower(args.Term, -1)
		signal = true
		rf.voteChan <- 1
	}

    // If current server has voted within this term and the voted server is 
    // not the candidate, return false
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		fmt.Printf("Server %v denied vote request from Server %v because of voted before\n", rf.me, args.CandidateId)
		return
	}

    // Election restriction
	if (args.LastLogTerm > rf.getLastLogTerm() || 
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		reply.VoteGranted = true
		rf.backToFollower(rf.currentTerm, args.CandidateId)
		fmt.Printf("Server %v votes for Server %v at term %v.\n", rf.me, args.CandidateId, rf.currentTerm)
		if !signal {
			rf.voteChan <- 1
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		fmt.Printf("Server %v refuse appendEntry from server %v because it's outdated \n", rf.me, args.LeaderId)
		return
	} 

	if args.Term > rf.currentTerm {
		fmt.Printf("Server %v converts back to follower because received higher term in appendEntry from server %v\n", rf.me, args.LeaderId)
		rf.backToFollower(args.Term, -1)
	}

    // Inform main thread to start another timing
	rf.appendChan <- 1

	if args.PrevLogIndex >= len(rf.logs) {
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = 0
		return
	} else {
		if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
			for i := 1; i < len(rf.logs); i++ {
				if rf.logs[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
			return
		}

		reply.Success = true
		index := args.PrevLogIndex
		for i := 0; i < len(args.Entries); i++ {
			index += 1
			if index > rf.getLastLogIndex() {
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}

			if rf.logs[index].Term != args.Entries[i].Term {
				for len(rf.logs) > index {
					fmt.Printf("Server %v deletes outdated log %v, Current term %v; Send Server %v, send term %v\n", rf.me, index, rf.currentTerm, args.LeaderId, args.Term)
					rf.logs = rf.logs[0 : index]

				}
				rf.logs = append(rf.logs, args.Entries[i])
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = findMin(args.LeaderCommit, rf.getLastLogIndex())
			fmt.Printf("Server %v updates its commitIndex to %v. loca1\n", rf.me, rf.commitIndex)
		}
	} 

	rf.applyLogs()
}

// Apply newly committed logs
func (rf *Raft) applyLogs() {

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied = rf.lastApplied + 1
		applyMsg := ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied}
		fmt.Printf("Server %v applied log. Index %v, Term %v\n", rf.me, rf.logs[rf.lastApplied].Index, rf.logs[rf.lastApplied].Term)
		rf.applyCh <- applyMsg
	}
}

func findMin(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

func findMax(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// Return last log term
func (rf *Raft) getLastLogTerm() int {
	if rf.getLastLogIndex() == 0 {
		return -1
	}
	return rf.logs[rf.getLastLogIndex()].Term
}

// Return last log index
func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
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

func (rf *Raft) sendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == Leader
	index := -1
	
	if isLeader {
		index = rf.getLastLogIndex() + 1
	}

	if isLeader {
		rf.logs = append(rf.logs, Log{term, index, command})
		rf.persist()
		fmt.Printf("Server %v add log index %v\n", rf.me, index)
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
	fmt.Printf("Server %v is killed.\n", rf.me)
	rf.exitCh <- 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0   
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{})
	rf.votedFor = -1

    rf.applyCh = applyCh
    rf.appendChan = make(chan int)
    rf.voteChan = make(chan int)
    rf.becomeLeaderChan = make(chan int)
    rf.exitCh = make(chan int)

    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

    // initialize from state persisted before a crash

    rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

    go func() {

    Loop:

    	for {

    		select {
			case <-rf.exitCh:
				break Loop
			default:
			}

    		// Generate random timeout for each for loop
    		timeOut := rand.Intn(150) + 300

    		switch rf.state{
    		case Follower:
    			select {
    			case <-time.After(time.Duration(timeOut) * time.Millisecond):
    				rf.mu.Lock()
    				rf.state = Candidate
    				rf.mu.Unlock()
    			case <-rf.appendChan:
    		    case <-rf.voteChan:
    		    }

    		case Candidate:
    			fmt.Printf("Server %v becomes Candidate, term is %v\n", rf.me, rf.currentTerm)
    			go rf.startLeaderElection()
    			select {
    			case <-time.After(time.Duration(timeOut) * time.Millisecond):
    			case <-rf.appendChan:
    		    case <-rf.voteChan:
    		    case <-rf.becomeLeaderChan:
    		    }

    		case Leader:
    			go rf.startAppendEntries()
    			select {
    			case <-time.After(100 * time.Millisecond):
    			case <-rf.appendChan:
    			case <-rf.voteChan:
    			}
    		}
        }
    }()

	return rf
}

func (rf *Raft) startLeaderElection () {

	rf.mu.Lock()

	// Increment current term to begin an election
	rf.currentTerm = rf.currentTerm + 1

	// Vote for self
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm

	rf.persist()

	request := RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogIndex(), rf.getLastLogTerm()}
	rf.mu.Unlock()

	fmt.Printf("Server %v start election, term is %v. \n", rf.me, rf.currentTerm)

	var countMutex sync.Mutex 
	voteCount := 0

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		go func (server int) {

			reply := RequestVoteReply{}
			res := rf.sendRequestVote(server, &request, &reply)

            // If did not reveive a valid response, return
			if !res {
				return
			}

			// If this server is out-of-date, convert back to Follower
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.backToFollower(reply.Term, -1)
				rf.voteChan <- 1
				return
			}

            // If receive votes, update voteCount and check if wins the election
			if reply.VoteGranted {
				countMutex.Lock()
				voteCount = voteCount + 1
				countMutex.Unlock()

	            if rf.state == Candidate && rf.currentTerm == currentTerm && voteCount >= len(rf.peers) / 2 {
	            	fmt.Printf("Server %v becomes the leader, term is %v, lastLogIndex is %v, commitIndex is %v\n", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.commitIndex)
	            	rf.convertToLeader()
		            rf.becomeLeaderChan <- 1
		        } 

			}

		}(i)
	}
}

func (rf *Raft) convertToLeader() {
	
	defer rf.persist()

	rf.votedFor = -1
	rf.state = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}

	rf.matchIndex = make([]int, len(rf.peers)) 

}

func (rf *Raft) startAppendEntries () {

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		go func (server int) {

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
            
			request := AppendEntriesArgs{}
			request.Term = rf.currentTerm
			request.LeaderId = rf.me
			request.PrevLogIndex = rf.getPrevLogIndex(server)
			request.PrevLogTerm = rf.getPrevLogTerm(server)
			entries := make([]Log, 0)
			request.Entries = append(entries, rf.logs[rf.nextIndex[server]:]...)
			request.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()

			reply := AppendEntriesReply{}

			valid := rf.sendEntries(server, &request, &reply)

			if !valid {
				return 
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.backToFollower(reply.Term, -1)
				rf.appendChan <- 1
				return
			}

			if rf.state != Leader || rf.currentTerm != request.Term {
				return
			}
            
			if reply.Success {
				rf.matchIndex[server] = request.PrevLogIndex + len(request.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.updateCommitIndex()
			}else {
				nextIndex := reply.ConflictIndex
				for i := 1; i < len(rf.logs); i++ {
					entry := rf.logs[i]
					if entry.Term == reply.ConflictTerm {
						nextIndex = i + 1
					}
				}
				rf.nextIndex[server] = findMax(1, nextIndex)
			}

		}(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	commitTmp := make([]int, len(rf.matchIndex))
	copy(commitTmp, rf.matchIndex)
	commitTmp[rf.me] = len(rf.logs) - 1

	sort.Ints(commitTmp)

	updatedIndex := commitTmp[len(rf.peers)/2]

	if updatedIndex > rf.commitIndex && rf.logs[updatedIndex].Term == rf.currentTerm {
		rf.commitIndex = updatedIndex
		fmt.Printf("Server %v updates its commitIndex to %v. loca2\n", rf.me, rf.commitIndex)
		rf.applyLogs()
	}
}

func (rf *Raft) getPrevLogIndex (server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm (server int) int {
	return rf.logs[rf.getPrevLogIndex(server)].Term
}

func (rf *Raft) backToFollower (term int, votedFor int) {
	defer rf.persist()
	
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.state = Follower
}

