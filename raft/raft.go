package raft

import (
	"math/rand/v2"
	"raft-kv-store/rpc"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// Node represents a single Raft server in a cluster.
type Node struct {
	mu sync.Mutex

	id      int
	peers   []int
	network rpc.Network

	role Role

	resetElectionChannel chan struct{}
	electionTimer        *time.Timer
	timeoutLower         int
	timeoutUpper         int

	// Persistent state (all servers)
	currentTerm int
	votedFor    *int
	log         []rpc.LogEntry

	// Volatile state (all servers)
	commitIndex int
	lastApplied int

	// Volatile state (leaders only)
	nextIndex  map[int]int
	matchIndex map[int]int
}

// CreateNode creates and initializes a new Raft node with the given ID.
func CreateNode(id int) *Node {
	return &Node{
		id:                   id,
		role:                 Follower,
		resetElectionChannel: make(chan struct{}, 1),
		log:                  []rpc.LogEntry{{Term: 0, Index: 0}},
	}
}

// SetPeers sets the list of all cluster node IDs including this node
func (node *Node) SetPeers(peers []int) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peers = peers
}

// SetNetwork sets the RPC network for this node.
func (node *Node) SetNetwork(network rpc.Network) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.network = network
}

// SetTimeoutRange sets the election timeout range in milliseconds.
func (node *Node) SetTimeoutRange(lower, upper int) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.timeoutLower = lower
	node.timeoutUpper = upper
}

// GetRole returns the current role of the node.
func (node *Node) GetRole() Role {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.role
}

// GetCurrentTerm returns the node's current term.
func (node *Node) GetCurrentTerm() int {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.currentTerm
}

// GetLogLength returns the node's current log length.
func (node *Node) GetLogLength() int {
	node.mu.Lock()
	defer node.mu.Unlock()
	return len(node.log)
}

// RunElectionTimer starts the election timer loop
func (node *Node) RunElectionTimer() {
	node.runElectionTimer()
}

// HandleAppendEntries processes an AppendEntries RPC from a leader.
// It updates the node's state based on the leader's entries and commit index,
// returning the current term and success status.
func (node *Node) HandleAppendEntries(args rpc.AppendEntriesArgs) rpc.AppendEntriesReply {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isStaleTerm(args.Term) {
		return rpc.AppendEntriesReply{Term: node.currentTerm, Success: false}
	}

	node.updateTermAndRole(args.Term)

	select {
	case node.resetElectionChannel <- struct{}{}:
	default:
	}

	if !node.checkPrevEntry(rpc.LogEntry{Term: args.PrevLogTerm, Index: args.PrevLogIndex, Command: nil}) {
		return rpc.AppendEntriesReply{Term: node.currentTerm, Success: false}
	}

	node.appendEntries(args)
	node.applyEntries()

	return rpc.AppendEntriesReply{Term: node.currentTerm, Success: true}
}

// HandleRequestVote processes a RequestVote RPC from a candidate during elections.
// It votes for the candidate if the candidate's log is up to date and we haven't voted yet.
func (node *Node) HandleRequestVote(args rpc.RequestVoteArgs) rpc.RequestVoteReply {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isStaleTerm(args.Term) {
		return rpc.RequestVoteReply{Term: node.currentTerm}
	}

	node.updateTermAndRole(args.Term)

	if node.votedFor == nil {
		if node.checkUpToDate(args) {
			node.votedFor = &args.CandidateID

			select {
			case node.resetElectionChannel <- struct{}{}:
			default:
			}

			return rpc.RequestVoteReply{Term: node.currentTerm, VoteGranted: true}
		}
	} else if *node.votedFor == args.CandidateID {
		return rpc.RequestVoteReply{Term: node.currentTerm, VoteGranted: true}
	}
	return rpc.RequestVoteReply{Term: node.currentTerm, VoteGranted: false}
}

// runElectionTimer runs the election timer loop in a background goroutine.
// It starts elections when the timeout expires and resets the timer on heartbeats.
func (node *Node) runElectionTimer() {
	timeout := node.randomElectionTimeout()
	node.electionTimer = time.NewTimer(timeout)
	for {
		select {
		case <-node.electionTimer.C:
			node.mu.Lock()
			if node.role != Leader {
				node.mu.Unlock()
				node.resetElectionTimer(node.randomElectionTimeout())
				node.startElection()
			} else {
				node.mu.Unlock()
			}

		case <-node.resetElectionChannel:
			node.resetElectionTimer(node.randomElectionTimeout())
		}
	}
}

// resetElectionTimer stops the current election timer and starts a new one with the given duration.
func (node *Node) resetElectionTimer(duration time.Duration) {
	if !node.electionTimer.Stop() {
		select {
		case <-node.electionTimer.C:
		default:
		}
	}
	node.electionTimer.Reset(duration)
}

// randomElectionTimeout returns a random election timeout between timeoutLower and timeoutUpper milliseconds.
func (node *Node) randomElectionTimeout() time.Duration {
	random := rand.IntN(node.timeoutUpper - node.timeoutLower)
	return time.Duration(random+node.timeoutLower) * time.Millisecond
}

// startElection initiates an election by incrementing the term and requesting votes from peers.
// It waits for votes to be collected and transitions to leader if a majority is reached.
func (node *Node) startElection() {
	node.mu.Lock()
	node.currentTerm++
	node.role = Candidate
	node.votedFor = &node.id
	electionTerm := node.currentTerm
	lastLogIndex := len(node.log) - 1
	lastLogTerm := node.log[lastLogIndex].Term
	node.mu.Unlock()

	voteCount := 1

	args := rpc.RequestVoteArgs{
		Term:         electionTerm,
		CandidateID:  node.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	majority := len(node.peers)/2 + 1
	if voteCount >= majority {
		node.mu.Lock()
		node.role = Leader
		node.mu.Unlock()
		return
	}

	for _, id := range node.peers {
		if id == node.id {
			continue
		}
		go func(id int) {
			reply := node.network.SendRequestVote(id, args)

			node.mu.Lock()
			defer node.mu.Unlock()

			if node.role != Candidate || node.currentTerm != electionTerm {
				return
			}

			if reply.Term > node.currentTerm {
				node.updateTermAndRole(reply.Term)
				return
			}

			if reply.VoteGranted {
				voteCount++
				if voteCount >= majority {
					node.role = Leader
					node.initializeLeader()
				}
			}
		}(id)
	}
}

func (node *Node) initializeLeader() {
	node.nextIndex = make(map[int]int)
	node.matchIndex = make(map[int]int)

	term := node.currentTerm

	for _, id := range node.peers {
		if id == node.id {
			continue
		}

		node.nextIndex[id] = len(node.log)
		node.matchIndex[id] = 0

		//go func(id int) {
		//	for {
		//		node.mu.Lock()
		//		if node.role != Leader {
		//			return
		//		}
		//		node.mu.Unlock()
		//
		//	}
		//}(id)
	}

	node.log = append(node.log, rpc.LogEntry{Term: term, Index: len(node.log), Command: nil})
}

// appendEntries appends new entries from the leader to the node's log.
func (node *Node) appendEntries(args rpc.AppendEntriesArgs) {
	i := 0
	for ; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i

		if index >= len(node.log) {
			break
		}

		if node.log[index].Term != args.Entries[i].Term {
			node.log = node.log[:index]
			break
		}
	}

	node.log = append(node.log, args.Entries[i:]...)

	lastNewIndex := args.PrevLogIndex + len(args.Entries)

	if args.LeaderCommit > node.commitIndex {
		node.commitIndex = min(args.LeaderCommit, lastNewIndex)
	}
}

// applyEntries applies committed log entries to the state machine.
// It moves the lastApplied pointer up to the commitIndex.
func (node *Node) applyEntries() {
	for node.lastApplied < node.commitIndex {
		node.lastApplied++
		// TODO apply(node.log[node.lastApplied])
	}
}

// checkPrevEntry verifies that the previous log entry matches expectations.
// This is used to check if the node can accept new entries from a leader.
func (node *Node) checkPrevEntry(prev rpc.LogEntry) bool {
	if prev.Index >= len(node.log) {
		return false
	}
	if node.log[prev.Index].Term != prev.Term {
		return false
	}
	return true
}

// checkUpToDate determines if a candidate's log is at least as up-to-date as this node's log.
// A log is more up to date if its final entry has a larger term or the same term with a greater or equal index.
func (node *Node) checkUpToDate(args rpc.RequestVoteArgs) bool {
	lastTerm := node.log[len(node.log)-1].Term

	if args.LastLogTerm > lastTerm {
		return true
	} else if args.LastLogTerm < lastTerm {
		return false
	}
	return args.LastLogIndex >= len(node.log)-1
}

// isStaleTerm checks if the given term is stale (older than the current term).
func (node *Node) isStaleTerm(term int) bool {
	if term < node.currentTerm {
		return true
	}
	return false
}

// updateTermAndRole updates the node's term and reverts to follower if a higher term is seen.
func (node *Node) updateTermAndRole(term int) {
	if term > node.currentTerm {
		node.role = Follower
		node.votedFor = nil
		node.currentTerm = term
	}
}
