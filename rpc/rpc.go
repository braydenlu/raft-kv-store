package rpc

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RaftNode interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
}

type Transport interface {
	SendRequestVote(to int, args RequestVoteArgs) RequestVoteReply
	SendAppendEntries(to int, args AppendEntriesArgs) AppendEntriesReply
}

type Network struct {
	Nodes map[int]RaftNode
}

func (net *Network) SendRequestVote(to int, args RequestVoteArgs) RequestVoteReply {
	return net.Nodes[to].HandleRequestVote(args)
}

func (net *Network) SendAppendEntries(to int, args AppendEntriesArgs) AppendEntriesReply {
	return net.Nodes[to].HandleAppendEntries(args)
}
