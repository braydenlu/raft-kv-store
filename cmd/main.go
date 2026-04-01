package main

import (
	"fmt"
	"log"
	"raft-kv-store/raft"
	"raft-kv-store/rpc"
	"sync"
	"time"
)

func main() {
	nodeIDs := []int{0, 1, 2}

	nodes := make(map[int]*raft.Node)
	for _, id := range nodeIDs {
		node := raft.CreateNode(id)
		node.SetPeers(nodeIDs)
		node.SetTimeoutRange(150, 300)
		nodes[id] = node
	}

	network := rpc.Network{
		Nodes: make(map[int]rpc.RaftNode),
	}

	for id, node := range nodes {
		network.Nodes[id] = node
	}

	for _, node := range nodes {
		node.SetNetwork(network)
	}

	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *raft.Node) {
			defer wg.Done()
			n.RunElectionTimer()
		}(node)
	}

	log.Println("Cluster started with 3 nodes. Waiting for election completion")
	time.Sleep(1 * time.Second)

	fmt.Println("Cluster State")
	for _, id := range nodeIDs {
		node := nodes[id]
		fmt.Printf("Node %d: Role=%s, Term=%d\n", id, roleToString(node.GetRole()), node.GetCurrentTerm())
	}

	time.Sleep(1 * time.Second)
	fmt.Println("Cluster State")
	for _, id := range nodeIDs {
		node := nodes[id]
		fmt.Printf("Node %d: Role=%s, Term=%d\n", id, roleToString(node.GetRole()), node.GetCurrentTerm())
	}
}

func roleToString(role raft.Role) string {
	switch role {
	case raft.Follower:
		return "Follower"
	case raft.Candidate:
		return "Candidate"
	case raft.Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}
