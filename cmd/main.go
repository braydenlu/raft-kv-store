package main

import (
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
	time.Sleep(5 * time.Second)
}
