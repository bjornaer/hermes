package cluster

import "fmt"

const initialPeerAddr = 8008

type Cluster struct {
	Nodes []string
}

func NewCluster(peers ...int) *Cluster {
	port := initialPeerAddr
	nodes := []string{}
	if len(peers) == 0 {
		peers[0] = 1
	}
	for i := 0; i < peers[0]; i++ {
		peer := fmt.Sprintf("http://localhost:%d", port+i)
		nodes = append(nodes, peer)
	}
	return &Cluster{Nodes: nodes}
}
