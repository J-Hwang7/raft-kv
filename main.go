package main

import (
	"flag"
	"strings"

	"https://github.com/J-Hwang7/raft-kv"
)

func main() {
	id := flag.Int("id", 0, "this node's 0-indexed ID")
	peerList := flag.String(
		"peers",
		"localhost:8000,localhost:8001,localhost:8002",
		"comma-separated addresses of all nodes",
	)
	httpAddr := flag.String("http", "", "HTTP API address (e.g. localhost:9000)")
	flag.Parse()

	peers := strings.Split(*peerList, ",")

	node := raft.NewRaftNode(*id, peers)
	if err := node.StartServer(); err != nil {
		panic(err)
	}

	if *httpAddr != "" {
		node.StartHTTP(*httpAddr)
	}

	go node.Run()
	select {}
}