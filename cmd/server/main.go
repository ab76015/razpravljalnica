package main

import (
	"flag"
	"log"

	server "github.com/ab76015/razpravljalnica/pkg/server"
)

func main() {
	host := flag.String("host", "localhost", "Server host")
	port := flag.String("port", "50051", "Server port")
	control := flag.String("control", "localhost:60051", "Control plane address")
	nodeID := flag.String("node_id", "", "Unique node ID")

	flag.Parse()

	if err := server.StartNode(*nodeID, *host, *port, *control); err != nil {
		log.Fatal(err)
	}
}
