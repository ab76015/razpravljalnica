package main

import (
	"flag"
	"log"

	client "github.com/ab76015/razpravljalnica/pkg/client"
)

func main() {
	addr := flag.String("addr", "localhost:60051", "Data node address")
	flag.Parse()

	if err := client.Run(*addr); err != nil {
		log.Fatal(err)
	}

}
