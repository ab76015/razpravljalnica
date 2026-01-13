package main

import (
	"flag"
	"log"

	control "github.com/ab76015/razpravljalnica/pkg/control"
)

func main() {
	addr := flag.String("addr", ":60051", "Control plane listen address")
	flag.Parse()

	if err := control.StartControl(*addr); err != nil {
		log.Fatal(err)
	}
}
