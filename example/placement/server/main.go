package main

import (
	"flag"
	"goleans/example/placement"
)

func main() {

	addr := flag.String("addr", "127.0.0.1:8110", "address of pd")

	svr, err := placement.NewServer("./storage.json")

	if err != nil {
		panic(err)
	}

	err = svr.Start(*addr)

	if err != nil {
		panic(err)
	}
}
