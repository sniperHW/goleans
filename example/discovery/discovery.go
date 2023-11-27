package main

import (
	"encoding/json"
	"flag"
	"os"

	"github.com/sniperHW/clustergo/example/discovery"
)

func main() {

	addr := flag.String("addr", "127.0.0.1:8110", "address of discovery")
	configPath := flag.String("membership", "membership.json", "membership")
	flag.Parse()

	var config []*discovery.Node
	f, err := os.Open(*configPath)
	if err != nil {
		panic(err)
	}

	decoder := json.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		panic(err)
	}

	svr := discovery.NewServer()

	svr.Start(*addr, config)

}
