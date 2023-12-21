package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/sniperHW/goleans/example/placement"

	"go.uber.org/zap"
)

func main() {

	addr := flag.String("addr", "127.0.0.1:8110", "address of pd")
	flag.Parse()

	placement.InitLogger(zap.NewExample())

	svr, err := placement.NewServer("./storage.json")

	if err != nil {
		panic(err)
	}

	err = svr.Start(*addr)

	if err != nil {
		panic(err)
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT) //监听指定信号
	_ = <-c
}
