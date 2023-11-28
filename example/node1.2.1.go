package main

import (
	"context"
	"flag"
	"goleans"
	"goleans/example/placement"
	"goleans/pd"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goleans/example/grain/user/rpc/service/echo"
	"goleans/example/grain/user/rpc/service/test"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/example/discovery"
	"github.com/sniperHW/clustergo/logger/zap"
)

func main() {

	pdAddr := flag.String("pdAddr", "127.0.0.1:8110", "address of pd")
	discoveryAddr := flag.String("discoveryAddr", "127.0.0.1:8110", "address of discovery")
	flag.Parse()

	l := zap.NewZapLogger("1.2.1.log", "./logfile", "debug", 1024*1024*100, 14, 28, true)
	placement.InitLogger(l)
	clustergo.InitLogger(l.Sugar())
	goleans.InitLogger(l.Sugar())
	localaddr, _ := addr.MakeLogicAddr("1.2.1")
	discoveryCli := discovery.NewClient(*discoveryAddr)
	pdClient := placement.NewCli(localaddr, *pdAddr)
	goleans.Start(discoveryCli, localaddr, pdClient, nil) //不作为silo,工厂函数填nil

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := echo.Call(ctx, pd.GrainIdentity("sniperHW1@User"), &echo.Request{
				Msg: "hello sniperHW1",
			})
			cancel()
			if err == nil {
				l.Sugar().Debug(resp.Msg)
			} else {
				l.Sugar().Debug(err)
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := test.Call(ctx, pd.GrainIdentity("sniperHW2@User"), &test.Request{
				Msg: "hello sniperHW2",
			})
			cancel()
			if err == nil {
				l.Sugar().Debug(resp.Msg)
			} else {
				l.Sugar().Debug(err)
			}
			time.Sleep(time.Second)
		}
	}()

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT) //监听指定信号
	_ = <-c
	goleans.Stop()
}
