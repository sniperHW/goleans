package main

import (
	"context"
	"flag"
	"goleans"
	"goleans/example/placement"
	"goleans/pd"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"goleans/example/grain/rpc/service/echo"
	"goleans/example/grain/rpc/service/test"

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

			var wait sync.WaitGroup
			wait.Add(2)
			begin := time.Now()
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				resp, err := echo.Call(ctx, pd.GrainIdentity("sniperHW1@User"), &echo.EchoReq{
					Msg: "hello sniperHW1",
				})
				cancel()
				if err == nil {
					l.Sugar().Debug(resp.Msg)
				} else {
					l.Sugar().Debug(err)
				}
				wait.Done()
			}()

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				resp, err := echo.Call(ctx, pd.GrainIdentity("sniperHW1@User"), &echo.EchoReq{
					Msg: "hello sniperHW1",
				})
				cancel()
				if err == nil {
					l.Sugar().Debug(resp.Msg)
				} else {
					l.Sugar().Debug(err)
				}
				wait.Done()
			}()

			wait.Wait()
			//user.Echo阻塞grain，只能顺序执行,因此两次调用耗时>= 4s
			l.Sugar().Debugf("echo use %v", time.Now().Sub(begin))
		}
	}()

	go func() {
		for {
			var wait sync.WaitGroup
			wait.Add(2)

			begin := time.Now()
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()
				resp, err := test.Call(ctx, pd.GrainIdentity("sniperHW2@Boss"), &test.TestReq{
					Msg: "hello sniperHW2",
				})
				if err == nil {
					l.Sugar().Debugf("%v", resp.Msg)
				} else {
					l.Sugar().Debugf("%v", err)
				}
				wait.Done()
			}()

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()
				resp, err := test.Call(ctx, pd.GrainIdentity("sniperHW2@Boss"), &test.TestReq{
					Msg: "hello sniperHW2",
				})
				if err == nil {
					l.Sugar().Debugf("%v", resp.Msg)
				} else {
					l.Sugar().Debugf("%v", err)
				}
				wait.Done()
			}()

			wait.Wait()
			//boss.Test不阻塞grain,可以同时执行多个请求,因此两次调用耗时>= 2s
			l.Sugar().Debugf("test use %v", time.Now().Sub(begin))
		}
	}()

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT) //监听指定信号
	_ = <-c
	goleans.Stop()
}
