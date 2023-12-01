package main

import (
	"flag"
	"goleans"
	"goleans/example/grain"
	"goleans/example/placement"
	"os"
	"os/signal"
	"syscall"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/example/discovery"
	"github.com/sniperHW/clustergo/logger/zap"
)

func main() {

	pdAddr := flag.String("pdAddr", "127.0.0.1:8110", "address of pd")
	discoveryAddr := flag.String("discoveryAddr", "127.0.0.1:8110", "address of discovery")
	flag.Parse()

	l := zap.NewZapLogger("1.1.1.log", "./logfile", "debug", 1024*1024*100, 14, 28, true)
	placement.InitLogger(l)
	clustergo.InitLogger(l.Sugar())
	goleans.InitLogger(l.Sugar())
	localaddr, _ := addr.MakeLogicAddr("1.1.1")
	discoveryCli := discovery.NewClient(*discoveryAddr)
	pdClient := placement.NewCli(localaddr, *pdAddr)
	goleans.StartSilo(discoveryCli, localaddr, pdClient, []goleans.GrainCfg{
		{
			Type:       "User",
			MailboxCap: 32,
		},
		{
			Type:       "Boss",
			MailboxCap: 32,
		},
	}, func(grainType string) goleans.UserObject {
		switch grainType {
		case "User":
			return &grain.User{
				Node: clustergo.GetDefaultNode(),
			}
		case "Boss":
			return &grain.Boss{
				Node: clustergo.GetDefaultNode(),
			}
		default:
			return nil
		}
	})

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT) //监听指定信号
	_ = <-c
	goleans.Stop()
}
