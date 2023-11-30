package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"goleans"
	"goleans/example/codec"
	"goleans/example/placement"
	"goleans/pd"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"goleans/example/grain/rpc/service/login"
	"goleans/example/grain/rpc/service/logout"
	"goleans/example/grain/rpc/service/relaymsg"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/example/discovery"
	"github.com/sniperHW/clustergo/logger/zap"
	"github.com/sniperHW/netgo"
)

type gateUser struct {
	id       uint64
	identity pd.GrainIdentity
	conn     netgo.Socket
}

var (
	mtx     sync.Mutex
	nextID  uint64
	userMap map[uint64]*gateUser = map[uint64]*gateUser{}
)

func main() {
	pdAddr := flag.String("pdAddr", "127.0.0.1:8110", "address of pd")
	discoveryAddr := flag.String("discoveryAddr", "127.0.0.1:8110", "address of discovery")
	flag.Parse()

	l := zap.NewZapLogger("gateway.log", "./logfile", "debug", 1024*1024*100, 14, 28, true)
	placement.InitLogger(l)
	clustergo.InitLogger(l.Sugar())
	goleans.InitLogger(l.Sugar())
	localaddr, _ := addr.MakeLogicAddr("1.2.1")
	discoveryCli := discovery.NewClient(*discoveryAddr)
	pdClient := placement.NewCli(localaddr, *pdAddr)

	clustergo.RegisterBinaryHandler(100, func(_ addr.LogicAddr, _ uint16, msg []byte) {
		//id在包尾巴
		id := binary.BigEndian.Uint64(msg[len(msg)-8:])
		mtx.Lock()
		u := userMap[id]
		mtx.Unlock()
		if u != nil {
			u.conn.Send(msg[:len(msg)-8], time.Now().Add(time.Millisecond*10))
		}
	})

	goleans.Start(discoveryCli, localaddr, pdClient) //不作为silo

	_, serve, _ := netgo.ListenTCP("tcp", "127.0.0.1:18113", func(conn *net.TCPConn) {
		go func() {
			//读取登录包，登录包为16个字符的账号
			account := make([]byte, 16)
			n, err := conn.Read(account)
			if n != 16 || err != nil {
				conn.Close()
				return
			}

			u := &gateUser{
				id:       atomic.AddUint64(&nextID, 1),
				identity: pd.GrainIdentity(fmt.Sprintf("%s@User", string(account))),
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			resp, err := login.Call(ctx, u.identity, &login.LoginReq{
				GateUserID: int64(u.id),
				GateAddr:   int32(clustergo.GetDefaultNode().Addr().LogicAddr()),
			})
			cancel()

			if err != nil {
				l.Sugar().Error(err)
				conn.Close()
				return
			}

			if !resp.Ok {
				conn.Close()
				return
			}

			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				logout.Call(ctx, u.identity, &logout.LogoutReq{
					GateUserID: int64(u.id),
					GateAddr:   int32(clustergo.GetDefaultNode().Addr().LogicAddr()),
				})
			}()

			codecc := codec.NewCodec()
			session := netgo.NewTcpSocket(conn, codecc)
			u.conn = session

			mtx.Lock()
			userMap[u.id] = u
			mtx.Unlock()

			for {
				msg, err := session.Recv(time.Now().Add(time.Second * 5))
				if err != nil {
					l.Sugar().Error(err)
					session.Close()
					return
				}

				//将消息转发给user处理
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				resp, err := relaymsg.Call(ctx, u.identity, &relaymsg.RelaymsgReq{
					GateUserID: int64(u.id),
					GateAddr:   int32(clustergo.GetDefaultNode().Addr().LogicAddr()),
					Msg:        msg})
				cancel()

				if err != nil || !resp.Ok {
					if err != nil {
						l.Sugar().Error(err)
					}
					mtx.Lock()
					delete(userMap, u.id)
					mtx.Unlock()
					session.Close()
					return
				}
			}
		}()
	})

	go serve()

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT) //监听指定信号
	_ = <-c
	goleans.Stop()
}
