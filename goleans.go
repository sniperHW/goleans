package goleans

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/goleans/pd"
	"github.com/sniperHW/goleans/rpc"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/membership"
	"google.golang.org/protobuf/proto"
)

var (
	silo      *Silo
	rpcClient *rpc.Client
	startOnce sync.Once
	started   atomic.Bool
)

// 不作为Silo启动
func Start(memberShip membership.Subscribe, localAddr addr.LogicAddr, placementDriver pd.PlacementDriver) error {
	ok := false
	startOnce.Do(func() {
		ok = true
	})
	if ok {
		err := clustergo.Start(memberShip, localAddr)
		if err != nil {
			return err
		}
		node := clustergo.GetDefaultNode()
		rpcClient = rpc.NewClient(node, placementDriver)
		node.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
			resp := rpc.ResponseMsg{}
			if err := resp.Decode(msg); err != nil {
				logger.Error(err)
			} else {
				rpcClient.OnRPCResponse(&resp)
			}
		})

		started.Store(true)
	}
	return nil
}

// 作为Silo启动
func StartSilo(memberShip membership.Subscribe, localAddr addr.LogicAddr, placementDriver pd.PlacementDriver, grainList []GrainCfg, grainFactory func(string) Grain) error {
	ok := false
	startOnce.Do(func() {
		ok = true
	})
	if ok {
		err := clustergo.Start(memberShip, localAddr)
		if err != nil {
			return err
		}

		node := clustergo.GetDefaultNode()

		if grainFactory != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			s, err := newSilo(ctx, placementDriver, node, grainList, grainFactory)
			if err != nil {
				clustergo.Stop()
				return err
			}
			silo = s

			node.RegisterBinaryHandler(rpc.Actor_request, func(ctx context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
				req := rpc.RequestMsg{}
				if err := req.Decode(msg); err != nil {
					logger.Error(err)
				} else {
					silo.OnRPCRequest(ctx, from, &req)
				}
			}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
				if len(msg) > 4 {
					newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
					placementDriver.ResetPlacementCache(string(msg[4:]), newAddr)
				}
			})
		}

		rpcClient = rpc.NewClient(node, placementDriver)
		node.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
			resp := rpc.ResponseMsg{}
			if err := resp.Decode(msg); err != nil {
				logger.Error(err)
			} else {
				rpcClient.OnRPCResponse(&resp)
			}
		})

		started.Store(true)
	}
	return nil
}

func Call(ctx context.Context, pid string, method uint16, arg proto.Message, ret proto.Message) error {
	if !started.Load() {
		return errors.New("not started")
	}
	return rpcClient.Call(ctx, pid, method, arg, ret)
}

func CallWithTimeout(pid string, method uint16, arg proto.Message, ret proto.Message, d time.Duration) error {
	if !started.Load() {
		return errors.New("not started")
	}
	return rpcClient.CallWithTimeout(pid, method, arg, ret, d)
}

func Stop() {
	if started.Load() {
		if silo != nil {
			silo.Stop()
		}
		clustergo.Stop()
	}
}
