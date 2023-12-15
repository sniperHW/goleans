package goleans

import (
	"context"
	"encoding/binary"
	"errors"
	"goleans/pd"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/membership"
	"google.golang.org/protobuf/proto"
)

var (
	silo      *Silo
	rpcClient *RPCClient
	startOnce sync.Once
	started   atomic.Bool
)

// 不作为Silo启动
func Start(memberShip membership.Client, localAddr addr.LogicAddr, placementDriver pd.PlacementDriver) error {
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
		rpcClient = NewRPCClient(node, placementDriver)
		node.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
			resp := ResponseMsg{}
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
func StartSilo(memberShip membership.Client, localAddr addr.LogicAddr, placementDriver pd.PlacementDriver, grainList []GrainCfg, siloObjectFactory func(string) UserObject) error {
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

		if siloObjectFactory != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			s, err := newSilo(ctx, placementDriver, node, grainList, siloObjectFactory)
			if err != nil {
				clustergo.Stop()
				return err
			}
			silo = s

			node.RegisterBinaryHandler(Actor_request, func(ctx context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
				req := RequestMsg{}
				if err := req.Decode(msg); err != nil {
					logger.Error(err)
				} else {
					silo.OnRPCRequest(ctx, from, &req)
				}
			}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
				if len(msg) > 4 {
					newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
					placementDriver.ResetPlacementCache(pd.GrainIdentity(msg[4:]), newAddr)
				}
			})
		}

		rpcClient = NewRPCClient(node, placementDriver)
		node.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
			resp := ResponseMsg{}
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

func Call(ctx context.Context, identity pd.GrainIdentity, method uint16, arg proto.Message, ret proto.Message) error {
	if !started.Load() {
		return errors.New("not started")
	}
	return rpcClient.Call(ctx, identity, method, arg, ret)
}

func Stop() {
	if started.Load() {
		if silo != nil {
			silo.Stop()
		}
		clustergo.Stop()
	}
}
