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
	"github.com/sniperHW/clustergo/discovery"
	"google.golang.org/protobuf/proto"
)

var (
	silo      *Silo
	rpcClient *RPCClient
	startOnce sync.Once
	started   atomic.Bool
)

func Start(discovery discovery.Discovery, localAddr addr.LogicAddr, placementDriver pd.PlacementDriver, userObjectFactory func(pd.GrainIdentity) UserObject) error {
	ok := false
	startOnce.Do(func() {
		ok = true
	})
	if ok {
		err := clustergo.Start(discovery, localAddr)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		node := clustergo.GetDefaultNode()
		s, err := newSilo(ctx, placementDriver, node, userObjectFactory)
		if err != nil {
			clustergo.Stop()
			return err
		}
		silo = s
		rpcClient = NewRPCClient(node, placementDriver)

		node.RegisterBinrayHandler(Actor_request, func(from addr.LogicAddr, cmd uint16, msg []byte) {
			req := RequestMsg{}
			if err := req.Decode(msg); err != nil {
				logger.Error(err)
			} else {
				silo.OnRPCRequest(context.TODO(), from, &req)
			}
		}).RegisterBinrayHandler(Actor_response, func(from addr.LogicAddr, cmd uint16, msg []byte) {
			resp := ResponseMsg{}
			if err := resp.Decode(msg); err != nil {
				logger.Error(err)
			} else {
				rpcClient.OnRPCResponse(context.TODO(), &resp)
			}
		}).RegisterBinrayHandler(Actor_notify_redirect, func(from addr.LogicAddr, cmd uint16, msg []byte) {
			if len(msg) > 4 {
				newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
				placementDriver.ResetPlacementCache(pd.GrainIdentity(msg[4:]), newAddr)
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
		silo.Stop()
		clustergo.Stop()
	}
}
