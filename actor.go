package actorgo

import (
	"actorgo/pd"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"google.golang.org/protobuf/proto"
)

var logger Logger

func InitLogger(l Logger) {
	logger = l
}

type ActorGo struct {
	sync.RWMutex
	grains          map[string]Grain
	node            *clustergo.Node
	rpcCli          *rcpClient
	placementDriver pd.PlacementDriver
}

func NewActorGo(placementDriver pd.PlacementDriver, node *clustergo.Node) *ActorGo {
	return &ActorGo{
		grains:          map[string]Grain{},
		node:            node,
		placementDriver: placementDriver,
		rpcCli: &rcpClient{
			timeOffset: uint32(time.Now().Unix() - time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).Unix()),
			startTime:  time.Now(),
		},
	}
}

func (a *ActorGo) OnActivate(identity string, grain Grain) {
	a.Lock()
	defer a.Unlock()
	a.grains[identity] = grain
}

func (a *ActorGo) OnDeActivate(identity string) {
	a.Lock()
	defer a.Unlock()
	delete(a.grains, identity)
}

func (a *ActorGo) OnNotifyGrainNotExist(identity string) {
	a.placementDriver.ClearPlacementInfo(identity)
}

func (a *ActorGo) OnRPCRequest(ctx context.Context, from addr.LogicAddr, req *RequestMsg) {
	replyer := &Replyer{
		seq:      req.Seq,
		oneway:   req.Oneway,
		from:     from,
		identity: req.To,
	}

	a.RLock()
	grain, ok := a.grains[req.To]
	a.RUnlock()
	if !ok {
		replyer.Error(ErrGrainNotExist)
	} else if fn := grain.GetMethod(req.Method); fn != nil {
		fn.call(ctx, replyer, req)
	} else {
		replyer.Error(ErrMethodNotExist)
	}
}

func (a *ActorGo) OnRPCResponse(ctx context.Context, resp *ResponseMsg) {
	if ctx, ok := a.rpcCli.pendingCall[int(resp.Seq)%len(a.rpcCli.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).callOnResponse(resp.Ret, resp.ErrCode)
	} else {
		clustergo.Log().Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (a *ActorGo) Call(ctx context.Context, identity string, method uint16, arg proto.Message, ret proto.Message) error {
	if b, err := proto.Marshal(arg); err != nil {
		return err
	} else {
		reqMessage := &RequestMsg{
			To:     identity,
			Seq:    a.rpcCli.makeSequence(),
			Method: method,
			Arg:    b,
		}

		for {
			remoteAddr, err := a.placementDriver.GetHostService(ctx, identity)
			if err != nil {
				return err
			}

			if ret != nil {
				waitC := make(chan error, 1)
				pending := &a.rpcCli.pendingCall[int(reqMessage.Seq)%len(a.rpcCli.pendingCall)]

				pending.Store(reqMessage.Seq, &callContext{
					respReceiver: ret,
					onResponse: func(_ interface{}, err error) {
						waitC <- err
					},
				})

				clustergo.SendBinMessage(remoteAddr, reqMessage.Encode(), Actor_request)

				select {
				case err := <-waitC:
					if err == nil || err != getDescByErrCode(ErrGrainNotExist) {
						return err
					} else {
						//err == ErrGrainNotExist
						a.placementDriver.ClearPlacementInfo(identity)
						//continue
					}
				case <-ctx.Done():
					pending.Delete(reqMessage.Seq)
					switch ctx.Err() {
					case context.Canceled:
						return errors.New("canceled")
					case context.DeadlineExceeded:
						return errors.New("timeout")
					default:
						return errors.New("unknow")
					}
				}
			} else {
				reqMessage.Oneway = true
				return clustergo.SendBinMessage(remoteAddr, reqMessage.Encode(), Actor_request)
			}
		}
	}
}
