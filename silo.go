package goleans

import (
	"context"
	"errors"
	"goleans/pd"
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

type Silo struct {
	sync.RWMutex
	grains          map[string]Grain
	node            *clustergo.Node
	rpcCli          *rcpClient
	placementDriver pd.PlacementDriver
}

func NewSilo(placementDriver pd.PlacementDriver, node *clustergo.Node) *Silo {
	return &Silo{
		grains:          map[string]Grain{},
		node:            node,
		placementDriver: placementDriver,
		rpcCli: &rcpClient{
			timeOffset: uint32(time.Now().Unix() - time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).Unix()),
			startTime:  time.Now(),
		},
	}
}

func (s *Silo) OnActivate(identity string, grain Grain) {
	s.Lock()
	defer s.Unlock()
	s.grains[identity] = grain
}

func (s *Silo) OnDeActivate(identity string) {
	s.Lock()
	defer s.Unlock()
	delete(s.grains, identity)
}

func (s *Silo) OnNotifyGrainNotExist(identity string) {
	s.placementDriver.ClearPlacementInfo(identity)
}

func (s *Silo) OnRPCRequest(ctx context.Context, from addr.LogicAddr, req *RequestMsg) {
	replyer := &Replyer{
		seq:      req.Seq,
		oneway:   req.Oneway,
		from:     from,
		identity: req.To,
	}

	s.RLock()
	grain, ok := s.grains[req.To]
	s.RUnlock()
	if !ok {
		replyer.Error(ErrGrainNotExist)
	} else if fn := grain.GetMethod(req.Method); fn != nil {
		fn.call(ctx, replyer, req)
	} else {
		replyer.Error(ErrMethodNotExist)
	}
}

func (s *Silo) OnRPCResponse(ctx context.Context, resp *ResponseMsg) {
	if ctx, ok := s.rpcCli.pendingCall[int(resp.Seq)%len(s.rpcCli.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).callOnResponse(resp.Ret, resp.ErrCode)
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (s *Silo) Call(ctx context.Context, identity string, method uint16, arg proto.Message, ret proto.Message) error {
	if b, err := proto.Marshal(arg); err != nil {
		return err
	} else {
		reqMessage := &RequestMsg{
			To:     identity,
			Seq:    s.rpcCli.makeSequence(),
			Method: method,
			Arg:    b,
		}

		for {
			remoteAddr, err := s.placementDriver.GetHostService(ctx, identity)
			if err != nil {
				return err
			}

			if ret != nil {
				waitC := make(chan error, 1)
				pending := &s.rpcCli.pendingCall[int(reqMessage.Seq)%len(s.rpcCli.pendingCall)]

				pending.Store(reqMessage.Seq, &callContext{
					respReceiver: ret,
					onResponse: func(_ interface{}, err error) {
						waitC <- err
					},
				})

				s.node.SendBinMessage(remoteAddr, reqMessage.Encode(), Actor_request)

				select {
				case err := <-waitC:
					if err == nil || err != getDescByErrCode(ErrGrainNotExist) {
						return err
					} else {
						//err == ErrGrainNotExist
						s.placementDriver.ClearPlacementInfo(identity)
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
				return s.node.SendBinMessage(remoteAddr, reqMessage.Encode(), Actor_request)
			}
		}
	}
}
