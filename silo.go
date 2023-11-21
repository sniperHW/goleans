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
	grains            map[string]*Grain
	node              *clustergo.Node
	rpcCli            *rcpClient
	placementDriver   pd.PlacementDriver
	userObjectFactory func(string) UserObject
	startOnce         sync.Once
}

func NewSilo(placementDriver pd.PlacementDriver, node *clustergo.Node, userObjectFactory func(string) UserObject) (*Silo, error) {
	if grains, err := placementDriver.Login(); err != nil {
		return nil, err
	} else {
		s := &Silo{
			grains:            map[string]*Grain{},
			node:              node,
			placementDriver:   placementDriver,
			userObjectFactory: userObjectFactory,
			rpcCli: &rcpClient{
				timeOffset: uint32(time.Now().Unix() - time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).Unix()),
				startTime:  time.Now(),
			},
		}

		for _, v := range grains {
			s.grains[v] = newGrain(s, v)
		}
		return s, nil
	}
}

func (s *Silo) ActiveCallback(identity string) {
	s.Lock()
	defer s.Lock()
	if _, ok := s.grains[identity]; !ok {
		s.grains[identity] = newGrain(s, identity)
	}
}

func (s *Silo) OnNotifyGrainNotExist(identity string) {
	s.placementDriver.ClearPlacementCache(identity)
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
	} else {
		var err error
		object := grain.userObject.Load().(UserObject)
		if object == emptyUserObject {
			if object, err = grain.createUserObj(s); err != nil {
				replyer.Error(ErrUserGrainCreateError)
			}
		}

		if fn := grain.GetMethod(req.Method); fn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			err = grain.AddTask(ctx, func() {
				if object != grain.userObject.Load().(UserObject) {
					//Init出错
					replyer.Error(ErrUserGrainInitError)
				} else {
					fn.call(ctx, replyer, req)
				}
			})

			if err == ErrMailBoxClosed {
				replyer.Error(ErrMethodNotExist)
			}
			//AddTask超时不返回，让调用方超时
		} else {
			replyer.Error(ErrMethodNotExist)
		}
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
			remoteAddr, err := s.placementDriver.GetPlacement(ctx, identity)
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
						s.placementDriver.ClearPlacementCache(identity)
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
