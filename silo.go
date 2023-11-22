package goleans

import (
	"context"
	"goleans/pd"
	"sync"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
)

var logger Logger

func InitLogger(l Logger) {
	logger = l
}

type Silo struct {
	sync.RWMutex
	grains            map[string]*Grain
	node              *clustergo.Node
	placementDriver   pd.PlacementDriver
	userObjectFactory func(string) UserObject
	startOnce         sync.Once
}

func NewSilo(ctx context.Context, placementDriver pd.PlacementDriver, node *clustergo.Node, userObjectFactory func(string) UserObject) (*Silo, error) {
	s := &Silo{
		grains:            map[string]*Grain{},
		node:              node,
		placementDriver:   placementDriver,
		userObjectFactory: userObjectFactory,
	}
	s.Lock()
	defer s.Unlock()
	placementDriver.SetActiveCallback(s.activeCallback)
	if grains, err := placementDriver.Login(ctx); err != nil {
		return nil, err
	} else {
		for _, v := range grains {
			s.grains[v] = newGrain(s, v)
		}
		return s, nil
	}
}

func (s *Silo) activeCallback(identity string) {
	s.Lock()
	defer s.Unlock()
	grain, ok := s.grains[identity]
	if !ok || grain.deactive {
		s.grains[identity] = newGrain(s, identity)
	}
}

func (s *Silo) OnRPCRequest(ctx context.Context, from addr.LogicAddr, req *RequestMsg) {

	logger.Debugf("OnRPCRequest from:%s method:%d", from.String(), req.Method)

	replyer := &Replyer{
		seq:      req.Seq,
		oneway:   req.Oneway,
		from:     from,
		identity: req.To,
		node:     s.node,
	}

	s.RLock()
	grain, ok := s.grains[req.To]
	s.RUnlock()

	if !ok {
		replyer.Error(ErrGrainNotExist)
	} else {
		grain.lastRequest.Store(time.Now())
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		err := grain.AddTask(ctx, func() {
			if grain.userObject == nil {
				userObj := s.userObjectFactory(grain.Identity)
				if userObj != nil {
					if err := userObj.Init(grain); err != nil {
						replyer.Error(ErrUserGrainInitError)
						return
					}
				} else {
					replyer.Error(ErrUserGrainCreateError)
					return
				}
				grain.userObject = userObj
			}

			if grain.deactive {
				replyer.Error(ErrMethodNotExist)
			} else if fn := grain.GetMethod(req.Method); fn != nil {
				fn.call(ctx, replyer, req)
			} else {
				replyer.Error(ErrMethodNotExist)
			}
		})

		if err == ErrMailBoxClosed {
			replyer.Error(ErrGrainNotExist)
		}
	}
}

func (s *Silo) Stop() {

}
