package goleans

import (
	"context"
	"goleans/pd"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
)

var logger Logger

func InitLogger(l Logger) {
	logger = l
}

func GetLogger() Logger {
	return logger
}

type GrainCfg struct {
	Type       string
	MailboxCap int
}

var (
	UserGrainFactory func(string) UserObject
)

type Silo struct {
	sync.RWMutex
	grains            map[pd.GrainIdentity]*Grain
	grainList         map[string]GrainCfg
	node              *clustergo.Node
	placementDriver   pd.PlacementDriver
	userObjectFactory func(string) UserObject
	stoped            atomic.Bool
}

func newSilo(ctx context.Context, placementDriver pd.PlacementDriver, node *clustergo.Node, grainList []GrainCfg, userObjectFactory func(string) UserObject) (*Silo, error) {
	s := &Silo{
		grains:            map[pd.GrainIdentity]*Grain{},
		node:              node,
		placementDriver:   placementDriver,
		userObjectFactory: userObjectFactory,
		grainList:         map[string]GrainCfg{},
	}

	placementDriver.SetGetMetric(s.getMetric)

	gl := []string{}

	for _, v := range grainList {
		s.grainList[v.Type] = v
		gl = append(gl, v.Type)
	}

	if err := placementDriver.Login(ctx, gl); err != nil {
		return nil, err
	} else {
		return s, nil
	}
}

func (s *Silo) getMetric() pd.Metric {
	s.Lock()
	defer s.Unlock()
	return pd.Metric{
		GrainCount: len(s.grains),
	}
}

func (s *Silo) removeGrain(grain *Grain) {
	s.Lock()
	defer s.Unlock()
	delete(s.grains, grain.Identity)
}

func (s *Silo) Stop() {
	if s.stoped.CompareAndSwap(false, true) {
		s.placementDriver.MarkUnAvaliable()
		//Grain Deactive
		var wait sync.WaitGroup
		s.Lock()
		for _, v := range s.grains {
			wait.Add(1)
			if v.mailbox.PushTask(context.TODO(), func() {
				v.onSiloStop(wait.Done)
			}) != nil {
				wait.Done()
			}
		}
		s.Unlock()
		wait.Wait()
		s.placementDriver.Logout(context.TODO())
	}
}

func (s *Silo) OnRPCRequest(ctx context.Context, from addr.LogicAddr, req *RequestMsg) {

	logger.Debugf("OnRPCRequest from:%s method:%d seq:%d", from.String(), req.Method, req.Seq)

	replyer := &Replyer{
		seq:      req.Seq,
		oneway:   req.Oneway,
		from:     from,
		identity: req.To,
		node:     s.node,
	}

	if s.stoped.Load() {
		replyer.Redirect(0)
		return
	}

	identity := pd.GrainIdentity(req.To)

	t := strings.Split(string(identity), "@")

	if len(t) < 2 {
		replyer.Error(ErrCodeInvaildIdentity)
		return
	}

	grainType := t[1]

	s.Lock()
	grain, ok := s.grains[identity]
	if !ok {
		grain = newGrain(s, identity, grainType)
		s.grains[identity] = grain
	}
	s.Unlock()
	grain.lastRequest.Store(time.Now())
	err := grain.AddTaskNoWait(func() {
		if grain.stoped {
			//silo正在停止
			replyer.Redirect(0)
			return
		}

		if grain.state == grain_un_activate {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
			err := s.placementDriver.Activate(ctx, identity)
			cancel()
			switch err := err.(type) {
			case pd.ErrorRedirect:
				logger.Errorf("Activate Grain:%s redirect to:%s", grain.Identity, err.Addr.String())
				replyer.Redirect(err.Addr)
				s.removeGrain(grain)
				grain.mailbox.Close(false)
				return
			case error:
				logger.Errorf("Activate Grain:%s error:%v", grain.Identity, err)
				replyer.Error(ErrCodeRetryAgain)
				s.removeGrain(grain)
				grain.mailbox.Close(false)
				return
			default:
				grain.state = grain_activated
			}
		}

		if grain.state == grain_activated {
			if grain.userObject == nil {
				if userObj := s.userObjectFactory(grainType); nil == userObj {
					logger.Errorf("Create Grain:%s Failed", grain.Identity)
					grain.deactive(nil)
					replyer.Redirect(addr.LogicAddr(0))
					return
				} else {
					grain.userObject = userObj
				}
			}

			if err := grain.userObject.Init(grain); err != nil {
				logger.Errorf("Create Grain:%s Init error:%e", grain.Identity, err)
				if err == ErrInitUnRetryAbleError {
					//通告调用方，调用不应再尝试
					replyer.Error(ErrCodeUserGrainInitError)
				} else {
					replyer.Error(ErrCodeRetryAgain)
				}
				return
			} else {
				grain.state = grain_running
			}
		}

		if grain.state == grain_running {
			if fn := grain.methods[req.Method]; fn != nil {
				fn.call(ctx, replyer, req)
			} else {
				replyer.Error(ErrCodeMethodNotExist)
			}
		} else {
			replyer.Redirect(addr.LogicAddr(0))
		}
	})

	if err == ErrMailBoxClosed {
		replyer.Redirect(addr.LogicAddr(0))
	} else if err == ErrMailBoxFull {
		replyer.Error(ErrCodeRetryAgain)
	}
}
