package goleans

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/goleans/pd"
	"github.com/sniperHW/goleans/rpc"
)

type GrainCfg struct {
	Type          string
	QueueCap      int
	AwaitQueueCap int
}

var (
	GrainFactory func(string) Grain
)

type Silo struct {
	sync.RWMutex
	grains          map[string]*GrainContext
	grainList       map[string]GrainCfg
	node            *clustergo.Node
	placementDriver pd.PlacementDriver
	grainFactory    func(string) Grain
	stoped          atomic.Bool
}

func newSilo(ctx context.Context, placementDriver pd.PlacementDriver, node *clustergo.Node, grainList []GrainCfg, grainFactory func(string) Grain) (*Silo, error) {
	s := &Silo{
		grains:          map[string]*GrainContext{},
		node:            node,
		placementDriver: placementDriver,
		grainFactory:    grainFactory,
		grainList:       map[string]GrainCfg{},
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

func (s *Silo) removeGrain(grainCtx *GrainContext) {
	s.Lock()
	defer s.Unlock()
	delete(s.grains, grainCtx.pid)
}

func (s *Silo) Stop() {
	if s.stoped.CompareAndSwap(false, true) {
		s.placementDriver.MarkUnAvaliable()
		//Grain Deactive
		var wait sync.WaitGroup
		s.Lock()
		for _, v := range s.grains {
			wait.Add(1)
			if v.mailbox.Input(func() {
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

func (s *Silo) OnRPCRequest(ctx context.Context, from addr.LogicAddr, req *rpc.RequestMsg) {

	logger.Debugf("OnRPCRequest from:%s method:%d seq:%d", from.String(), req.Method, req.Seq)

	replyer := &Replyer{
		from: from,
		node: s.node,
		req:  req,
	}

	if s.stoped.Load() {
		replyer.redirect(0)
		return
	}

	identity := req.To

	t := strings.Split(string(identity), "@")

	if len(t) < 2 {
		replyer.error(rpc.ErrCodeInvaildPid)
		return
	}

	grainType := t[1]

	s.Lock()
	grainCtx, ok := s.grains[identity]
	if !ok {
		grainCtx = newGrainContext(s, identity, grainType)
		s.grains[identity] = grainCtx
	}
	s.Unlock()
	grainCtx.lastRequest.Store(time.Now())
	err := grainCtx.mailbox.InputNoWait(func() {
		if grainCtx.stoped {
			//silo正在停止
			replyer.redirect(0)
			return
		}

		if grainCtx.state == grain_un_place {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
			err := s.placementDriver.Place(ctx, identity)
			cancel()
			switch err := err.(type) {
			case pd.ErrorRedirect:
				logger.Errorf("Place Grain:%s redirect to:%s", grainCtx.Pid(), err.Addr.String())
				replyer.redirect(err.Addr)
				s.removeGrain(grainCtx)
				grainCtx.mailbox.Close(false)
				return
			case error:
				logger.Errorf("Place Grain:%s error:%v", grainCtx.Pid(), err)
				replyer.error(rpc.ErrCodeRetryAgain)
				s.removeGrain(grainCtx)
				grainCtx.mailbox.Close(false)
				return
			default:
				grainCtx.state = grain_place_ok
			}
		}

		if grainCtx.state == grain_place_ok {
			if grainCtx.grain == nil {
				if grain := s.grainFactory(grainType); nil == grain {
					logger.Errorf("Create Grain:%s Failed", grainCtx.Pid())
					grainCtx.deactive()
					replyer.redirect(addr.LogicAddr(0))
					return
				} else {
					grainCtx.grain = grain
				}
			}
			err, retry := grainCtx.grain.Activate(grainCtx)
			if err != nil {
				logger.Errorf("Create Grain:%s Init error:%e", grainCtx.Pid(), err)
				if retry {
					replyer.error(rpc.ErrCodeRetryAgain)
				} else {
					grainCtx.deactive()
					//通告调用方，调用不应再尝试
					replyer.error(rpc.ErrCodeActivateFailed)
				}
				return
			} else {
				grainCtx.state = grain_activated
			}
		}

		if grainCtx.state == grain_activated {
			grainCtx.serveCall(ctx, replyer, req)
		} else {
			replyer.redirect(addr.LogicAddr(0))
		}
	})

	if err == rpc.ErrMailBoxClosed {
		replyer.redirect(addr.LogicAddr(0))
	} else if err == rpc.ErrMailBoxFull {
		replyer.error(rpc.ErrCodeRetryAgain)
	}
}
