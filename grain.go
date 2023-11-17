package actorgo

import (
	"context"
	"sync"

	"github.com/sniperHW/clustergo/addr"
)

type Grain interface {
	GetIdentity() string
	RegisterMethod(uint16, interface{}) error
	GetMethod(uint16) *methodCaller
}

type GrainMgr struct {
	sync.RWMutex
	grains map[string]Grain
}

var grainMgr GrainMgr = GrainMgr{grains: map[string]Grain{}}

func OnActivate(identity string, grain Grain) {
	grainMgr.Lock()
	defer grainMgr.Unlock()
	grainMgr.grains[identity] = grain
}

func OnDeActivate(identity string) {
	grainMgr.Lock()
	defer grainMgr.Unlock()
	delete(grainMgr.grains, identity)
}

func OnRPCRequest(ctx context.Context, from addr.LogicAddr, req *RequestMsg) {
	replyer := &Replyer{
		seq:    req.Seq,
		oneway: req.Oneway,
		from:   from,
	}

	grainMgr.RLock()
	grain, ok := grainMgr.grains[req.To]
	grainMgr.RUnlock()
	if !ok {
		replyer.Error(ErrGrainNotExist)
	} else if fn := grain.GetMethod(req.Method); fn != nil {
		fn.call(ctx, replyer, req)
	} else {
		replyer.Error(ErrMethodNotExist)
	}
}

type GrainBase struct {
	Mailbox
	Identity string
	Methods  map[uint16]*methodCaller
}

func (grain *GrainBase) GetIdentity() string {
	return grain.Identity
}

func (grain *GrainBase) GetMethod(method uint16) *methodCaller {
	return grain.Methods[method]
}

func (grain *GrainBase) InitMailbox(mailBoxOpt MailboxOption) {
	grain.Mailbox = Mailbox{
		taskQueue:  make(chan func(), mailBoxOpt.TaskQueueCap),
		awakeQueue: make(chan *goroutine, 64),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}
}

func (grain *GrainBase) RegisterMethod(method uint16, fn interface{}) error {
	if caller, err := makeMethodCaller(fn); err != nil {
		return err
	} else {
		grain.Methods[method] = caller
		return nil
	}
}
