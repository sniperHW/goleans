package client

import (
	"context"
	"sync"

	"github.com/sniperHW/clustergo/addr"
)

type placement struct {
	sync.Mutex
	address *addr.LogicAddr
	waits   []chan struct{}
}

type ExampleClient struct {
	sync.Mutex
	activeCallback func(string)
	placementInfo  map[string]*placement
}

func (ec *ExampleClient) GetHostService(ctx context.Context, identity string) (addr.LogicAddr, error) {

	return addr.LogicAddr(0), nil
}

func (ec *ExampleClient) Deactive(ctx context.Context, identity string, localAddr addr.LogicAddr) error {
	return nil
}

func (ec *ExampleClient) SetActiveCallback(fn func(string)) {
	ec.activeCallback = fn
}

func (ec *ExampleClient) ClearPlacementInfo(identity string) {
	ec.Lock()
	defer ec.Unlock()
	if p, ok := ec.placementInfo[identity]; ok {
		p.Lock()
		if p.address != nil {
			delete(ec.placementInfo, identity)
		}
		p.Unlock()
	}
}
