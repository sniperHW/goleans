package goleans

import (
	"context"
	"errors"
	"fmt"
	"goleans/pd"
	"goleans/testproto/echo"
	"sync"
	"testing"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/discovery"
	"github.com/sniperHW/clustergo/logger/zap"
	"github.com/stretchr/testify/assert"
)

type localDiscovery struct {
	nodes      map[addr.LogicAddr]*discovery.Node
	subscribes []func(discovery.DiscoveryInfo)
}

// 订阅变更
func (d *localDiscovery) Subscribe(updateCB func(discovery.DiscoveryInfo)) error {
	d.subscribes = append(d.subscribes, updateCB)
	i := discovery.DiscoveryInfo{}
	for _, v := range d.nodes {
		i.Add = append(i.Add, *v)
	}
	updateCB(i)
	return nil
}

func (d *localDiscovery) AddNode(n *discovery.Node) {
	d.nodes[n.Addr.LogicAddr()] = n
	add := discovery.DiscoveryInfo{
		Add: []discovery.Node{*n},
	}
	for _, v := range d.subscribes {
		v(add)
	}
}

func (d *localDiscovery) RemoveNode(logicAddr addr.LogicAddr) {
	if n := d.nodes[logicAddr]; n != nil {
		delete(d.nodes, logicAddr)
		remove := discovery.DiscoveryInfo{
			Remove: []discovery.Node{*n},
		}
		for _, v := range d.subscribes {
			v(remove)
		}
	}
}

func (d *localDiscovery) ModifyNode(modify *discovery.Node) {
	if n, ok := d.nodes[modify.Addr.LogicAddr()]; ok {
		if n.Available != modify.Available || n.Addr.NetAddr() != modify.Addr.NetAddr() {
			d.nodes[modify.Addr.LogicAddr()] = modify
			//nodes := d.LoadNodeInfo()
			update := discovery.DiscoveryInfo{
				Update: []discovery.Node{*modify},
			}

			for _, v := range d.subscribes {
				v(update)
			}
		}
	}
}

func (d *localDiscovery) Close() {

}

type pdSilo struct {
	logicAddr      addr.LogicAddr
	grains         map[string]pd.Grain
	activeCallback func(pd.Grain) bool
}

type placementDriver struct {
	sync.Mutex
	placement map[string]*pdSilo
	silos     []*pdSilo
	nextSilo  int
	version   uint64
}

func (p *placementDriver) Login(pdc *placementDriverClient) (grains []pd.Grain) {
	p.Lock()
	defer p.Unlock()
	var s *pdSilo
	for _, v := range pdc.driver.silos {
		if v.logicAddr == pdc.selfAddr {
			s = v
		}
	}

	if s == nil {
		s = &pdSilo{
			logicAddr:      pdc.selfAddr,
			grains:         map[string]pd.Grain{},
			activeCallback: pdc.activeCallback,
		}
		p.silos = append(p.silos, s)
	}

	for _, v := range s.grains {
		grains = append(grains, v)
	}
	return grains
}

func (p *placementDriver) GetPlacement(pdc *placementDriverClient, identity string) (addr.LogicAddr, error) {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[identity]
	if ok {
		return silo.logicAddr, nil
	}

	if len(p.silos) == 0 {
		return addr.LogicAddr(0), errors.New("no silo")
	}

	i := p.nextSilo
	for {
		silo := p.silos[i]
		if silo.logicAddr != pdc.selfAddr {
			p.nextSilo = (i + 1) % len(p.silos)
			p.version++
			grain := pd.Grain{
				Identity: identity,
				Version:  p.version,
			}

			if silo.activeCallback(grain) {
				silo.grains[identity] = grain
				return silo.logicAddr, nil
			}
		} else {
			i = (i + 1) % len(p.silos)
			if i == p.nextSilo {
				return addr.LogicAddr(0), errors.New("no silo")
			}
		}
	}
}

func (pd *placementDriver) Deactvie(siloAddr addr.LogicAddr, grain pd.Grain) {
	fmt.Println("Deactvie")
	pd.Lock()
	defer pd.Unlock()
	silo, ok := pd.placement[grain.Identity]
	if ok && silo.logicAddr == siloAddr {
		if g, ok := silo.grains[grain.Identity]; ok && g.Identity == grain.Identity {
			delete(silo.grains, grain.Identity)
			delete(pd.placement, grain.Identity)
		}
	}
}

type placementDriverClient struct {
	sync.Mutex
	driver         *placementDriver
	localCache     map[string]addr.LogicAddr
	selfAddr       addr.LogicAddr
	activeCallback func(pd.Grain) bool
}

func (pdc *placementDriverClient) Login(ctx context.Context) (grains []pd.Grain, err error) {
	grains = pdc.driver.Login(pdc)
	pdc.Lock()
	defer pdc.Unlock()
	for _, v := range grains {
		pdc.localCache[v.Identity] = pdc.selfAddr
	}
	return grains, err
}

func (pdc *placementDriverClient) GetPlacement(ctx context.Context, identity string) (addr.LogicAddr, error) {
	pdc.Lock()
	defer pdc.Unlock()
	logicAddr, ok := pdc.localCache[identity]
	if ok {
		return logicAddr, nil
	} else {
		logicAddr, err := pdc.driver.GetPlacement(pdc, identity)
		if err == nil {
			pdc.localCache[identity] = logicAddr
		}
		return logicAddr, err
	}
}

func (pdc *placementDriverClient) SetActiveCallback(cb func(pd.Grain) bool) {
	pdc.activeCallback = func(grain pd.Grain) bool {
		if !cb(grain) {
			return false
		}
		pdc.Lock()
		pdc.localCache[grain.Identity] = pdc.selfAddr
		pdc.Unlock()
		return true
	}
}

func (pdc *placementDriverClient) Deactvie(ctx context.Context, grain pd.Grain) error {
	pdc.driver.Deactvie(pdc.selfAddr, grain)
	return nil
}

func (pdc *placementDriverClient) MarkUnAvaliable() {

}

func (pdc *placementDriverClient) ClearPlacementCache(identity string) {
	pdc.Lock()
	defer pdc.Unlock()
	delete(pdc.localCache, identity)
}

type User struct {
	grain *Grain
}

func (u *User) Echo(ctx context.Context, r *Replyer, arg *echo.Request) {
	r.Reply(&echo.Response{
		Msg: fmt.Sprintf("%s -> %s", arg.Msg, u.grain.GetIdentity()),
	})
}

func (u *User) Init(grain *Grain) error {
	//从数据库加载数据，初始化User
	u.grain = grain
	grain.RegisterMethod(1, u.Echo)
	return nil
}

func (u *User) Deactive() error {
	//将User保存到数据库
	return nil
}

func factory(identity string) UserObject {
	return &User{}
}

func init() {
	l := zap.NewZapLogger("grain_test.log", "./logfile", "debug", 1024*1024*100, 14, 28, true)
	clustergo.InitLogger(l.Sugar())
	InitLogger(l.Sugar())
}

func createSilo(node *clustergo.Node, pdc *placementDriverClient) *Silo {
	silo, _ := newSilo(context.Background(), pdc, node, factory)
	//pdc.SetActiveCallback(silo.activeCallback)
	node.RegisterBinrayHandler(Actor_request, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		req := RequestMsg{}
		if err := req.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			silo.OnRPCRequest(context.TODO(), from, &req)
		}
	}).RegisterBinrayHandler(Actor_notify_not_exist, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		pdc.ClearPlacementCache(string(msg))
	})
	return silo
}

func TestGoleans(t *testing.T) {
	localDiscovery := &localDiscovery{
		nodes: map[addr.LogicAddr]*discovery.Node{},
	}

	node1Addr, _ := addr.MakeAddr("1.1.1", "localhost:28110")
	node2Addr, _ := addr.MakeAddr("1.2.1", "localhost:28111")

	localDiscovery.AddNode(&discovery.Node{
		Addr:      node1Addr,
		Available: true,
	})

	localDiscovery.AddNode(&discovery.Node{
		Addr:      node2Addr,
		Available: true,
	})

	pdServer := &placementDriver{
		placement: map[string]*pdSilo{},
	}

	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]addr.LogicAddr{},
		selfAddr:   node1Addr.LogicAddr(),
	}

	err := Start(localDiscovery, node1Addr.LogicAddr(), pdClient1, factory)
	if err != nil {
		panic(err)
	}

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node2.Start(localDiscovery, node2Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]addr.LogicAddr{},
		selfAddr:   node2Addr.LogicAddr(),
	}

	rpcClient := NewRPCClient(node2, pdClient2)

	node2.RegisterBinrayHandler(Actor_response, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			rpcClient.OnRPCResponse(context.TODO(), &resp)
		}
	}).RegisterBinrayHandler(Actor_notify_not_exist, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		pdClient2.ClearPlacementCache(string(msg))
	})

	var resp echo.Response
	err = rpcClient.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, &resp)

	fmt.Println(err, &resp)
	node2.Stop()

	err = Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello2",
	}, &resp)

	fmt.Println(err, &resp)

	Stop()
}

func TestGrain(t *testing.T) {

	localDiscovery := &localDiscovery{
		nodes: map[addr.LogicAddr]*discovery.Node{},
	}

	node1Addr, _ := addr.MakeAddr("1.1.1", "localhost:28110")
	node2Addr, _ := addr.MakeAddr("1.2.1", "localhost:28111")

	localDiscovery.AddNode(&discovery.Node{
		Addr:      node1Addr,
		Available: true,
	})

	localDiscovery.AddNode(&discovery.Node{
		Addr:      node2Addr,
		Available: true,
	})

	pdServer := &placementDriver{
		placement: map[string]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]addr.LogicAddr{},
		selfAddr:   node1Addr.LogicAddr(),
	}
	silo1 := createSilo(node1, pdClient1)

	err := node1.Start(localDiscovery, node1Addr.LogicAddr())
	assert.Nil(t, err)

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node2.Start(localDiscovery, node2Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]addr.LogicAddr{},
		selfAddr:   node2Addr.LogicAddr(),
	}

	rpcClient := NewRPCClient(node2, pdClient2)

	node2.RegisterBinrayHandler(Actor_response, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			rpcClient.OnRPCResponse(context.TODO(), &resp)
		}
	}).RegisterBinrayHandler(Actor_notify_not_exist, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		pdClient2.ClearPlacementCache(string(msg))
	})

	var resp echo.Response
	err = rpcClient.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, &resp)

	fmt.Println(err, &resp)

	silo1.Stop()
	node1.Stop()
	node2.Stop()
}
