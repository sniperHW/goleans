package goleans

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"context"
	"encoding/binary"
	"fmt"
	"goleans/pd"
	"goleans/testproto/echo"
	"sync"
	"testing"
	"time"

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
	logicAddr addr.LogicAddr
	grains    map[pd.GrainIdentity]struct{}
	metrics   pd.Metric
}

type placementDriver struct {
	sync.Mutex
	placement map[pd.GrainIdentity]*pdSilo
	silos     []*pdSilo
	nextSilo  int
}

func (p *placementDriver) Login(siloAddr addr.LogicAddr, metric pd.Metric) {
	p.Lock()
	defer p.Unlock()

	var s *pdSilo
	for _, v := range p.silos {
		if v.logicAddr == siloAddr {
			s = v
		}
	}

	if s == nil {
		s = &pdSilo{
			logicAddr: siloAddr,
			grains:    map[pd.GrainIdentity]struct{}{},
		}
		p.silos = append(p.silos, s)
	}

	s.metrics = metric
}

func (p *placementDriver) GetPlacement(selfAddr addr.LogicAddr, identity pd.GrainIdentity) (addr.LogicAddr, error) {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[identity]
	if ok {
		return silo.logicAddr, nil
	}

	if len(p.silos) == 0 {
		return addr.LogicAddr(0), fmt.Errorf("no avaliable silo to activate grain:%v", identity)
	}

	i := p.nextSilo
	for {
		silo := p.silos[i]
		if silo.logicAddr != selfAddr {
			p.nextSilo = (i + 1) % len(p.silos)
			return silo.logicAddr, nil
		} else {
			i = (i + 1) % len(p.silos)
			if i == p.nextSilo {
				return addr.LogicAddr(0), fmt.Errorf("no avaliable silo to activate grain:%v", identity)
			}
		}
	}
}

func (p *placementDriver) Deactivate(siloAddr addr.LogicAddr, identity pd.GrainIdentity) {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[identity]
	if ok && silo.logicAddr == siloAddr {
		delete(silo.grains, identity)
		delete(p.placement, identity)
	}
}

func (p *placementDriver) Activate(siloAddr addr.LogicAddr, identity pd.GrainIdentity) error {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[identity]
	if !ok || silo.logicAddr == siloAddr {
		for _, v := range p.silos {
			if v.logicAddr == siloAddr {
				v.grains[identity] = struct{}{}
				return nil
			}
		}
		panic("siloAddr not found")
	}
	return pd.ErrorRedirect{Addr: silo.logicAddr}
}

type placementCache struct {
	siloAddr      addr.LogicAddr
	cacheDeadline time.Time
}

type placementDriverClient struct {
	sync.Mutex
	driver     *placementDriver
	localCache map[pd.GrainIdentity]placementCache
	selfAddr   addr.LogicAddr
	getMetric  func() pd.Metric
	cacheTime  time.Duration
}

func (pdc *placementDriverClient) SetCacheTime(d time.Duration) {
	pdc.cacheTime = d
}

func (pdc *placementDriverClient) SetGetMetric(getMetric func() pd.Metric) {
	pdc.getMetric = getMetric
}

func (pdc *placementDriverClient) Login(ctx context.Context, _ []string) (err error) {
	pdc.driver.Login(pdc.selfAddr, pdc.getMetric())
	return nil
}

func (pdc *placementDriverClient) GetPlacement(ctx context.Context, identity pd.GrainIdentity) (addr.LogicAddr, error) {
	pdc.Lock()
	defer pdc.Unlock()
	now := time.Now()
	cache, ok := pdc.localCache[identity]
	if ok && cache.cacheDeadline.After(now) {
		return cache.siloAddr, nil
	} else {
		siloAddr, err := pdc.driver.GetPlacement(pdc.selfAddr, identity)
		if err == nil {
			pdc.localCache[identity] = placementCache{
				siloAddr:      siloAddr,
				cacheDeadline: time.Now().Add(pdc.cacheTime),
			}
		}
		return siloAddr, err
	}
}

func (pdc *placementDriverClient) Activate(ctx context.Context, identity pd.GrainIdentity) error {
	return pdc.driver.Activate(pdc.selfAddr, identity)
}

func (pdc *placementDriverClient) Deactivate(ctx context.Context, identity pd.GrainIdentity) error {
	pdc.driver.Deactivate(pdc.selfAddr, identity)
	return nil
}

func (pdc *placementDriverClient) MarkUnAvaliable() {

}

func (pdc *placementDriverClient) ResetPlacementCache(identity pd.GrainIdentity, newAddr addr.LogicAddr) {
	pdc.Lock()
	defer pdc.Unlock()
	if newAddr.Empty() {
		delete(pdc.localCache, identity)
	} else {
		pdc.localCache[identity] = placementCache{
			siloAddr:      newAddr,
			cacheDeadline: time.Now().Add(pdc.cacheTime),
		}
	}
}

func (pdc *placementDriverClient) Logout(context.Context) error {
	return nil
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

func (u *User) Deactivate() error {
	//将User保存到数据库
	return nil
}

func factory(identity pd.GrainIdentity) UserObject {
	return &User{}
}

func init() {
	l := zap.NewZapLogger("grain_test.log", "./logfile", "debug", 1024*1024*100, 14, 28, true)
	clustergo.InitLogger(l.Sugar())
	InitLogger(l.Sugar())
}

func createSilo(node *clustergo.Node, pdc *placementDriverClient) *Silo {
	silo, _ := newSilo(context.Background(), pdc, node, []string{}, factory)
	node.RegisterBinrayHandler(Actor_request, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		req := RequestMsg{}
		if err := req.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			silo.OnRPCRequest(context.TODO(), from, &req)
		}
	}).RegisterBinrayHandler(Actor_notify_redirect, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdc.ResetPlacementCache(pd.GrainIdentity(msg[4:]), newAddr)
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
		placement: map[pd.GrainIdentity]*pdSilo{},
	}

	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.GrainIdentity]placementCache{},
		selfAddr:   node1Addr.LogicAddr(),
	}

	err := StartSilo(localDiscovery, node1Addr.LogicAddr(), pdClient1, []string{}, factory)
	if err != nil {
		panic(err)
	}

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node2.Start(localDiscovery, node2Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.GrainIdentity]placementCache{},
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
	}).RegisterBinrayHandler(Actor_notify_redirect, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient2.ResetPlacementCache(pd.GrainIdentity(msg[4:]), newAddr)
	})

	var resp echo.Response
	err = rpcClient.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, &resp)

	fmt.Println(err, &resp)
	node2.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = Call(ctx, "sniperHW@User", 1, &echo.Request{
		Msg: "Hello2",
	}, &resp)

	fmt.Println(err)

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
		placement: map[pd.GrainIdentity]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.GrainIdentity]placementCache{},
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
		localCache: map[pd.GrainIdentity]placementCache{},
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
	}).RegisterBinrayHandler(Actor_notify_redirect, func(from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient2.ResetPlacementCache(pd.GrainIdentity(msg[4:]), newAddr)
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var resp echo.Response
	err = rpcClient.Call(ctx, "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, &resp)

	fmt.Println(err, &resp)

	silo1.Stop()
	node1.Stop()
	node2.Stop()
}
