package goleans

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sniperHW/goleans/pd"
	"github.com/sniperHW/goleans/testproto/echo"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/logger/zap"
	"github.com/sniperHW/clustergo/membership"
	"github.com/stretchr/testify/assert"
)

type localMemberShip struct {
	nodes      map[addr.LogicAddr]*membership.Node
	subscribes []func(membership.MemberInfo)
}

// 订阅变更
func (d *localMemberShip) Subscribe(updateCB func(membership.MemberInfo)) error {
	d.subscribes = append(d.subscribes, updateCB)
	i := membership.MemberInfo{}
	for _, v := range d.nodes {
		i.Add = append(i.Add, *v)
	}
	updateCB(i)
	return nil
}

func (d *localMemberShip) AddNode(n *membership.Node) {
	d.nodes[n.Addr.LogicAddr()] = n
	add := membership.MemberInfo{
		Add: []membership.Node{*n},
	}
	for _, v := range d.subscribes {
		v(add)
	}
}

func (d *localMemberShip) RemoveNode(logicAddr addr.LogicAddr) {
	if n := d.nodes[logicAddr]; n != nil {
		delete(d.nodes, logicAddr)
		remove := membership.MemberInfo{
			Remove: []membership.Node{*n},
		}
		for _, v := range d.subscribes {
			v(remove)
		}
	}
}

func (d *localMemberShip) ModifyNode(modify *membership.Node) {
	if n, ok := d.nodes[modify.Addr.LogicAddr()]; ok {
		if n.Available != modify.Available || n.Addr.NetAddr() != modify.Addr.NetAddr() {
			logger.Debug("modify")
			d.nodes[modify.Addr.LogicAddr()] = modify
			//nodes := d.LoadNodeInfo()
			update := membership.MemberInfo{
				Update: []membership.Node{*modify},
			}

			for _, v := range d.subscribes {
				v(update)
			}
		}
	}
}

func (d *localMemberShip) Close() {

}

type pdSilo struct {
	logicAddr addr.LogicAddr
	grains    map[pd.Pid]struct{}
	metrics   pd.Metric
}

type placementDriver struct {
	sync.Mutex
	placement map[pd.Pid]*pdSilo
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
			grains:    map[pd.Pid]struct{}{},
		}
		p.silos = append(p.silos, s)
	}

	s.metrics = metric
}

func (p *placementDriver) GetPlacement(selfAddr addr.LogicAddr, identity pd.Pid) (addr.LogicAddr, error) {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[identity]
	if ok {
		logger.Debugf("GetPlacement 1 %v", silo.logicAddr.String())
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
			logger.Debugf("GetPlacement 2 %v", silo.logicAddr.String())
			return silo.logicAddr, nil
		} else {
			i = (i + 1) % len(p.silos)
			if i == p.nextSilo {
				return addr.LogicAddr(0), fmt.Errorf("no avaliable silo to activate grain:%v", identity)
			}
		}
	}
}

func (p *placementDriver) Deactivate(siloAddr addr.LogicAddr, identity pd.Pid) {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[identity]
	logger.Debugf("Deactivate")
	if ok && silo.logicAddr == siloAddr {
		logger.Debugf("Deactivate %v", silo.logicAddr.String())
		delete(silo.grains, identity)
		delete(p.placement, identity)
	}
}

func (p *placementDriver) Activate(siloAddr addr.LogicAddr, identity pd.Pid) error {
	p.Lock()
	defer p.Unlock()
	logger.Debugf("Activate")
	silo, ok := p.placement[identity]
	if !ok {
		for _, v := range p.silos {
			if v.logicAddr == siloAddr {
				v.grains[identity] = struct{}{}
				p.placement[identity] = v
				logger.Debugf("Activate %v", siloAddr.String())
				return nil
			}
		}
		panic("siloAddr not found")
	} else if silo.logicAddr == siloAddr {
		return nil
	} else {
		return pd.ErrorRedirect{Addr: silo.logicAddr}
	}
}

type placementCache struct {
	siloAddr      addr.LogicAddr
	cacheDeadline time.Time
}

type placementDriverClient struct {
	sync.Mutex
	driver     *placementDriver
	localCache map[pd.Pid]placementCache
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

func (pdc *placementDriverClient) GetPlacement(ctx context.Context, identity pd.Pid) (addr.LogicAddr, error) {
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

func (pdc *placementDriverClient) Activate(ctx context.Context, identity pd.Pid) error {
	return pdc.driver.Activate(pdc.selfAddr, identity)
}

func (pdc *placementDriverClient) Deactivate(ctx context.Context, identity pd.Pid) error {
	pdc.driver.Deactivate(pdc.selfAddr, identity)
	return nil
}

func (pdc *placementDriverClient) MarkUnAvaliable() {

}

func (pdc *placementDriverClient) ResetPlacementCache(identity pd.Pid, newAddr addr.LogicAddr) {
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
		Msg: fmt.Sprintf("%s -> %s", arg.Msg, u.grain.Pid()),
	})
}

func (u *User) Init(grain *Grain) error {
	//从数据库加载数据，初始化User
	u.grain = grain
	grain.RegisterMethod(1, u.Echo)
	grain.AddCallPipeline(func(replyer *Replyer, req *RequestMsg) bool {
		beg := time.Now()
		replyer.SetReplyHook(func(_ *RequestMsg) {
			logger.Debugf("call %s.%d(%v) use:%v", grain.Pid(), req.Method, req.arg, time.Now().Sub(beg))
		})
		return true
	})
	return nil
}

func (u *User) Deactivate() error {
	//将User保存到数据库
	return nil
}

func factory(grainType string) UserObject {
	return &User{}
}

func init() {
	l := zap.NewZapLogger("grain_test.log", "./logfile", "debug", 1024*1024*100, 14, 28, true)
	clustergo.InitLogger(l.Sugar())
	InitLogger(l.Sugar())
}

func createSilo(node *clustergo.Node, pdc *placementDriverClient) *Silo {
	silo, _ := newSilo(context.Background(), pdc, node, []GrainCfg{
		{
			Type:          "User",
			NormalBoxCap:  32,
			UrgentBoxCap:  32,
			AwaitQueueCap: 23,
		},
	}, factory)
	node.RegisterBinaryHandler(Actor_request, func(ctx context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		req := RequestMsg{}
		if err := req.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			silo.OnRPCRequest(ctx, from, &req)
		}
	}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdc.ResetPlacementCache(pd.Pid(msg[4:]), newAddr)
	})
	return silo
}

func TestGoleans(t *testing.T) {
	localDiscovery := &localMemberShip{
		nodes: map[addr.LogicAddr]*membership.Node{},
	}

	node1Addr, _ := addr.MakeAddr("1.1.1", "localhost:28110")
	node2Addr, _ := addr.MakeAddr("1.2.1", "localhost:28111")

	localDiscovery.AddNode(&membership.Node{
		Addr:      node1Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node2Addr,
		Available: true,
	})

	pdServer := &placementDriver{
		placement: map[pd.Pid]*pdSilo{},
	}

	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node1Addr.LogicAddr(),
	}

	err := StartSilo(localDiscovery, node1Addr.LogicAddr(), pdClient1, []GrainCfg{
		{
			Type:          "User",
			NormalBoxCap:  32,
			UrgentBoxCap:  32,
			AwaitQueueCap: 23,
		},
	}, factory)
	if err != nil {
		panic(err)
	}

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node2.Start(localDiscovery, node2Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node2Addr.LogicAddr(),
	}

	rpcClient := NewRPCClient(node2, pdClient2)

	node2.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient2.ResetPlacementCache(pd.Pid(msg[4:]), newAddr)
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

	localDiscovery := &localMemberShip{
		nodes: map[addr.LogicAddr]*membership.Node{},
	}

	node1Addr, _ := addr.MakeAddr("1.1.1", "localhost:28110")
	node2Addr, _ := addr.MakeAddr("1.2.1", "localhost:28111")

	localDiscovery.AddNode(&membership.Node{
		Addr:      node1Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node2Addr,
		Available: true,
	})

	pdServer := &placementDriver{
		placement: map[pd.Pid]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
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
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node2Addr.LogicAddr(),
	}

	rpcClient := NewRPCClient(node2, pdClient2)

	node2.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient2.ResetPlacementCache(pd.Pid(msg[4:]), newAddr)
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

func TestRedirect(t *testing.T) {

	DefaultDeactiveTime = time.Second * 2
	GrainTickInterval = time.Second

	localDiscovery := &localMemberShip{
		nodes: map[addr.LogicAddr]*membership.Node{},
	}

	node1Addr, _ := addr.MakeAddr("1.1.1", "localhost:28110")
	node2Addr, _ := addr.MakeAddr("1.2.1", "localhost:28111")
	node3Addr, _ := addr.MakeAddr("1.3.1", "localhost:28112")
	node4Addr, _ := addr.MakeAddr("1.4.1", "localhost:28113")

	localDiscovery.AddNode(&membership.Node{
		Addr:      node1Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node2Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node3Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node4Addr,
		Available: true,
	})

	pdServer := &placementDriver{
		placement: map[pd.Pid]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node1Addr.LogicAddr(),
	}
	silo1 := createSilo(node1, pdClient1)

	err := node1.Start(localDiscovery, node1Addr.LogicAddr())
	assert.Nil(t, err)

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node2Addr.LogicAddr(),
	}
	silo2 := createSilo(node2, pdClient2)
	err = node2.Start(localDiscovery, node2Addr.LogicAddr())
	assert.Nil(t, err)

	node3 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node3.Start(localDiscovery, node3Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient3 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	pdClient3.SetCacheTime(time.Second * 10)

	rpcClient := NewRPCClient(node3, pdClient3)

	node3.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient3.ResetPlacementCache(pd.Pid(msg[4:]), newAddr)
	})

	var resp echo.Response
	err = rpcClient.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, &resp)

	fmt.Println(err, &resp)

	node4 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node4.Start(localDiscovery, node4Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient4 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	rpcClient4 := NewRPCClient(node4, pdClient4)

	node4.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient4.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient4.ResetPlacementCache(pd.Pid(msg[4:]), newAddr)
	})

	time.Sleep(time.Second * 5)

	err = rpcClient4.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, &resp)

	fmt.Println(err, &resp)

	err = rpcClient.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, &resp)

	fmt.Println(err, &resp)

	silo1.Stop()
	silo2.Stop()
	node1.Stop()
	node2.Stop()
	node3.Stop()
	node4.Stop()

	GrainTickInterval = time.Second * 30
	DefaultDeactiveTime = time.Minute * 5
}

func TestOneway(t *testing.T) {

	DefaultDeactiveTime = time.Second * 2
	GrainTickInterval = time.Second

	localDiscovery := &localMemberShip{
		nodes: map[addr.LogicAddr]*membership.Node{},
	}

	node1Addr, _ := addr.MakeAddr("1.1.1", "localhost:28110")
	node2Addr, _ := addr.MakeAddr("1.2.1", "localhost:28111")
	node3Addr, _ := addr.MakeAddr("1.3.1", "localhost:28112")
	node4Addr, _ := addr.MakeAddr("1.4.1", "localhost:28113")

	localDiscovery.AddNode(&membership.Node{
		Addr:      node1Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node2Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node3Addr,
		Available: true,
	})

	localDiscovery.AddNode(&membership.Node{
		Addr:      node4Addr,
		Available: true,
	})

	pdServer := &placementDriver{
		placement: map[pd.Pid]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node1Addr.LogicAddr(),
	}
	silo1 := createSilo(node1, pdClient1)

	err := node1.Start(localDiscovery, node1Addr.LogicAddr())
	assert.Nil(t, err)

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node2Addr.LogicAddr(),
	}
	silo2 := createSilo(node2, pdClient2)
	err = node2.Start(localDiscovery, node2Addr.LogicAddr())
	assert.Nil(t, err)

	node3 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node3.Start(localDiscovery, node3Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient3 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	pdClient3.SetCacheTime(time.Second * 10)

	rpcClient := NewRPCClient(node3, pdClient3)

	node3.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient3.ResetPlacementCache(pd.Pid(msg[4:]), newAddr)
	})

	//var resp echo.Response
	err = rpcClient.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, nil)

	time.Sleep(time.Millisecond * 100)

	node4 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	err = node4.Start(localDiscovery, node4Addr.LogicAddr())
	assert.Nil(t, err)

	pdClient4 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[pd.Pid]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	rpcClient4 := NewRPCClient(node4, pdClient4)

	node4.RegisterBinaryHandler(Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient4.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient4.ResetPlacementCache(pd.Pid(msg[4:]), newAddr)
	})

	time.Sleep(time.Second * 5)

	err = rpcClient4.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, nil)

	time.Sleep(time.Millisecond * 100)

	err = rpcClient.Call(context.Background(), "sniperHW@User", 1, &echo.Request{
		Msg: "Hello",
	}, nil)

	silo1.Stop()
	silo2.Stop()
	node1.Stop()
	node2.Stop()
	node3.Stop()
	node4.Stop()

	GrainTickInterval = time.Second * 30
	DefaultDeactiveTime = time.Minute * 5
}
