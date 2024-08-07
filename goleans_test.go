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

	"github.com/sniperHW/goleans/grain"
	"github.com/sniperHW/goleans/pd"
	"github.com/sniperHW/goleans/rpc"
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
	grains    map[string]struct{}
	metrics   pd.Metric
}

type placementDriver struct {
	sync.Mutex
	placement map[string]*pdSilo
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
			grains:    map[string]struct{}{},
		}
		p.silos = append(p.silos, s)
	}

	s.metrics = metric
}

func (p *placementDriver) GetPlacement(selfAddr addr.LogicAddr, pid string) (addr.LogicAddr, error) {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[pid]
	if ok {
		logger.Debugf("GetPlacement 1 %v", silo.logicAddr.String())
		return silo.logicAddr, nil
	}

	if len(p.silos) == 0 {
		return addr.LogicAddr(0), fmt.Errorf("no avaliable silo to activate grain:%v", pid)
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
				return addr.LogicAddr(0), fmt.Errorf("no avaliable silo to activate grain:%v", pid)
			}
		}
	}
}

func (p *placementDriver) Remove(siloAddr addr.LogicAddr, pid string) {
	p.Lock()
	defer p.Unlock()
	silo, ok := p.placement[pid]
	logger.Debugf("Remove")
	if ok && silo.logicAddr == siloAddr {
		logger.Debugf("Remove %v", silo.logicAddr.String())
		delete(silo.grains, pid)
		delete(p.placement, pid)
	}
}

func (p *placementDriver) Place(siloAddr addr.LogicAddr, pid string) error {
	p.Lock()
	defer p.Unlock()
	logger.Debugf("Place")
	silo, ok := p.placement[pid]
	if !ok {
		for _, v := range p.silos {
			if v.logicAddr == siloAddr {
				v.grains[pid] = struct{}{}
				p.placement[pid] = v
				logger.Debugf("Place %v", siloAddr.String())
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
	localCache map[string]placementCache
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

func (pdc *placementDriverClient) GetPlacement(ctx context.Context, pid string) (addr.LogicAddr, error) {
	pdc.Lock()
	defer pdc.Unlock()
	now := time.Now()
	cache, ok := pdc.localCache[pid]
	if ok && cache.cacheDeadline.After(now) {
		return cache.siloAddr, nil
	} else {
		siloAddr, err := pdc.driver.GetPlacement(pdc.selfAddr, pid)
		if err == nil {
			pdc.localCache[pid] = placementCache{
				siloAddr:      siloAddr,
				cacheDeadline: time.Now().Add(pdc.cacheTime),
			}
		}
		return siloAddr, err
	}
}

func (pdc *placementDriverClient) Place(ctx context.Context, pid string) error {
	return pdc.driver.Place(pdc.selfAddr, pid)
}

func (pdc *placementDriverClient) Remove(ctx context.Context, pid string) error {
	pdc.driver.Remove(pdc.selfAddr, pid)
	return nil
}

func (pdc *placementDriverClient) MarkUnAvaliable() {

}

func (pdc *placementDriverClient) ResetPlacementCache(pid string, newAddr addr.LogicAddr) {
	pdc.Lock()
	defer pdc.Unlock()
	if newAddr.Empty() {
		delete(pdc.localCache, pid)
	} else {
		pdc.localCache[pid] = placementCache{
			siloAddr:      newAddr,
			cacheDeadline: time.Now().Add(pdc.cacheTime),
		}
	}
}

func (pdc *placementDriverClient) Logout(context.Context) error {
	return nil
}

type User struct {
	ctx grain.Context
}

func (u *User) Echo(ctx context.Context, r rpc.Replyer, arg *echo.Request) {
	r.Reply(&echo.Response{
		Msg: fmt.Sprintf("%s -> %s", arg.Msg, u.ctx.Pid()),
	})
}

func (u *User) Activate(ctx grain.Context) (error, bool) {
	//从数据库加载数据，初始化User
	u.ctx = ctx
	ctx.RegisterMethod(1, u.Echo)
	ctx.AddCallPipeline(func(replyer rpc.Replyer, req *rpc.RequestMsg) bool {
		beg := time.Now()
		replyer.SetReplyHook(func(_ *rpc.RequestMsg) {
			logger.Debugf("call %s.%d(%v) use:%v", ctx.Pid(), req.Method, req.Argument, time.Now().Sub(beg))
		})
		return true
	})
	return nil, false
}

func (u *User) Deactivate() error {
	//将User保存到数据库
	return nil
}

func factory(grainType string) Grain {
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
			QueueCap:      32,
			AwaitQueueCap: 23,
		},
	}, factory)
	node.RegisterBinaryHandler(rpc.Actor_request, func(ctx context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		req := rpc.RequestMsg{}
		if err := req.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			silo.OnRPCRequest(ctx, from, &req)
		}
	}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdc.ResetPlacementCache(string(msg[4:]), newAddr)
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
		placement: map[string]*pdSilo{},
	}

	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]placementCache{},
		selfAddr:   node1Addr.LogicAddr(),
	}

	err := StartSilo(localDiscovery, node1Addr.LogicAddr(), pdClient1, []GrainCfg{
		{
			Type:          "User",
			QueueCap:      32,
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
		localCache: map[string]placementCache{},
		selfAddr:   node2Addr.LogicAddr(),
	}

	rpcClient := rpc.NewClient(node2, pdClient2)

	node2.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := rpc.ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient2.ResetPlacementCache(string(msg[4:]), newAddr)
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
		placement: map[string]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]placementCache{},
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
		localCache: map[string]placementCache{},
		selfAddr:   node2Addr.LogicAddr(),
	}

	rpcClient := rpc.NewClient(node2, pdClient2)

	node2.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := rpc.ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient2.ResetPlacementCache(string(msg[4:]), newAddr)
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
		placement: map[string]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]placementCache{},
		selfAddr:   node1Addr.LogicAddr(),
	}
	silo1 := createSilo(node1, pdClient1)

	err := node1.Start(localDiscovery, node1Addr.LogicAddr())
	assert.Nil(t, err)

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]placementCache{},
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
		localCache: map[string]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	pdClient3.SetCacheTime(time.Second * 10)

	rpcClient := rpc.NewClient(node3, pdClient3)

	node3.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := rpc.ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient3.ResetPlacementCache(string(msg[4:]), newAddr)
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
		localCache: map[string]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	rpcClient4 := rpc.NewClient(node4, pdClient4)

	node4.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := rpc.ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient4.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient4.ResetPlacementCache(string(msg[4:]), newAddr)
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
		placement: map[string]*pdSilo{},
	}

	node1 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient1 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]placementCache{},
		selfAddr:   node1Addr.LogicAddr(),
	}
	silo1 := createSilo(node1, pdClient1)

	err := node1.Start(localDiscovery, node1Addr.LogicAddr())
	assert.Nil(t, err)

	node2 := clustergo.NewClusterNode(clustergo.JsonCodec{})
	pdClient2 := &placementDriverClient{
		driver:     pdServer,
		localCache: map[string]placementCache{},
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
		localCache: map[string]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	pdClient3.SetCacheTime(time.Second * 10)

	rpcClient := rpc.NewClient(node3, pdClient3)

	node3.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := rpc.ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient3.ResetPlacementCache(string(msg[4:]), newAddr)
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
		localCache: map[string]placementCache{},
		selfAddr:   node3Addr.LogicAddr(),
	}

	rpcClient4 := rpc.NewClient(node4, pdClient4)

	node4.RegisterBinaryHandler(rpc.Actor_response, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		resp := rpc.ResponseMsg{}
		if err := resp.Decode(msg); err != nil {
			logger.Error(err)
		} else {
			logger.Debugf("resp from %v", from.String())
			rpcClient4.OnRPCResponse(&resp)
		}
	}).RegisterBinaryHandler(rpc.Actor_notify_redirect, func(_ context.Context, from addr.LogicAddr, cmd uint16, msg []byte) {
		newAddr := addr.LogicAddr(binary.BigEndian.Uint32(msg[:4]))
		pdClient4.ResetPlacementCache(string(msg[4:]), newAddr)
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
