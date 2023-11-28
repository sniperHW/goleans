package placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"goleans/pd"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/codec/buffer"
	"github.com/sniperHW/netgo"
	"go.uber.org/zap"
)

const (
	loginReq = iota + 1
	loginResp
	logoutReq
	logoutResp
	heartbeatReq
	heartbeatResp
	activateReq
	activateResp
	deactivateReq
	deactivateResp
	getplacementReq
	getplacementResp
	mark_unavaliableReq
	mark_unavaliableResp
)

var logger *zap.Logger

func InitLogger(l *zap.Logger) {
	logger = l
}

type LoginReq struct {
	Addr   addr.LogicAddr
	Metric pd.Metric
}

type LoginResp struct {
}

type LogoutReq struct {
	Addr addr.LogicAddr
}

type LogoutResp struct {
}

type HeartbeatReq struct {
	Addr   addr.LogicAddr
	Metric pd.Metric
}

type HeartbeatResp struct {
}

type ActivateReq struct {
	Addr     addr.LogicAddr
	Identity pd.GrainIdentity
}

type ActivateResp struct {
	Addr addr.LogicAddr
}

type DeactivateReq struct {
	Addr     addr.LogicAddr
	Identity pd.GrainIdentity
}

type DeactivateResp struct {
}

type GetPlacementReq struct {
	Identity pd.GrainIdentity
}

type GetPlacementResp struct {
	Err  *string
	Addr addr.LogicAddr
}

type MarkUnAvaliableReq struct {
	Addr addr.LogicAddr
}

type MarkUnAvaliableResp struct {
}

type Message struct {
	Seq     uint32
	PayLoad interface{}
}

type codec struct {
	buff   []byte
	w      int
	r      int
	reader buffer.BufferReader
}

func (cc *codec) Encode(buffs net.Buffers, o interface{}) (net.Buffers, int) {
	switch o := o.(type) {
	case *Message:
		if buff, err := json.Marshal(o.PayLoad); err != nil {
			log.Println("json.Marshal error:", err)
			return buffs, 0
		} else {
			b := make([]byte, 0, 12)
			b = buffer.AppendUint32(b, uint32(len(buff)+4+4))
			//logger.Sugar().Debugf("encode seq:%d", o.Seq)
			b = buffer.AppendUint32(b, o.Seq)
			switch o.PayLoad.(type) {
			case *LoginReq:
				b = buffer.AppendUint32(b, loginReq)
			case *LoginResp:
				b = buffer.AppendUint32(b, loginResp)
			case *LogoutReq:
				b = buffer.AppendUint32(b, logoutReq)
			case *LogoutResp:
				b = buffer.AppendUint32(b, logoutResp)
			case *HeartbeatReq:
				b = buffer.AppendUint32(b, heartbeatReq)
			case *HeartbeatResp:
				b = buffer.AppendUint32(b, heartbeatResp)
			case *ActivateReq:
				b = buffer.AppendUint32(b, activateReq)
			case *ActivateResp:
				b = buffer.AppendUint32(b, activateResp)
			case *DeactivateReq:
				b = buffer.AppendUint32(b, deactivateReq)
			case *DeactivateResp:
				b = buffer.AppendUint32(b, deactivateResp)
			case *GetPlacementReq:
				b = buffer.AppendUint32(b, getplacementReq)
			case *GetPlacementResp:
				b = buffer.AppendUint32(b, getplacementResp)
			case *MarkUnAvaliableReq:
				b = buffer.AppendUint32(b, mark_unavaliableReq)
			case *MarkUnAvaliableResp:
				b = buffer.AppendUint32(b, mark_unavaliableResp)
			default:
				log.Println("invaild packet")
				return buffs, 0
			}
			return append(buffs, b, buff), len(b) + len(buff)
		}

	default:
		return buffs, 0
	}
}

func (cc *codec) Decode(payload []byte) (interface{}, error) {
	cc.reader.Reset(payload)
	seq := cc.reader.GetUint32()
	cmd := cc.reader.GetUint32()
	//logger.Sugar().Debugf("decode %d,%d", cmd, seq)
	var o interface{}
	switch cmd {
	case loginReq:
		o = &LoginReq{}
	case loginResp:
		o = &LoginResp{}
	case logoutReq:
		o = &LogoutReq{}
	case logoutResp:
		o = &LogoutResp{}
	case heartbeatReq:
		o = &HeartbeatReq{}
	case heartbeatResp:
		o = &HeartbeatResp{}
	case activateReq:
		o = &ActivateReq{}
	case activateResp:
		o = &ActivateResp{}
	case deactivateReq:
		o = &DeactivateReq{}
	case deactivateResp:
		o = &DeactivateResp{}
	case getplacementReq:
		o = &GetPlacementReq{}
	case getplacementResp:
		o = &GetPlacementResp{}
	case mark_unavaliableReq:
		o = &MarkUnAvaliableReq{}
	case mark_unavaliableResp:
		o = &MarkUnAvaliableResp{}
	default:
		return nil, errors.New("invaild packet")
	}

	err := json.Unmarshal(payload[8:], o)
	return &Message{Seq: seq, PayLoad: o}, err
}

func (cc *codec) read(readable netgo.ReadAble, deadline time.Time) (int, error) {
	if err := readable.SetReadDeadline(deadline); err != nil {
		return 0, err
	} else {
		return readable.Read(cc.buff[cc.w:])
	}
}

func (cc *codec) Recv(readable netgo.ReadAble, deadline time.Time) (pkt []byte, err error) {
	sizeLen := 4
	for {
		unpackSize := cc.w - cc.r
		if unpackSize >= sizeLen {
			cc.reader.Reset(cc.buff[cc.r:cc.w])
			payload := int(cc.reader.GetUint32())

			if payload == 0 {
				return nil, fmt.Errorf("zero payload")
			}

			totalSize := payload + sizeLen

			if totalSize <= unpackSize {
				cc.r += sizeLen
				pkt := cc.buff[cc.r : cc.r+payload]
				cc.r += payload
				if cc.r == cc.w {
					cc.r = 0
					cc.w = 0
				}
				return pkt, nil
			} else {
				if totalSize > cap(cc.buff) {
					buff := make([]byte, totalSize)
					copy(buff, cc.buff[cc.r:cc.w])
					cc.buff = buff
				} else {
					//空间足够容纳下一个包，
					copy(cc.buff, cc.buff[cc.r:cc.w])
				}
				cc.w = cc.w - cc.r
				cc.r = 0
			}
		}

		var n int
		n, err = cc.read(readable, deadline)
		if n > 0 {
			cc.w += n
		}
		if nil != err {
			return
		}
	}
}

type silo struct {
	Addr      addr.LogicAddr
	Metric    pd.Metric
	Abaliable bool
	last      time.Time
}

type tmpPlacement struct {
	identity pd.GrainIdentity
	addr     addr.LogicAddr
	timer    *time.Timer
}

type placementSvr struct {
	sync.Mutex
	Placement     map[pd.GrainIdentity]addr.LogicAddr
	tempPlacement map[pd.GrainIdentity]*tmpPlacement
	Silos         map[addr.LogicAddr]*silo
	siloArray     []*silo //可供分配的silo
	storage       *os.File
}

func NewServer(storage string) (*placementSvr, error) {
	f, err := os.OpenFile(storage, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	svr := &placementSvr{
		Placement:     map[pd.GrainIdentity]addr.LogicAddr{},
		tempPlacement: map[pd.GrainIdentity]*tmpPlacement{},
		Silos:         map[addr.LogicAddr]*silo{},
	}
	decoder := json.NewDecoder(f)
	err = decoder.Decode(svr)
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}
	f.Close()

	for _, v := range svr.Silos {
		if v.Abaliable {
			svr.siloArray = append(svr.siloArray, v)
		}
	}

	svr.storage, _ = os.OpenFile(storage, os.O_CREATE|os.O_WRONLY, 0644)
	return svr, nil
}

func (s *placementSvr) save() {
	s.storage.Truncate(0)
	s.storage.Seek(0, 0)
	b, _ := json.Marshal(s)
	s.storage.Write(b)
	s.storage.Sync()
}

func (s *placementSvr) Login(sess *netgo.AsynSocket, msg *Message) {
	s.Lock()
	defer s.Unlock()
	req := msg.PayLoad.(*LoginReq)
	if _, ok := s.Silos[req.Addr]; !ok {
		s.Silos[req.Addr] = &silo{
			Addr:      req.Addr,
			Metric:    req.Metric,
			Abaliable: true,
		}
		s.siloArray = append(s.siloArray, s.Silos[req.Addr])
		s.save()
	}

	logger.Sugar().Debugf("on login %v seq:%d", req, msg.Seq)

	sess.Send(&Message{Seq: msg.Seq, PayLoad: &LoginResp{}})
}

func (s *placementSvr) Logout(sess *netgo.AsynSocket, msg *Message) {
	s.Lock()
	defer s.Unlock()
	req := msg.PayLoad.(*LogoutReq)
	delete(s.Silos, req.Addr)
	for i := 0; i < len(s.siloArray); i++ {
		if s.siloArray[i].Addr == req.Addr {
			s.siloArray[i], s.siloArray[len(s.siloArray)-1] = s.siloArray[len(s.siloArray)-1], s.siloArray[i]
			s.siloArray = s.siloArray[:len(s.siloArray)-1]
			break
		}
	}
	s.save()
	sess.Send(&Message{Seq: msg.Seq, PayLoad: &LogoutResp{}})
}

func (s *placementSvr) Heartbeat(sess *netgo.AsynSocket, msg *Message) {
	s.Lock()
	defer s.Unlock()
	req := msg.PayLoad.(*HeartbeatReq)
	if silo, ok := s.Silos[req.Addr]; ok {
		silo.Metric = req.Metric
		silo.last = time.Now()
		s.save()
	}
	sess.Send(&Message{Seq: msg.Seq, PayLoad: &HeartbeatResp{}})
}

func (s *placementSvr) Activate(sess *netgo.AsynSocket, msg *Message) {
	s.Lock()
	defer s.Unlock()
	req := msg.PayLoad.(*ActivateReq)
	if place, ok := s.Placement[req.Identity]; ok {
		if place != req.Addr {
			sess.Send(&Message{Seq: msg.Seq, PayLoad: &ActivateResp{Addr: place}})
			return
		}
	}

	s.Placement[req.Identity] = req.Addr

	if tmp := s.tempPlacement[req.Identity]; tmp != nil {
		logger.Sugar().Debug("delete tempPlacement")
		tmp.timer.Stop()
		delete(s.tempPlacement, req.Identity)
	}
	s.save()
	sess.Send(&Message{Seq: msg.Seq, PayLoad: &ActivateResp{Addr: req.Addr}})
}

func (s *placementSvr) Deactivate(sess *netgo.AsynSocket, msg *Message) {
	s.Lock()
	defer s.Unlock()
	req := msg.PayLoad.(*DeactivateReq)
	if place, ok := s.Placement[req.Identity]; ok {
		if place == req.Addr {
			delete(s.Placement, req.Identity)
			if tmp := s.tempPlacement[req.Identity]; tmp != nil {
				tmp.timer.Stop()
				delete(s.tempPlacement, req.Identity)
			}
			s.save()
		}
	}
	sess.Send(&Message{Seq: msg.Seq, PayLoad: &DeactivateResp{}})
}

func (s *placementSvr) GetPlacement(sess *netgo.AsynSocket, msg *Message) {
	s.Lock()
	defer s.Unlock()
	req := msg.PayLoad.(*GetPlacementReq)
	if place, ok := s.Placement[req.Identity]; ok {
		sess.Send(&Message{Seq: msg.Seq, PayLoad: &GetPlacementResp{Addr: place}})
		return
	}

	if place, ok := s.tempPlacement[req.Identity]; ok {
		sess.Send(&Message{Seq: msg.Seq, PayLoad: &GetPlacementResp{Addr: place.addr}})
		return
	}

	if len(s.siloArray) > 0 {
		silo := s.siloArray[int(rand.Int31())%len(s.siloArray)]
		tmp := &tmpPlacement{
			identity: req.Identity,
			addr:     silo.Addr,
		}
		tmp.timer = time.AfterFunc(time.Second*5, func() {
			s.Lock()
			defer s.Unlock()
			if s.tempPlacement[req.Identity] == tmp {
				delete(s.tempPlacement, req.Identity)
			}
		})
		s.tempPlacement[req.Identity] = tmp
		sess.Send(&Message{Seq: msg.Seq, PayLoad: &GetPlacementResp{Addr: silo.Addr}})
	} else {
		err := "no avaliable silo for grain"
		sess.Send(&Message{Seq: msg.Seq, PayLoad: &GetPlacementResp{Err: &err}})
	}
}

func (s *placementSvr) MarkUnAvaliable(sess *netgo.AsynSocket, msg *Message) {
	s.Lock()
	defer s.Unlock()
	req := msg.PayLoad.(*MarkUnAvaliableReq)
	if silo, ok := s.Silos[req.Addr]; ok {
		silo.Abaliable = false
		for i := 0; i < len(s.siloArray); i++ {
			if s.siloArray[i].Addr == req.Addr {
				s.siloArray[i], s.siloArray[len(s.siloArray)-1] = s.siloArray[len(s.siloArray)-1], s.siloArray[i]
				s.siloArray = s.siloArray[:len(s.siloArray)-1]
				break
			}
		}
		s.save()
	}
	sess.Send(&Message{Seq: msg.Seq, PayLoad: &MarkUnAvaliableResp{}})
}

func (svr *placementSvr) Start(service string) error {
	_, serve, err := netgo.ListenTCP("tcp", service, func(conn *net.TCPConn) {
		log.Println("new client")
		cc := &codec{
			buff: make([]byte, 65535),
		}
		netgo.NewAsynSocket(netgo.NewTcpSocket(conn, cc),
			netgo.AsynSocketOption{
				Codec:    cc,
				AutoRecv: true,
			}).SetCloseCallback(func(s *netgo.AsynSocket, _ error) {
		}).SetPacketHandler(func(_ context.Context, as *netgo.AsynSocket, packet interface{}) error {
			switch packet.(*Message).PayLoad.(type) {
			case *LoginReq:
				svr.Login(as, packet.(*Message))
			case *LogoutReq:
				svr.Logout(as, packet.(*Message))
			case *HeartbeatReq:
				svr.Heartbeat(as, packet.(*Message))
			case *MarkUnAvaliableReq:
				svr.MarkUnAvaliable(as, packet.(*Message))
			case *GetPlacementReq:
				svr.GetPlacement(as, packet.(*Message))
			case *ActivateReq:
				svr.Activate(as, packet.(*Message))
			case *DeactivateReq:
				svr.Deactivate(as, packet.(*Message))
			}
			return nil
		}).Recv()

	})

	if err != nil {
		return err
	} else {
		go serve()
		return nil
	}
}

type placementCache struct {
	addr          addr.LogicAddr
	cacheDeadline time.Time
}

type placementCli struct {
	sync.Mutex
	callMtx    sync.Mutex
	localCache map[pd.GrainIdentity]placementCache
	selfAddr   addr.LogicAddr
	getMetric  func() pd.Metric
	cacheTime  time.Duration
	server     string
	session    *netgo.AsynSocket
	nextseq    uint32
	pending    map[uint32]func(*Message)
	closed     chan struct{}
}

func NewCli(selfAddr addr.LogicAddr, server string) *placementCli {
	cli := &placementCli{
		localCache: map[pd.GrainIdentity]placementCache{},
		selfAddr:   selfAddr,
		server:     server,
		closed:     make(chan struct{}),
		pending:    map[uint32]func(*Message){},
	}
	return cli
}

func (cli *placementCli) addPending(seq uint32, fn func(*Message)) {
	cli.callMtx.Lock()
	defer cli.callMtx.Unlock()
	cli.pending[seq] = fn
}

func (cli *placementCli) remPending(seq uint32) (fn func(*Message)) {
	cli.callMtx.Lock()
	defer cli.callMtx.Unlock()
	fn = cli.pending[seq]
	delete(cli.pending, seq)
	return fn
}

func (cli *placementCli) onResponse(rsp *Message) {
	if fn := cli.remPending(rsp.Seq); fn != nil {
		fn(rsp)
	} else {
		logger.Sugar().Debugf("call %d no context", rsp.Seq)
	}
}

func (cli *placementCli) call(ctx context.Context, req *Message) (resp *Message, err error) {
	cli.callMtx.Lock()
	cli.nextseq++
	req.Seq = cli.nextseq
	if cli.session == nil {
		err = cli.dial()
		if err != nil {
			cli.callMtx.Unlock()
			return nil, err
		}
	}

	c := make(chan *Message)

	//logger.Sugar().Debugf("call seq:%d", req.Seq)

	cli.pending[req.Seq] = func(m *Message) {
		c <- m
	}

	cli.session.Send(req)
	cli.callMtx.Unlock()
	select {
	case resp = <-c:
		return resp, nil
	case <-ctx.Done():
		cli.remPending(req.Seq)
		return nil, ctx.Err()
	}
}

func (cli *placementCli) dial() error {
	//logger.Sugar().Debug("dial")
	dialer := &net.Dialer{}
	if conn, err := dialer.Dial("tcp", cli.server); err != nil {
		return err
	} else {
		cc := &codec{
			buff: make([]byte, 65535),
		}
		as := netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), cc),
			netgo.AsynSocketOption{
				Codec:    cc,
				AutoRecv: true,
			}).SetCloseCallback(func(_ *netgo.AsynSocket, _ error) {
			cli.Lock()
			defer cli.Unlock()
			cli.session = nil
		}).SetPacketHandler(func(_ context.Context, as *netgo.AsynSocket, packet interface{}) error {
			cli.onResponse(packet.(*Message))
			return nil
		}).Recv()
		logger.Sugar().Debug("connect server ok")
		cli.session = as
		return nil
	}
}

func (cli *placementCli) SetGetMetric(fn func() pd.Metric) {
	cli.getMetric = fn
}

func (cli *placementCli) SetCacheTime(d time.Duration) {
	cli.cacheTime = d
}

func (cli *placementCli) Login(ctx context.Context) error {
	req := &Message{
		PayLoad: &LoginReq{
			Addr:   cli.selfAddr,
			Metric: cli.getMetric(),
		},
	}

	_, err := cli.call(ctx, req)

	if err == nil && cli.getMetric != nil {
		go func() {
			for {
				time.Sleep(time.Second * 10)
				select {
				case <-cli.closed:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				cli.call(ctx, &Message{
					PayLoad: &HeartbeatReq{
						Addr:   cli.selfAddr,
						Metric: cli.getMetric(),
					},
				})
				cancel()
			}
		}()
	}

	return err
}

func (cli *placementCli) Logout(ctx context.Context) error {
	req := &Message{
		PayLoad: &LogoutReq{
			Addr: cli.selfAddr,
		},
	}
	_, err := cli.call(ctx, req)
	return err
}

func (cli *placementCli) MarkUnAvaliable() {
	req := &Message{
		PayLoad: &MarkUnAvaliableReq{
			Addr: cli.selfAddr,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	cli.call(ctx, req)
	return
}

func (cli *placementCli) ResetPlacementCache(identity pd.GrainIdentity, newAddr addr.LogicAddr) {
	cli.Lock()
	defer cli.Unlock()
	if newAddr.Empty() {
		delete(cli.localCache, identity)
	} else {
		cli.localCache[identity] = placementCache{
			addr:          newAddr,
			cacheDeadline: time.Now().Add(cli.cacheTime),
		}
	}
}

func (cli *placementCli) Activate(ctx context.Context, identity pd.GrainIdentity) error {
	req := &Message{
		PayLoad: &ActivateReq{
			Identity: identity,
			Addr:     cli.selfAddr,
		},
	}

	resp, err := cli.call(ctx, req)
	if err != nil {
		return err
	}

	if resp.PayLoad.(*ActivateResp).Addr == cli.selfAddr {
		cli.Lock()
		defer cli.Unlock()
		cli.localCache[identity] = placementCache{
			addr: resp.PayLoad.(*ActivateResp).Addr,
		}
		return nil
	} else {
		return pd.ErrorRedirect{Addr: resp.PayLoad.(*ActivateResp).Addr}
	}
}

func (cli *placementCli) Deactivate(ctx context.Context, identity pd.GrainIdentity) error {
	req := &Message{
		PayLoad: &DeactivateReq{
			Identity: identity,
			Addr:     cli.selfAddr,
		},
	}

	_, err := cli.call(ctx, req)
	if err != nil {
		return err
	}
	cli.Lock()
	defer cli.Unlock()
	delete(cli.localCache, identity)

	return nil
}

func (cli *placementCli) GetPlacement(ctx context.Context, identity pd.GrainIdentity) (addr.LogicAddr, error) {
	cli.Lock()
	defer cli.Unlock()
	cache, ok := cli.localCache[identity]
	if ok && time.Now().Before(cache.cacheDeadline) {
		return cache.addr, nil
	}

	req := &Message{
		PayLoad: &GetPlacementReq{
			Identity: identity,
		},
	}

	resp, err := cli.call(ctx, req)
	if err != nil {
		return addr.LogicAddr(0), err
	}

	getplacementResp := resp.PayLoad.(*GetPlacementResp)
	if getplacementResp.Err != nil {
		return addr.LogicAddr(0), errors.New(*getplacementResp.Err)
	} else {
		return getplacementResp.Addr, nil
	}
}
