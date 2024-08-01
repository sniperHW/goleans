package goleans

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/goleans/grain"
	"github.com/sniperHW/goleans/rpc"

	"google.golang.org/protobuf/proto"
)

var (
	GrainTickInterval   = time.Second * 30
	GoroutinePoolCap    = 0xFFF           //goroutine池容量,大小必须为2的幂次方-1。
	DefaultDeactiveTime = time.Minute * 5 //Grain空闲超过这个时间后执行Deactive
)

type Grain interface {
	Activate(grain.Context) (error, bool) //成功返回nil,否则返回错误以及是否可以重试
	Deactivate() error
}

const (
	grain_un_activate  = 0 //尚未激活
	grain_activated    = 1 //已经激活
	grain_running      = 2
	grain_deactivating = 3
	grain_destroy      = 4
)

type Replyer struct {
	req     *rpc.RequestMsg
	replyed int32
	from    addr.LogicAddr
	node    *clustergo.Node
	hook    func(*rpc.RequestMsg)
}

func (r *Replyer) SetReplyHook(hook func(_ *rpc.RequestMsg)) {
	r.hook = hook
}

func (r *Replyer) redirect(redirectAddr addr.LogicAddr) {
	if r.req.Oneway {
		//通告对端identity不在当前节点
		buff := make([]byte, rpc.LenAddr, len(r.req.To)+rpc.LenAddr)
		binary.BigEndian.PutUint32(buff, uint32(redirectAddr))
		buff = append(buff, []byte(r.req.To)...)
		r.node.SendBinMessage(r.from, rpc.Actor_notify_redirect, buff)
	} else if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &rpc.ResponseMsg{
			Seq:          r.req.Seq,
			ErrCode:      rpc.ErrCodeRedirect,
			RedirectAddr: int(redirectAddr),
		}
		if err := r.node.SendBinMessage(r.from, rpc.Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

func (r *Replyer) error(errCode int) {
	if !r.req.Oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &rpc.ResponseMsg{
			Seq:     r.req.Seq,
			ErrCode: errCode,
		}
		if err := r.node.SendBinMessage(r.from, rpc.Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

func (r *Replyer) callHook() {
	if r.hook == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("%s ", fmt.Errorf(fmt.Sprintf("%v: %s", r, debug.Stack())))
		}
	}()
	r.hook(r.req)
}

func (r *Replyer) Reply(ret proto.Message) {
	if !r.req.Oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		r.callHook()
		resp := &rpc.ResponseMsg{
			Seq: r.req.Seq,
		}

		if b, err := proto.Marshal(ret); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		} else {
			resp.Ret = b
		}

		if err := r.node.SendBinMessage(r.from, rpc.Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

type GrainContext struct {
	mailbox      *Mailbox
	pid          string
	methods      map[uint16]*rpc.MethodCaller
	grain        Grain
	lastRequest  atomic.Value
	silo         *Silo
	state        int
	stoped       bool
	deactiveTime time.Duration
	before       []func(rpc.Replyer, *rpc.RequestMsg) bool //前置管道线
}

func newGrainContext(silo *Silo, pid string, grainType string) *GrainContext {

	grainCfg, ok := silo.grainList[grainType]
	if !ok {
		return nil
	}

	grainCtx := &GrainContext{
		silo:    silo,
		pid:     pid,
		methods: map[uint16]*rpc.MethodCaller{},
		mailbox: NewMailbox(MailboxOption{
			QueueCap:      grainCfg.QueueCap,
			AwaitQueueCap: grainCfg.AwaitQueueCap,
		}),
		deactiveTime: DefaultDeactiveTime,
	}
	grainCtx.lastRequest.Store(time.Now())
	grainCtx.AfterFunc(GrainTickInterval, grainCtx.tick)
	grainCtx.mailbox.Start()
	return grainCtx
}

// 添加前置管道线处理
func (grainCtx *GrainContext) AddCallPipeline(fn func(rpc.Replyer, *rpc.RequestMsg) bool) grain.Context {
	grainCtx.before = append(grainCtx.before, fn)
	return grainCtx
}

func (grainCtx *GrainContext) SetDeactiveTime(deactiveTime time.Duration) {
	grainCtx.deactiveTime = deactiveTime
}

func (grainCtx *GrainContext) Pid() string {
	return grainCtx.pid
}

func (grainCtx *GrainContext) Await(fn interface{}, args ...interface{}) (ret []interface{}) {
	return grainCtx.mailbox.Await(fn, args...)
}

func (grainCtx *GrainContext) RegisterMethod(method uint16, fn interface{}) error {
	if caller, err := rpc.MakeMethodCaller(fn); err != nil {
		return err
	} else {
		grainCtx.methods[method] = caller
		return nil
	}
}

func (grainCtx *GrainContext) deactive(fn func()) {
	if grainCtx.state < grain_deactivating {
		defer func() {
			grainCtx.silo.removeGrain(grainCtx)
			if fn == nil {
				grainCtx.mailbox.Close(false)
			} else {
				go func() {
					grainCtx.mailbox.Close(true)
					fn()
				}()
			}
		}()

		grainCtx.state = grain_deactivating
		if grainCtx.state != grain_un_activate {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			grainCtx.silo.placementDriver.Deactivate(ctx, grainCtx.pid)
		}
	}
}

func (grainCtx *GrainContext) serveCall(ctx context.Context, replyer *Replyer, req *rpc.RequestMsg) {
	call := grainCtx.methods[req.Method]
	if call == nil {
		replyer.error(rpc.ErrCodeMethodNotExist)
		return
	}
	arg := reflect.New(call.ArgType).Interface().(proto.Message)
	err := proto.Unmarshal(req.Arg, arg)
	if err != nil {
		replyer.error(rpc.ErrCodeInvaildArg)
		return
	}
	req.Argument = arg
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("%s ", fmt.Errorf(fmt.Sprintf("%v: %s", r, debug.Stack())))
			replyer.error(rpc.ErrCodeMethodCallPanic)
		}
	}()
	for _, v := range grainCtx.before {
		if !v(replyer, req) {
			return
		}
	}
	call.Fn.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(replyer), reflect.ValueOf(arg)})
}

func (grainCtx *GrainContext) tick() {
	switch grainCtx.state {
	case grain_un_activate, grain_activated, grain_running:
		now := time.Now()
		lastRequest := grainCtx.lastRequest.Load().(time.Time)
		if grainCtx.mailbox.awaitCount.Load() == 0 && now.Sub(lastRequest) > grainCtx.deactiveTime {
			if grainCtx.grain == nil {
				grainCtx.deactive(nil)
			} else if err := grainCtx.grain.Deactivate(); err != nil {
				logger.Errorf("grain:%s userObject.Deactivate() error:%v", grainCtx.pid, err)
				grainCtx.AfterFunc(time.Second, grainCtx.tick)
			} else {
				grainCtx.deactive(nil)
			}
		} else {
			grainCtx.AfterFunc(GrainTickInterval, grainCtx.tick)
		}
	default:
	}
}

func (grain *GrainContext) AfterFunc(d time.Duration, f func()) grain.Timer {
	return grain.mailbox.AfterFunc(d, f)
}

func (grain *GrainContext) NewBarrier() grain.Barrier {
	return &Barrier{
		m:        grain.mailbox,
		waitlist: list.New(),
	}
}

func (grainCtx *GrainContext) onSiloStop(fn func()) {
	grainCtx.stoped = true
	switch grainCtx.state {
	case grain_activated:
		grainCtx.deactive(fn)
	case grain_running:
		//只有当邮箱已经排空并且没有await调用才可以取消激活
		if grainCtx.mailbox.Empty() && grainCtx.mailbox.awaitCount.Load() == 0 {
			var err error
			for i := 0; i < 3; i++ {
				if err = grainCtx.grain.Deactivate(); err != nil {
					time.Sleep(time.Second)
				} else {
					break
				}
			}

			if err != nil {
				logger.Errorf("grain:%s Deactivate() error:%v", grainCtx.pid, err)
			}

			grainCtx.deactive(fn)
		} else {
			//还有异步任务未完成，100毫秒后尝试
			grainCtx.AfterFunc(time.Millisecond*100, func() {
				grainCtx.onSiloStop(fn)
			})
		}
	default:
		fn()
	}
}
