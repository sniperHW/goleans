package goleans

import (
	"context"
	"goleans/pd"
	"sync/atomic"
	"time"
)

var (
	GrainMailboxCap    = 256 //Grain任务队列大小，队列满时调用Grain.AddTask将会阻塞
	GrainAwakeQueueCap = 64  //Grain Await队列大小，队列满时Await返回时将阻塞
	GrainTickInterval  = time.Second * 30
	GoroutinePoolCap   = 0xFFF           //goroutine池容量,大小必须为2的幂次方-1。
	GrainGCTime        = time.Minute * 5 //Grain空闲超过这个时间后执行Deactive
)

/*
 *  对不可重试的错误，Init必须返回ErrInitUnRetryAbleError。
 *  例如Account对象，它代表玩家的账号，玩家首先要创建账号之后才能接收请求，对于一个不存在的Account对象，当它
 *  接收到请求被创建出来，执行Init。此时需要从数据库读取用户数据执行初始化，因为Account没有创建过，所以在数据库中不存在相关记录
 *  此时应该返回ErrInitUnRetryAbleError通知框架层执行正确的逻辑（某些对象的行为可能是不存在数据库记录就插入一条记录）
 */
type UserObject interface {
	Init(*Grain) error
	Deactivate() error
}

const (
	grain_un_activate  = 0 //尚未激活
	grain_activated    = 1 //已经激活
	grain_running      = 2
	grain_deactivating = 3
	grain_destroy      = 4
)

type Grain struct {
	mailbox     *Mailbox
	Identity    pd.GrainIdentity
	methods     map[uint16]*methodCaller
	userObject  UserObject
	lastRequest atomic.Value
	silo        *Silo
	state       int
	stoped      bool
}

func newGrain(silo *Silo, identity pd.GrainIdentity, grainType string) *Grain {

	grainCfg, ok := silo.grainList[grainType]
	if !ok {
		return nil
	}

	grain := &Grain{
		silo:     silo,
		Identity: identity,
		methods:  map[uint16]*methodCaller{},
		mailbox: &Mailbox{
			taskQueue:  make(chan func(), grainCfg.MailboxCap),
			awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
			die:        make(chan struct{}),
			closeCh:    make(chan struct{}),
		},
	}
	grain.lastRequest.Store(time.Now())
	grain.AfterFunc(GrainTickInterval, grain.tick)
	grain.mailbox.Start()
	return grain
}

func (grain *Grain) GetIdentity() pd.GrainIdentity {
	return grain.Identity
}

func (grain *Grain) AddTask(ctx context.Context, task func()) error {
	return grain.mailbox.PushTask(ctx, task)
}

func (grain *Grain) AddTaskNoWait(task func()) error {
	return grain.mailbox.PushTaskNoWait(task)
}

func (grain *Grain) Await(fn interface{}, args ...interface{}) (ret []interface{}) {
	return grain.mailbox.Await(fn, args...)
}

func (grain *Grain) RegisterMethod(method uint16, fn interface{}) error {
	if caller, err := makeMethodCaller(fn); err != nil {
		return err
	} else {
		grain.methods[method] = caller
		return nil
	}
}

func (grain *Grain) deactive(fn func()) {
	if grain.state < grain_deactivating {
		defer func() {
			grain.silo.removeGrain(grain)
			if fn == nil {
				grain.mailbox.Close(false)
			} else {
				go func() {
					grain.mailbox.Close(true)
					fn()
				}()
			}
		}()

		grain.state = grain_deactivating
		if grain.state != grain_un_activate {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			grain.silo.placementDriver.Deactivate(ctx, grain.Identity)
		}
	}
}

func (grain *Grain) tick() {
	switch grain.state {
	case grain_un_activate, grain_activated, grain_running:
		now := time.Now()
		lastRequest := grain.lastRequest.Load().(time.Time)
		if grain.mailbox.awaitCount == 0 && now.Sub(lastRequest) > GrainGCTime {
			if grain.userObject == nil {
				grain.deactive(nil)
			} else if err := grain.userObject.Deactivate(); err != nil {
				logger.Errorf("grain:%s userObject.Deactivate() error:%v", grain.Identity, err)
				grain.AfterFunc(time.Second, grain.tick)
			} else {
				grain.deactive(nil)
			}
		} else {
			grain.AfterFunc(GrainTickInterval, grain.tick)
		}
	default:
	}
}

func (grain *Grain) AfterFunc(d time.Duration, f func()) {
	time.AfterFunc(d, func() {
		grain.mailbox.PushTask(context.TODO(), f)
	})
}

func (grain *Grain) onSiloStop(fn func()) {
	grain.stoped = true
	switch grain.state {
	case grain_activated:
		grain.deactive(fn)
	case grain_running:
		if grain.mailbox.awaitCount == 0 {
			var err error
			for i := 0; i < 3; i++ {
				if err = grain.userObject.Deactivate(); err != nil {
					time.Sleep(time.Second)
				} else {
					break
				}
			}

			if err != nil {
				logger.Errorf("grain:%s userObject.Deactivate() error:%v", grain.Identity, err)
			}

			grain.deactive(fn)
		} else {
			//还有异步任务未完成，100毫秒后尝试
			grain.AfterFunc(time.Millisecond*100, func() {
				grain.onSiloStop(fn)
			})
		}
	default:
		fn()
	}
}
