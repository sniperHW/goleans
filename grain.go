package goleans

import (
	"context"
	"sync/atomic"
	"time"
)

type Grain struct {
	mailbox     *Mailbox
	Identity    string
	methods     map[uint16]*methodCaller
	userObject  UserObject
	lastRequest atomic.Value
	silo        *Silo
	deactive    bool
}

type UserObject interface {
	Init(*Grain) error
	Deactive() error
}

func newGrain(silo *Silo, identity string) *Grain {
	grain := &Grain{
		silo:     silo,
		Identity: identity,
		methods:  map[uint16]*methodCaller{},
		mailbox: &Mailbox{
			taskQueue:  make(chan func(), 256),
			awakeQueue: make(chan *goroutine, 64),
			die:        make(chan struct{}),
			closeCh:    make(chan struct{}),
		},
	}
	grain.lastRequest.Store(time.Now())
	time.AfterFunc(time.Second*30, func() {
		grain.mailbox.PushTask(context.TODO(), grain.tick)
	})
	grain.Start()
	return grain
}

func (grain *Grain) Start() {
	grain.mailbox.Start()
}

func (grain *Grain) AddTask(ctx context.Context, task func()) error {
	return grain.mailbox.PushTask(ctx, task)
}

func (grain *Grain) Await(fn interface{}, args ...interface{}) (ret []interface{}) {
	return grain.mailbox.Await(fn, args...)
}

func (grain *Grain) GetIdentity() string {
	return grain.Identity
}

func (grain *Grain) GetMethod(method uint16) *methodCaller {
	return grain.methods[method]
}

func (grain *Grain) RegisterMethod(method uint16, fn interface{}) error {
	if caller, err := makeMethodCaller(fn); err != nil {
		return err
	} else {
		grain.methods[method] = caller
		return nil
	}
}

func (grain *Grain) tick() {
	now := time.Now()
	lastRequest := grain.lastRequest.Load().(time.Time)
	if grain.mailbox.awaitCount == 0 && now.Sub(lastRequest) > time.Minute*5 {
		if err := grain.userObject.Deactive(); err != nil {
			logger.Errorf("grain:%s userObject.Deactive() error:%v", grain.Identity, err)
			time.AfterFunc(time.Second*1, func() {
				grain.mailbox.PushTask(context.TODO(), grain.tick)
			})
		} else {
			//空闲超过5分钟，执行deactive
			for {
				/*
				 * 只会返回超时错误，为了避免发生Grain在两个Silo被激活的情况发生，这里必须等到pd返回执行成功
				 *
				 * 考虑一下情形:
				 * Silo:A向pd发送Grain:1 Deactvie，Silo成功执行，之后Silo:A跟pd发生网络分区，Silo:A无法收到pd的响应。
				 * 最终Silo:A将超时，如果此时退出Deactvie,等待下一次tick再尝试，那么在这个时间段内，pd再次收到Grain:1的请求,
				 * pd选择在Silo:B激活Grain:1,此时，Grain:1同时在Silo:B,Silo:A存在。
				 */
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				err := grain.silo.placementDriver.Deactvie(ctx, grain.Identity)
				cancel()
				if err == nil {
					grain.deactive = true
					grain.silo.Lock()
					delete(grain.silo.grains, grain.Identity)
					grain.silo.Unlock()
					break
				} else {
					logger.Errorf("Deactvie error:%v", err)
				}
			}

			go func() {
				grain.mailbox.Close()
			}()
			return
		}
	}

	time.AfterFunc(time.Second*30, func() {
		grain.mailbox.PushTask(context.TODO(), grain.tick)
	})
}
