package goleans

import (
	"context"
	"goleans/pd"
	"sync/atomic"
	"time"
)

var (
	GrainTaskQueueCap  = 256 //Grain任务队列大小，队列满时调用Grain.AddTask将会阻塞
	GrainAwakeQueueCap = 64  //Grain Await队列大小，队列满时Await返回时将阻塞
	GrainTickInterval  = time.Second * 30
	GoroutinePoolCap   = 0xFFF           //goroutine池容量,大小必须为2的幂次方-1。
	GrainGCTime        = time.Minute * 5 //Grain空闲超过这个时间后执行Deactive
)

type Grain struct {
	mailbox     *Mailbox
	Identity    string
	version     uint64
	methods     map[uint16]*methodCaller
	userObject  UserObject
	lastRequest atomic.Value
	silo        *Silo
	deactived   int32
}

type UserObject interface {
	Init(*Grain) error
	Deactive() error
}

func newGrain(silo *Silo, identity string, version uint64) *Grain {
	grain := &Grain{
		silo:     silo,
		Identity: identity,
		methods:  map[uint16]*methodCaller{},
		version:  version,
		mailbox: &Mailbox{
			taskQueue:  make(chan func(), GrainTaskQueueCap),
			awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
			die:        make(chan struct{}),
			closeCh:    make(chan struct{}),
		},
	}
	grain.lastRequest.Store(time.Now())
	time.AfterFunc(GrainTickInterval, func() {
		grain.mailbox.PushTask(context.TODO(), grain.tick)
	})
	grain.mailbox.Start()
	return grain
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

func (grain *Grain) RegisterMethod(method uint16, fn interface{}) error {
	if caller, err := makeMethodCaller(fn); err != nil {
		return err
	} else {
		grain.methods[method] = caller
		return nil
	}
}

func (grain *Grain) deactive(fn func()) {
	if atomic.CompareAndSwapInt32(&grain.deactived, 0, 1) { //grain.deactived.CompareAndSwap(false, true) {
		// 空闲超过5分钟，执行deactive
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

			/*
			 *   假如此时，因为pd逻辑错误再次向Silo发送Active,将导致Silo创建新的Grain
			 *   此时，将会使用老的version向pd请求Deactvie,正确的pd实现应该发现version不一致，所以不会实际执行Deactvie,
			 *   但是需要向请求方返回成功，让旧的Grain消亡。
			 */
			err := grain.silo.placementDriver.Deactvie(ctx, pd.Grain{Identity: grain.Identity, Version: atomic.LoadUint64(&grain.version)})
			cancel()
			if err == nil {
				go func() {
					//如果发生了问题1，这里的remove将会失败，此时在pd上，当前silo不拥有grain,但是grain却没能从内存中销毁
					grain.silo.removeGrain(grain)
					grain.mailbox.Close()
					if fn != nil {
						fn()
					}
				}()
				return
			} else {
				logger.Errorf("Deactvie error:%v", err)
			}
		}
	}
}

func (grain *Grain) tick() {
	now := time.Now()
	lastRequest := grain.lastRequest.Load().(time.Time)
	if grain.mailbox.awaitCount == 0 && now.Sub(lastRequest) > GrainGCTime {
		if err := grain.userObject.Deactive(); err != nil {
			logger.Errorf("grain:%s userObject.Deactive() error:%v", grain.Identity, err)
			time.AfterFunc(time.Second*1, func() {
				grain.mailbox.PushTask(context.TODO(), grain.tick)
			})
		} else {
			grain.deactive(nil)
			return
		}
	}

	time.AfterFunc(GrainTickInterval, func() {
		grain.mailbox.PushTask(context.TODO(), grain.tick)
	})
}
