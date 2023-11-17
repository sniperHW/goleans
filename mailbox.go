package goleans

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
)

var nextID int32

type goroutine struct {
	id     int32
	signal chan *Mailbox
}

func (co *goroutine) yield() *Mailbox {
	m := <-co.signal
	m.current = co
	return m
}

func (co *goroutine) resume(m *Mailbox) {
	co.signal <- m
}

type Mailbox struct {
	taskQueue  chan func()
	awakeQueue chan *goroutine
	current    *goroutine
	die        chan struct{}
	startOnce  sync.Once
	closed     int32
	awaitCount int32
	closeCh    chan struct{}
}

type MailboxOption struct {
	TaskQueueCap int //任务队列容量
}

func (m *Mailbox) PushTask(ctx context.Context, fn func()) error {
	if m.closed == 1 {
		return errors.New("mailbox closed")
	} else {
		select {
		case m.taskQueue <- fn:
			return nil
		case <-m.die:
			return errors.New("mailbox closed")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

const mask int = 0xFFF

type goroutine_pool struct {
	sync.Mutex
	head int
	tail int
	pool [mask + 1]*goroutine
}

func (p *goroutine_pool) put(g *goroutine) bool {
	var ok bool
	p.Lock()
	if (p.tail+1)&mask != p.head {
		p.pool[p.tail] = g
		p.tail = (p.tail + 1) & mask
		ok = true
	}
	p.Unlock()
	return ok
}

func (p *goroutine_pool) get() (g *goroutine) {
	p.Lock()
	if p.head != p.tail {
		g = p.pool[p.head]
		p.head = (p.head + 1) & mask
	}
	p.Unlock()
	return g
}

var gotine_pool goroutine_pool = goroutine_pool{}

func (m *Mailbox) onDie(co *goroutine) {
	//处理taskQueue中剩余任务，等待awaitCount变成0
	for atomic.LoadInt32(&m.awaitCount) > 0 || len(m.taskQueue) > 0 {
		select {
		case gotine := <-m.awakeQueue:
			atomic.AddInt32(&m.awaitCount, -1)
			gotine.resume(m)
			return
		case fn := <-m.taskQueue:
			fn()
		}
	}
	close(m.closeCh)
}

func (co *goroutine) loop(m *Mailbox) {
	for {
		if atomic.LoadInt32(&m.awaitCount) > 0 {
			select {
			case gotine := <-m.awakeQueue:
				atomic.AddInt32(&m.awaitCount, -1)
				gotine.resume(m)
				return
			case fn := <-m.taskQueue:
				fn()
			case <-m.die:
				m.onDie(co)
				return
			}
		} else {
			select {
			case fn := <-m.taskQueue:
				fn()
			case <-m.die:
				m.onDie(co)
				return
			}
		}
	}
}

func (m *Mailbox) sche() {
	gotine := gotine_pool.get()
	if gotine == nil {
		gotine = &goroutine{
			signal: make(chan *Mailbox),
			id:     atomic.AddInt32(&nextID, 1),
		}
		go func() {
			for m := gotine.yield(); ; m = gotine.yield() {
				gotine.loop(m)
				if !gotine_pool.put(gotine) {
					return
				}
			}
		}()
	}
	gotine.resume(m)
}

func (m *Mailbox) Start() {
	m.startOnce.Do(m.sche)
}

func call(fn interface{}, args ...interface{}) (result []interface{}) {
	fnType := reflect.TypeOf(fn)
	fnValue := reflect.ValueOf(fn)
	numIn := fnType.NumIn()

	var out []reflect.Value
	if numIn == 0 {
		out = fnValue.Call(nil)
	} else {
		argsLength := len(args)
		argumentIn := numIn
		if fnType.IsVariadic() {
			argumentIn--
		}

		if argsLength < argumentIn {
			panic("with too few input arguments")
		}

		if !fnType.IsVariadic() && argsLength > argumentIn {
			panic("with too many input arguments")
		}

		in := make([]reflect.Value, numIn)
		for i := 0; i < argumentIn; i++ {
			if args[i] == nil {
				in[i] = reflect.Zero(fnType.In(i))
			} else {
				in[i] = reflect.ValueOf(args[i])
			}
		}

		if fnType.IsVariadic() {
			m := argsLength - argumentIn
			slice := reflect.MakeSlice(fnType.In(numIn-1), m, m)
			in[numIn-1] = slice
			for i := 0; i < m; i++ {
				x := args[argumentIn+i]
				if x != nil {
					slice.Index(i).Set(reflect.ValueOf(x))
				}
			}
			out = fnValue.CallSlice(in)
		} else {
			out = fnValue.Call(in)
		}
	}

	if len(out) > 0 {
		result = make([]interface{}, len(out))
		for i, v := range out {
			result[i] = v.Interface()
		}
	}
	return result
}

// fn将与m的任务队列并行执行，因此fn中不能访问线程不安全的数据
func (m *Mailbox) Await(fn interface{}, args ...interface{}) (ret []interface{}) {
	atomic.AddInt32(&m.awaitCount, 1)
	current := m.current
	//调度另一个goroutine去执行m的任务队列
	m.sche()
	//在这个点,fn跟m的任务队列并发执行
	ret = call(fn, args...)
	//将自己添加到待唤醒通道中
	m.awakeQueue <- current
	//等待被唤醒后继续执行
	current.yield()
	return ret
}

func (m *Mailbox) Close() {
	if atomic.CompareAndSwapInt32(&m.closed, 0, 1) {
		close(m.die)
	}
	<-m.closeCh
}
