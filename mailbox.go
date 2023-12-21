package goleans

import (
	"container/list"
	"errors"
	"reflect"
	"runtime"
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

type ringqueue[T any] struct {
	head      int
	tail      int
	queue     []T
	cond      *sync.Cond
	locker    sync.Locker
	waitCount int
}

func newRingQueue[T any](cap int, l sync.Locker) *ringqueue[T] {
	if cap <= 0 {
		cap = 1
	}
	ring := &ringqueue[T]{
		queue:  make([]T, cap+1),
		cond:   sync.NewCond(l),
		locker: l,
	}
	return ring
}

func (ring *ringqueue[T]) empty() bool {
	return ring.head == ring.tail
}

func (ring *ringqueue[T]) full() bool {
	return (ring.tail+1)%len(ring.queue) == ring.head
}

func (ring *ringqueue[T]) put(v T) {
	ring.queue[ring.tail] = v
	ring.tail = (ring.tail + 1) % len(ring.queue)
}

func (ring *ringqueue[T]) pop() T {
	v := ring.queue[ring.head]
	var zero T
	ring.queue[ring.head] = zero
	ring.head = (ring.head + 1) % len(ring.queue)
	return v
}

func (ring *ringqueue[T]) wait() {
	ring.waitCount++
	ring.cond.Wait()
}

func (ring *ringqueue[T]) signalAndUnlock() {
	if ring.waitCount > 0 {
		ring.waitCount--
		ring.locker.Unlock()
		ring.cond.Signal()
	} else {
		ring.locker.Unlock()
	}
}

func (ring *ringqueue[T]) broadcast() {
	ring.cond.Broadcast()
}

type Mailbox struct {
	timedHeap
	mtx                sync.Mutex
	current            *goroutine
	startOnce          sync.Once
	closed             bool
	awaitCount         atomic.Int32
	continueTaskLimits int //>0，连续执行continueTaskLimits个任务后让出时间片，避免其他actor饥饿。
	closeCh            chan struct{}
	waiting            bool
	suspended          bool
	cond               *sync.Cond
	awaitQueue         *ringqueue[*goroutine]
	urgentQueue        *ringqueue[func()]
	normalQueue        *ringqueue[func()]
}

type MailboxOption struct {
	UrgentQueueCap     int
	NormalQueueCap     int
	AwaitQueueCap      int
	ContinueTaskLimits int
}

func NewMailbox(opt MailboxOption) *Mailbox {
	m := &Mailbox{
		closeCh: make(chan struct{}),
	}
	m.awaitQueue = newRingQueue[*goroutine](opt.AwaitQueueCap, &m.mtx)
	m.urgentQueue = newRingQueue[func()](opt.UrgentQueueCap, &m.mtx)
	m.normalQueue = newRingQueue[func()](opt.NormalQueueCap, &m.mtx)
	m.cond = sync.NewCond(&m.mtx)
	m.continueTaskLimits = opt.ContinueTaskLimits
	return m
}

func (m *Mailbox) signalAndUnlock() {
	if m.waiting {
		m.waiting = false
		m.mtx.Unlock()
		m.cond.Signal()
	} else {
		m.mtx.Unlock()
	}
}

func (m *Mailbox) wait() {
	m.waiting = true
	m.cond.Wait()
}

func (m *Mailbox) putAwait(g *goroutine) {
	m.mtx.Lock()
	for m.awaitQueue.full() {
		m.awaitQueue.wait()
	}
	m.awaitQueue.put(g)
	m.signalAndUnlock()
}

func (m *Mailbox) Input(fn func()) error {
	m.mtx.Lock()
	if m.closed {
		m.mtx.Unlock()
		return errors.New("mailbox closed")
	}
	for m.normalQueue.full() {
		m.normalQueue.wait()
		if m.closed {
			m.mtx.Unlock()
			return errors.New("mailbox closed")
		}
	}
	m.normalQueue.put(fn)
	m.signalAndUnlock()
	return nil
}

func (m *Mailbox) InputNoWait(fn func()) error {
	m.mtx.Lock()
	if m.closed {
		m.mtx.Unlock()
		return errors.New("mailbox closed")
	} else if m.normalQueue.full() {
		m.mtx.Unlock()
		return errors.New("full")
	}
	m.normalQueue.put(fn)
	m.signalAndUnlock()
	return nil
}

func (m *Mailbox) InputUrgent(fn func()) error {
	m.mtx.Lock()
	if m.closed {
		m.mtx.Unlock()
		return errors.New("mailbox closed")
	}
	for m.urgentQueue.full() {
		m.urgentQueue.wait()
		if m.closed {
			m.mtx.Unlock()
			return errors.New("mailbox closed")
		}
	}
	m.urgentQueue.put(fn)
	m.signalAndUnlock()
	return nil
}

var mask int = GoroutinePoolCap

type goroutine_pool struct {
	sync.Mutex
	head int
	tail int
	pool []*goroutine
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

var gotine_pool goroutine_pool = goroutine_pool{
	pool: make([]*goroutine, mask+1),
}

func (co *goroutine) loop(m *Mailbox) {
	c := 0
	for {
		if m.continueTaskLimits > 0 && c > m.continueTaskLimits {
			c = 0
			runtime.Gosched()
		}
		c++
		m.mtx.Lock()
		for {

			if m.checkTimer {
				m.doTimer()
			}

			if !m.awaitQueue.empty() {
				gotine := m.awaitQueue.pop()
				m.awaitQueue.signalAndUnlock()
				m.awaitCount.Add(-1)
				//1
				gotine.resume(m)
				//actor的执行将由Await.1继续，当前goroutine退出循环
				return
			} else if !m.urgentQueue.empty() {
				fn := m.urgentQueue.pop()
				m.urgentQueue.signalAndUnlock()
				fn()
				break
			} else if m.suspended {
				if m.closed && m.awaitCount.Load() == 0 && m.normalQueue.empty() {
					m.mtx.Unlock()
					close(m.closeCh)
					return
				} else {
					m.wait()
					c = 0
				}
			} else if !m.normalQueue.empty() {
				fn := m.normalQueue.pop()
				m.normalQueue.signalAndUnlock()
				fn()
				break
			} else {
				if m.closed && m.awaitCount.Load() == 0 {
					m.mtx.Unlock()
					close(m.closeCh)
					return
				}
				m.wait()
				c = 0
			}
			/* else if !m.normalQueue.empty() {
				fn := m.normalQueue.pop()
				m.normalQueue.signalAndUnlock()
				fn()
				break
			} else {
				if m.closed && m.awaitCount.Load() == 0 {
					m.mtx.Unlock()
					close(m.closeCh)
					return
				}
				m.wait()
				c = 0
			}*/
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

func (m *Mailbox) Empty() bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.urgentQueue.empty() && m.normalQueue.empty()
}

func (m *Mailbox) Suspend() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.suspended = true
}

func (m *Mailbox) Resume() {
	m.mtx.Lock()
	m.suspended = false
	m.signalAndUnlock()
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
	m.awaitCount.Add(1)
	current := m.current
	//调度另一个goroutine去执行m的任务队列
	m.sche()
	//在这个点,fn跟m的任务队列并发执行
	ret = call(fn, args...)
	//将自己添加到待唤醒通道中
	m.putAwait(current)
	//等待被唤醒后继续执行
	//loop.1.resume之后从yield返回
	current.yield()
	//1
	return ret
}

func (m *Mailbox) Close(wait bool) {
	m.mtx.Lock()
	if !m.closed {
		m.closed = true
		m.normalQueue.broadcast()
		m.urgentQueue.broadcast()
		m.signalAndUnlock()
	} else {
		m.mtx.Unlock()
	}

	if wait {
		<-m.closeCh
	}
}

type Mutex struct {
	m        *Mailbox
	owner    *goroutine
	waitlist *list.List
}

func (mtx *Mutex) Lock() {
	current := mtx.m.current
	if mtx.owner == nil {
		mtx.owner = current
	} else {
		if mtx.owner == current {
			panic("Lock error")
		}
		mtx.waitlist.PushBack(current)
		mtx.m.awaitCount.Add(1)
		mtx.m.sche()
		//等待唤醒
		current.yield()
		//1
	}
}

func (mtx *Mutex) Unlock() {
	if mtx.owner != mtx.m.current {
		panic("Unlock error")
	} else {
		mtx.owner = nil
		front := mtx.waitlist.Front()
		if front != nil {
			co := mtx.waitlist.Remove(front).(*goroutine)
			mtx.owner = co
			mtx.m.putAwait(co)
		}
	}
}
