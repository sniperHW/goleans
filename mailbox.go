package goleans

import (
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

type ringqueue[T any] struct {
	head      int
	tail      int
	queue     []T
	cond      *sync.Cond
	waitCount int
}

func newRingQueue[T any](cap int, l sync.Locker) *ringqueue[T] {
	if cap <= 0 {
		cap = 1
	}
	ring := &ringqueue[T]{
		queue: make([]T, cap+1),
		cond:  sync.NewCond(l),
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
	ring.head = (ring.head + 1) % len(ring.queue)
	return v
}

func (ring *ringqueue[T]) wait() {
	ring.waitCount++
	ring.cond.Wait()
	ring.waitCount--
}

func (ring *ringqueue[T]) signal() {
	if ring.waitCount > 0 {
		ring.cond.Signal()
	}
}

func (ring *ringqueue[T]) broadcast() {
	if ring.waitCount > 0 {
		ring.cond.Broadcast()
	}
}

type Mailbox struct {
	mtx         sync.Mutex
	current     *goroutine
	startOnce   sync.Once
	closed      bool
	awaitCount  int32
	closeCh     chan struct{}
	waitCount   int
	cond        *sync.Cond
	awaitQueue  *ringqueue[*goroutine]
	urgentQueue *ringqueue[func()]
	normalQueue *ringqueue[func()]
}

type MailboxOption struct {
	UrgentQueueCap int
	NormalQueueCap int
	AwaitQueueCap  int
}

func NewMailbox(opt MailboxOption) *Mailbox {
	m := &Mailbox{
		closeCh: make(chan struct{}),
	}
	m.awaitQueue = newRingQueue[*goroutine](opt.AwaitQueueCap, &m.mtx)
	m.urgentQueue = newRingQueue[func()](opt.UrgentQueueCap, &m.mtx)
	m.normalQueue = newRingQueue[func()](opt.AwaitQueueCap, &m.mtx)
	m.cond = sync.NewCond(&m.mtx)
	return m
}

func (m *Mailbox) signal() {
	if m.waitCount > 0 {
		m.cond.Signal()
	}
}

func (m *Mailbox) wait() {
	m.waitCount++
	m.cond.Wait()
	m.waitCount--
}

func (m *Mailbox) putAwait(g *goroutine) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for m.awaitQueue.full() {
		m.awaitQueue.wait()
	}
	m.awaitQueue.put(g)
	m.signal()
}

func (m *Mailbox) PutNormal(fn func()) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return errors.New("mailbox closed")
	}
	for m.normalQueue.full() {
		m.normalQueue.wait()
		if m.closed {
			return errors.New("mailbox closed")
		}
	}
	m.normalQueue.put(fn)
	m.signal()
	return nil
}

func (m *Mailbox) PutNormalNoWait(fn func()) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return errors.New("mailbox closed")
	} else if m.normalQueue.full() {
		return errors.New("full")
	}
	m.normalQueue.put(fn)
	m.signal()
	return nil
}

func (m *Mailbox) PutUrgent(fn func()) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return errors.New("mailbox closed")
	}
	for m.urgentQueue.full() {
		m.urgentQueue.wait()
		if m.closed {
			return errors.New("mailbox closed")
		}
	}
	m.urgentQueue.put(fn)
	m.signal()
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

	for {
		m.mtx.Lock()
		for {
			if !m.awaitQueue.empty() {
				gotine := m.awaitQueue.pop()
				m.awaitQueue.signal()
				m.mtx.Unlock()
				atomic.AddInt32(&m.awaitCount, -1)
				gotine.resume(m)
				return
			} else if !m.urgentQueue.empty() {
				fn := m.urgentQueue.pop()
				m.urgentQueue.signal()
				m.mtx.Unlock()
				fn()
				break
			} else if !m.normalQueue.empty() {
				fn := m.normalQueue.pop()
				m.normalQueue.signal()
				m.mtx.Unlock()
				fn()
				break
			} else {
				if m.closed && atomic.LoadInt32(&m.awaitCount) == 0 {
					m.mtx.Unlock()
					close(m.closeCh)
					return
				}
				m.wait()
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

func (m *Mailbox) Empty() bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.urgentQueue.empty() && m.normalQueue.empty()
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
	m.putAwait(current)
	//等待被唤醒后继续执行
	current.yield()
	return ret
}

func (m *Mailbox) Close(wait bool) {
	m.mtx.Lock()
	if !m.closed {
		m.closed = true
		m.normalQueue.broadcast()
		m.urgentQueue.broadcast()
		m.signal()
	}
	m.mtx.Unlock()

	if wait {
		<-m.closeCh
	}
}
