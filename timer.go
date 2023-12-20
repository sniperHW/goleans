package goleans

import (
	"container/heap"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var timerPool = sync.Pool{
	New: func() interface{} { return &Timer{} },
}

type Timer struct {
	deadline time.Time
	idx      int
	fn       func()
	fired    atomic.Bool
}

func (t *Timer) Stop() bool {
	if t.fired.CompareAndSwap(false, true) {
		return true
	} else {
		return false
	}
}

func (t *Timer) call() {
	if t.fired.CompareAndSwap(false, true) {
		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("%s ", fmt.Errorf(fmt.Sprintf("%v: %s", r, debug.Stack())))
			}
		}()
		t.fn()
	}
}

type timedHeap struct {
	timers     []*Timer
	timer      *time.Timer
	checkTimer bool
}

func (h timedHeap) Len() int {
	return len(h.timers)
}
func (h timedHeap) Less(i, j int) bool {
	return h.timers[i].deadline.Before(h.timers[j].deadline)
}
func (h timedHeap) Swap(i, j int) {
	h.timers[i], h.timers[j] = h.timers[j], h.timers[i]
	h.timers[i].idx = i
	h.timers[j].idx = j
}

func (h *timedHeap) Push(x interface{}) {
	h.timers = append(h.timers, x.(*Timer))
	n := len(h.timers)
	h.timers[n-1].idx = n - 1
}

func (h *timedHeap) Pop() interface{} {
	old := h.timers
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.timers = old[0 : n-1]
	return x
}

func (m *Mailbox) AfterFunc(d time.Duration, fn func()) *Timer {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return nil
	}

	t := timerPool.Get().(*Timer)
	t.deadline = time.Now().Add(d)
	t.fn = fn
	t.fired.Store(false)

	heap.Push(&m.timedHeap, t)
	if !m.checkTimer && t == m.timers[0] {
		//新定时器是时间最近的定时器
		if m.timer == nil {
			m.timer = time.AfterFunc(d, func() {
				m.mtx.Lock()
				if !m.checkTimer {
					m.checkTimer = true
					m.signalAndUnlock()
				} else {
					m.mtx.Unlock()
				}
			})
		} else {
			if !m.timer.Stop() {
				<-m.timer.C
			}
			m.timer.Reset(d)
		}
	}
	return t
}

func (m *Mailbox) doTimer() {
	if m.checkTimer {
		now := time.Now()
		for len(m.timers) > 0 {
			near := m.timers[0]
			if now.After(near.deadline) {
				heap.Pop(&m.timedHeap)
				m.mtx.Unlock()
				near.call()
				m.mtx.Lock()
				timerPool.Put(near)
			} else {
				break
			}
		}
		if len(m.timers) > 0 {
			m.timer.Reset(time.Until(m.timers[0].deadline))
		}
		m.checkTimer = false
	}
}
