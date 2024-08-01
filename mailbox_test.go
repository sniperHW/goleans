package goleans

import (
	"container/list"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestBenchmarkMailbox(t *testing.T) {
	box := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 64,
	})

	box.Start()
	counter := int32(0)
	go func() {
		for {
			box.Input(func() {
				atomic.AddInt32(&counter, 1)
			})
		}
	}()

	end := time.Now().Add(time.Second * 5)

	for time.Now().Before(end) {
		time.Sleep(time.Second)
		fmt.Println(atomic.LoadInt32(&counter))
		atomic.StoreInt32(&counter, 0)
	}
}

func TestBenchmarkAwait(t *testing.T) {
	box := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 32,
	})

	box.Start()
	counter := int32(0)

	//限定流速，避免同时触发太多goroutine执行Await
	rateLimit := make(chan struct{}, 100)

	end := time.Now().Add(time.Second * 5)

	go func() {
		for i := 0; ; i++ {
			rateLimit <- struct{}{}
			box.Input(func() {
				box.Await(runtime.Gosched)
				atomic.AddInt32(&counter, 1)
				<-rateLimit
			})
		}
	}()

	for time.Now().Before(end) {
		time.Sleep(time.Second)
		fmt.Println(atomic.LoadInt32(&counter))
		atomic.StoreInt32(&counter, 0)
	}
}

/*func BenchmarkChannel(b *testing.B) {
	box := make(chan func(), 64)

	die := make(chan struct{})

	go func() {
		for {
			select {
			case fn := <-box:
				fn()
			case <-die:
				return
			}
		}
	}()

	counter := int32(0)
	go func() {
		for {
			select {
			case box <- func() {
				atomic.AddInt32(&counter, 1)
			}:
			case <-die:
				return
			}
		}
	}()

	for {
		time.Sleep(time.Second)
		fmt.Println(atomic.LoadInt32(&counter))
		atomic.StoreInt32(&counter, 0)
	}
}

func BenchmarkMailbox(b *testing.B) {
	box := NewMailbox(MailboxOption{
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	box.Start()
	counter := int32(0)
	go func() {
		for {
			box.PutNormal(func() {
				atomic.AddInt32(&counter, 1)
			})
		}
	}()

	for {
		time.Sleep(time.Second)
		fmt.Println(atomic.LoadInt32(&counter))
		atomic.StoreInt32(&counter, 0)
	}
}

func BenchmarkAwait(b *testing.B) {
	box := NewMailbox(MailboxOption{
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	box.Start()
	counter := int32(0)

	//限定流速，避免同时触发太多goroutine执行Await
	rateLimit := make(chan struct{}, 100)

	go func() {
		for i := 0; ; i++ {
			rateLimit <- struct{}{}
			box.PutNormal(func() {
				box.Await(runtime.Gosched)
				atomic.AddInt32(&counter, 1)
				<-rateLimit
			})
		}
	}()

	for {
		time.Sleep(time.Second)
		fmt.Println(atomic.LoadInt32(&counter))
		atomic.StoreInt32(&counter, 0)
	}
}*/

func BenchmarkChannel(b *testing.B) {
	box := make(chan func(), 64)
	die := make(chan struct{})
	go func() {
		for {
			select {
			case fn := <-box:
				fn()
			case <-die:
				return
			}
		}
	}()

	ret := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		box <- func() {
			ret <- struct{}{}
		}
		<-ret
	}
	close(die)
}

func BenchmarkMailbox(b *testing.B) {
	box := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 64,
	})

	box.Start()
	ret := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		box.Input(func() {
			ret <- struct{}{}
		})
		<-ret
	}
	box.Close(true)

}

func BenchmarkAwait(b *testing.B) {
	box := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 64,
	})

	box.Start()
	ret := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		box.Input(func() {
			box.Await(runtime.Gosched)
			ret <- struct{}{}
		})
		<-ret
	}
	box.Close(true)
}

func TestMailbox(t *testing.T) {
	{
		box := NewMailbox(MailboxOption{
			QueueCap:      64,
			AwaitQueueCap: 64,
		})

		box.Start()
		for i := 0; i < 64; i++ {
			v := i
			box.Input(func() {
				fmt.Println(v)
				time.Sleep(time.Millisecond * 10)
			})
		}

		box.Close(true)
		fmt.Println("box closed")
	}

	{
		box := NewMailbox(MailboxOption{
			QueueCap:      64,
			AwaitQueueCap: 64,
		})

		box.Start()
		for i := 0; i < 64; i++ {
			v := i
			box.Input(func() {
				fmt.Println(v)
				box.Await(time.Sleep, time.Millisecond*10)
			})
		}

		box.Close(true)
		fmt.Println("box closed")
	}

}

func TestAwait(t *testing.T) {

	box := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 64,
	})

	box.Start()

	box.Input(func() {
		fmt.Println("hello")
		box.Await(func() {
			time.Sleep(time.Millisecond * 10)
		})
	})

	time.Sleep(time.Second)

	box.Close(true)
	fmt.Println("box closed")
}

func TestNornal(t *testing.T) {
	s := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 64,
	})

	c := make(chan struct{})
	go func() {
		for i := 0; i < 64; i++ {
			s.Input(func() {
				fmt.Println("normal")
			})
		}
		fmt.Println(s.InputNoWait(func() {
			fmt.Println("normal nowait")
		}))

		s.Input(func() {
			fmt.Println("normal")
		})
		close(c)
	}()
	time.Sleep(time.Second)
	s.Start()
	<-c
	s.Close(true)

}

func TestBarrier(t *testing.T) {
	s := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 64,
	})

	mtx := &Barrier{
		m:        s,
		waitlist: list.New(),
	}

	s.Input(func() {
		for i := 0; i < 10; i++ {
			mtx.Acquire()
			fmt.Println("a")
			mtx.Release()
			s.Await(time.Sleep, time.Millisecond*100)
		}
	})

	s.Input(func() {
		for i := 0; i < 10; i++ {
			mtx.Acquire()
			fmt.Println("b")
			mtx.Release()
			s.Await(time.Sleep, time.Millisecond*100)
		}
	})

	s.Input(func() {
		for i := 0; i < 10; i++ {
			mtx.Acquire()
			fmt.Println("c")
			mtx.Release()
			s.Await(time.Sleep, time.Millisecond*100)
		}
	})

	s.Start()

	s.Close(true)
}

func TestTimer(t *testing.T) {

	box := NewMailbox(MailboxOption{
		QueueCap:      64,
		AwaitQueueCap: 64,
	})

	box.Start()

	c := make(chan struct{})

	box.AfterFunc(time.Second*2, func() {
		fmt.Println("func1")
		close(c)
	})

	box.AfterFunc(time.Millisecond*500, func() {
		fmt.Println("func2")
	})

	tt := box.AfterFunc(time.Millisecond*500, func() {
		fmt.Println("stoped")
	})

	fmt.Println(tt.Stop())

	tt2 := box.AfterFunc(time.Millisecond*1500, func() {
		fmt.Println("func3")
	})

	<-c

	fmt.Println(tt2.Stop())

	box.Close(true)
	fmt.Println("box closed")
}
