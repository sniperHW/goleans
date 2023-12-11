package goleans

import (
	"container/list"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

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
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	box.Start()
	ret := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		box.PutNormal(func() {
			ret <- struct{}{}
		})
		<-ret
	}
	box.Close(true)

}

func BenchmarkAwait(b *testing.B) {
	box := NewMailbox(MailboxOption{
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	box.Start()
	ret := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		box.PutNormal(func() {
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
			UrgentQueueCap: 64,
			NormalQueueCap: 64,
			AwaitQueueCap:  64,
		})

		box.Start()
		for i := 0; i < 64; i++ {
			v := i
			box.PutNormal(func() {
				fmt.Println(v)
				time.Sleep(time.Millisecond * 10)
			})
		}

		box.Close(true)
		fmt.Println("box closed")
	}

	{
		box := NewMailbox(MailboxOption{
			UrgentQueueCap: 64,
			NormalQueueCap: 64,
			AwaitQueueCap:  64,
		})

		box.Start()
		for i := 0; i < 64; i++ {
			v := i
			box.PutNormal(func() {
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
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	box.Start()

	box.PutUrgent(func() {
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
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	c := make(chan struct{})
	go func() {
		for i := 0; i < 64; i++ {
			s.PutNormal(func() {
				fmt.Println("normal")
			})
		}
		fmt.Println(s.PutNormalNoWait(func() {
			fmt.Println("normal nowait")
		}))

		s.PutNormal(func() {
			fmt.Println("normal")
		})
		close(c)
	}()
	time.Sleep(time.Second)
	s.Start()
	<-c
	s.Close(true)

}

func TestUrgent(t *testing.T) {
	s := NewMailbox(MailboxOption{
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	urgent := int32(0)

	s.PutNormal(func() {
		fmt.Println("normal")
		if atomic.LoadInt32(&urgent) != 65 {
			panic("panic")
		}
	})

	c := make(chan struct{})
	go func() {
		for i := 0; i < 64; i++ {
			s.PutUrgent(func() {
				fmt.Println("urgent", atomic.AddInt32(&urgent, 1))
			})
		}
		//fmt.Println(s.PutUrgentNoWait(func() {
		//	fmt.Println("urgent nowait")
		//}))
		s.PutUrgent(func() {
			fmt.Println("urgent", atomic.AddInt32(&urgent, 1))
		})

		close(c)
	}()

	time.Sleep(time.Second)
	s.Start()
	<-c
	s.Close(true)
}

func TestLock(t *testing.T) {
	s := NewMailbox(MailboxOption{
		UrgentQueueCap: 64,
		NormalQueueCap: 64,
		AwaitQueueCap:  64,
	})

	mtx := &Mutex{
		m:        s,
		waitlist: list.New(),
	}

	s.PutNormal(func() {
		for i := 0; i < 10; i++ {
			mtx.Lock()
			fmt.Println("a")
			mtx.Unlock()
			s.Await(time.Sleep, time.Millisecond*100)
		}
	})

	s.PutNormal(func() {
		for i := 0; i < 10; i++ {
			mtx.Lock()
			fmt.Println("b")
			mtx.Unlock()
			s.Await(time.Sleep, time.Millisecond*100)
		}
	})

	s.PutNormal(func() {
		for i := 0; i < 10; i++ {
			mtx.Lock()
			fmt.Println("c")
			mtx.Unlock()
			s.Await(time.Sleep, time.Millisecond*100)
		}
	})

	s.Start()

	s.Close(true)
}
