package goleans

import (
	"context"
	"fmt"
	"testing"
	"time"
)

/*
func TestBenchmarkChannel(t *testing.T) {
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

func TestBenchmarkMailbox(t *testing.T) {
	box := &Mailbox{
		taskQueue:  make(chan func(), GrainTaskQueueCap),
		awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}

	box.Start()
	counter := int32(0)
	go func() {
		for {
			box.PushTask(context.Background(), func() {
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

func TestBenchmarkAwait(t *testing.T) {
	box := &Mailbox{
		taskQueue:  make(chan func(), GrainTaskQueueCap),
		awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}

	box.Start()
	counter := int32(0)

	//限定流速，避免同时触发太多goroutine执行Await
	rateLimit := make(chan struct{}, 100)

	go func() {
		for i := 0; ; i++ {
			rateLimit <- struct{}{}
			box.PushTask(context.Background(), func() {
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

func TestMailbox(t *testing.T) {
	{
		box := &Mailbox{
			taskQueue:  make(chan func(), GrainTaskQueueCap),
			awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
			die:        make(chan struct{}),
			closeCh:    make(chan struct{}),
		}

		box.Start()
		for i := 0; i < 64; i++ {
			v := i
			box.PushTask(context.Background(), func() {
				fmt.Println(v)
				time.Sleep(time.Millisecond * 10)
			})
		}

		box.Close(true)
		fmt.Println("box closed")
	}

	{
		box := &Mailbox{
			taskQueue:  make(chan func(), GrainTaskQueueCap),
			awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
			die:        make(chan struct{}),
			closeCh:    make(chan struct{}),
		}

		box.Start()
		for i := 0; i < 64; i++ {
			v := i
			box.PushTask(context.Background(), func() {
				fmt.Println(v)
				box.Await(time.Sleep, time.Millisecond*10)
			})
		}

		box.Close(true)
		fmt.Println("box closed")
	}

}

func TestAwait(t *testing.T) {

	box := &Mailbox{
		taskQueue:  make(chan func(), GrainTaskQueueCap),
		awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}

	box.Start()

	box.PushTask(context.Background(), func() {
		fmt.Println("hello")
		box.Await(func() {
			time.Sleep(time.Millisecond * 10)
		})
	})

	time.Sleep(time.Second)

	box.Close(true)
	fmt.Println("box closed")
}
