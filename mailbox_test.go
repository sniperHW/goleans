package goleans

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

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
	box := &Mailbox{
		taskQueue:  make(chan func(), 32),
		awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}

	box.Start()
	ret := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		box.PushTask(context.Background(), func() {
			ret <- struct{}{}
		})
		<-ret
	}
	box.Close(true)

}

func BenchmarkAwait(b *testing.B) {
	box := &Mailbox{
		taskQueue:  make(chan func(), 32),
		awakeQueue: make(chan *goroutine, GrainAwakeQueueCap),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}

	box.Start()
	ret := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		box.PushTask(context.Background(), func() {
			box.Await(runtime.Gosched)
			ret <- struct{}{}
		})
		<-ret
	}
	box.Close(true)
}

func TestMailbox(t *testing.T) {
	{
		box := &Mailbox{
			taskQueue:  make(chan func(), 32),
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
			taskQueue:  make(chan func(), 32),
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
		taskQueue:  make(chan func(), 32),
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
