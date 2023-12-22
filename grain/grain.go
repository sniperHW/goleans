package grain

import (
	"time"

	"github.com/sniperHW/goleans/rpc"
)

type Barrier interface {
	Acquire()
	Release()
}

type Timer interface {
	Stop() bool
}

type Context interface {
	AddCallPipeline(func(rpc.Replyer, *rpc.RequestMsg) bool) Context
	SetDeactiveTime(time.Duration)
	Pid() string
	Await(fn interface{}, args ...interface{}) (ret []interface{})
	RegisterMethod(method uint16, fn interface{}) error
	AfterFunc(d time.Duration, f func()) Timer
	NewBarrier() Barrier
}
