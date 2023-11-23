package pd

import (
	"context"

	"github.com/sniperHW/clustergo/addr"
)

type Metric struct {
	GrainCount int
	Memory     float32
	CPU        float32
}

type GrainIdentity string

type ErrorRedirect struct {
	Addr addr.LogicAddr
}

func (e ErrorRedirect) Error() string {
	return "redirect"
}

type PlacementDriver interface {
	SetGetMetric(func() Metric)

	Login(context.Context) error

	Logout(context.Context) error

	//要求pd将Silo标记为不可分配
	MarkUnAvaliable()

	GetPlacement(context.Context, GrainIdentity) (addr.LogicAddr, error)

	ResetPlacementCache(GrainIdentity, addr.LogicAddr)

	Deactivate(context.Context, GrainIdentity) error

	/*
	 *  尝试请求pd将GrainIdentity的地址锁定为请求Silo的地址
	 *  如果GrainIdentity已经被其它Silo锁定，将返回ErrorRedirect,Addr字段含有重定向地址
	 */
	Activate(context.Context, GrainIdentity) error
}
