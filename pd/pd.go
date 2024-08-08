package pd

import (
	"context"
	"errors"
	"time"

	"github.com/sniperHW/clustergo/addr"
)

type Metric struct {
	GrainCount int
	Memory     float32
	CPU        float32
}

type ErrorRedirect struct {
	Addr addr.LogicAddr
}

func (e ErrorRedirect) Error() string {
	return "redirect"
}

var ErrInvaildIdentity error = errors.New("invaild identity")
var ErrNoAvaliableSilo error = errors.New("no avaliable silo")

type PlacementDriver interface {
	//设置Grain地址信息的缓存时间，GetPlacement会将本地Cache作废，强制从pdserver获取一次
	SetCacheTime(time.Duration)

	SetGetMetric(func() Metric)

	//grainList []string,Silo支持的Grain类型
	Login(context.Context, []string) error

	Logout(context.Context) error

	//要求pd将Silo标记为不可分配
	MarkUnAvaliable()

	GetPlacement(context.Context, string) (addr.LogicAddr, error)

	ResetPlacementCache(string, addr.LogicAddr)

	/*
	 * 放置
	 */
	Place(context.Context, string) error

	/*
	 *  尝试请求pd将Pid的地址锁定为请求Silo的地址
	 *  如果Pid已经被其它Silo锁定，将返回ErrorRedirect,Addr字段含有重定向地址
	 */
	Remove(context.Context, string) error
}
