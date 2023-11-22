package pd

import (
	"context"

	"github.com/sniperHW/clustergo/addr"
)

type PlacementDriver interface {
	Login(context.Context) ([]string, error)

	//要求pd将Silo标记为不可分配
	MarkUnAvaliable()

	/*
	 *获取identidy所在节点地址,ctx,identity
	 */
	GetPlacement(context.Context, string) (addr.LogicAddr, error)

	/*
	 * 清理identity的placement信息，收到对端返回actor不存在错误之后调用
	 */
	ClearPlacementCache(string)

	//如果callback返回false表明Silo不能激活Grain(例如正在Stop)
	SetActiveCallback(func(string) bool)

	Deactvie(context.Context, string) error
}
