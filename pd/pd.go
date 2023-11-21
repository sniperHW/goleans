package pd

import (
	"context"

	"github.com/sniperHW/clustergo/addr"
)

type PlacementDriver interface {
	Login() ([]string, error)
	/*
	 *获取identidy所在节点地址,ctx,identity
	 */
	GetPlacement(context.Context, string) (addr.LogicAddr, error)

	/*
	 * 清理identity的placement信息，收到对端返回actor不存在错误之后调用
	 */
	ClearPlacementCache(string)

	SetActiveCallback(func(string))

	Deactvie(context.Context) error
}
