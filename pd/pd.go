package pd

import (
	"context"

	"github.com/sniperHW/clustergo/addr"
)

type PlacementDriver interface {
	/*
	 *获取identidy所在节点地址,ctx,identity
	 */
	GetHostService(context.Context, string) (addr.LogicAddr, error)

	/*
	 *取消对actor的激活,ctx,identity,node addr
	 */
	Deactive(context.Context, string, addr.LogicAddr) error

	/*
	 *设置回调函数，当actor在本地激活时调用
	 */
	SetActiveCallback(func(string))

	/*
	 * 清理identity的placement信息，收到对端返回actor不存在错误之后调用
	 */
	ClearPlacementInfo(string)
}
