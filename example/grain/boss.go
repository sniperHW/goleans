package grain

import (
	"context"
	"fmt"
	"time"

	"github.com/sniperHW/goleans/example/grain/rpc/service/test"
	"github.com/sniperHW/goleans/grain"

	"github.com/sniperHW/clustergo"
)

type Boss struct {
	ctx  grain.Context
	Node *clustergo.Node
}

func (u *Boss) ServeTest(ctx context.Context, replyer *test.Replyer, arg *test.TestReq) {
	u.ctx.Await(time.Sleep, time.Second*2) //不会阻塞当前grain，可以继续处理请求
	replyer.Reply(&test.TestRsp{
		Msg: fmt.Sprintf("test response from (%s:%s) msg:%s", u.Node.Addr().LogicAddr().String(), u.ctx.Pid(), arg.Msg),
	})
}

func (u *Boss) Activate(ctx grain.Context) (error, bool) {
	u.ctx = ctx
	//注册rpc方法
	test.Register(ctx, u)
	return nil, false
}

func (u *Boss) Deactivate() error {
	return nil
}
