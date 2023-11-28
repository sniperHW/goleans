package grain

import (
	"context"
	"fmt"
	"goleans"
	"time"

	"goleans/example/grain/rpc/service/test"

	"github.com/sniperHW/clustergo"
)

type Boss struct {
	grain *goleans.Grain
	Node  *clustergo.Node
}

func (u *Boss) ServeTest(ctx context.Context, replyer *test.Replyer, arg *test.TestReq) {
	u.grain.Await(time.Sleep, time.Second*2) //不会阻塞当前grain，可以继续处理请求
	replyer.Reply(&test.TestRsp{
		Msg: fmt.Sprintf("test response from (%s:%s) msg:%s", u.Node.Addr().LogicAddr().String(), u.grain.Identity, arg.Msg),
	})
}

func (u *Boss) Init(grain *goleans.Grain) error {
	u.grain = grain
	//注册rpc方法
	test.Register(grain, u)
	return nil
}

func (u *Boss) Deactivate() error {
	return nil
}
