package grain

import (
	"context"
	"fmt"
	"goleans"
	"time"

	"goleans/example/grain/rpc/service/echo"

	"github.com/sniperHW/clustergo"
)

type User struct {
	grain *goleans.Grain
	name  string
	Node  *clustergo.Node
}

func (u *User) ServeEcho(ctx context.Context, replyer *echo.Replyer, arg *echo.EchoReq) {
	time.Sleep(time.Second * 2) //阻塞当前grain
	replyer.Reply(&echo.EchoRsp{
		Msg: fmt.Sprintf("echo response from (%s:%s) msg:%s", u.Node.Addr().LogicAddr().String(), u.grain.Identity, arg.Msg),
	})
}

func (u *User) Init(grain *goleans.Grain) error {
	//从数据库加载数据，初始化User
	u.grain = grain
	//注册rpc方法
	echo.Register(grain, u)
	return nil
}

func (u *User) Deactivate() error {
	//将User保存到数据库
	return nil
}
