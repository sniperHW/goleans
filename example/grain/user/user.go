package user

import (
	"context"
	"fmt"
	"goleans"

	"goleans/example/grain/user/rpc/service/echo"
	"goleans/example/grain/user/rpc/service/test"

	"github.com/sniperHW/clustergo"
)

type User struct {
	grain *goleans.Grain
	name  string
	Node  *clustergo.Node
}

func (u *User) echo(ctx context.Context, replyer *echo.Replyer, arg *echo.Request) {
	replyer.Reply(&echo.Response{
		Msg: fmt.Sprintf("echo response from (%s:%s) msg:%s", u.Node.Addr().LogicAddr().String(), u.grain.Identity, arg.Msg),
	})
}

func (u *User) test(ctx context.Context, replyer *test.Replyer, arg *test.Request) {
	replyer.Reply(&test.Response{
		Msg: fmt.Sprintf("test response from (%s:%s) msg:%s", u.Node.Addr().LogicAddr().String(), u.grain.Identity, arg.Msg),
	})
}

func (u *User) Init(grain *goleans.Grain) error {
	//从数据库加载数据，初始化User
	u.grain = grain
	//注册rpc方法
	echo.Register(grain, u.echo)
	test.Register(grain, u.test)
	return nil
}

func (u *User) Deactivate() error {
	//将User保存到数据库
	return nil
}
