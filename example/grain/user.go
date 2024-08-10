package grain

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/sniperHW/goleans"

	"github.com/sniperHW/goleans/example/codec"
	"github.com/sniperHW/goleans/example/grain/rpc/service/echo"
	"github.com/sniperHW/goleans/example/grain/rpc/service/login"
	"github.com/sniperHW/goleans/example/grain/rpc/service/logout"
	"github.com/sniperHW/goleans/example/grain/rpc/service/relaymsg"
	"github.com/sniperHW/goleans/grain"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/clustergo/codec/buffer"
)

type gateUser struct {
	gateAddr   addr.LogicAddr
	gateUserID uint64
}

type User struct {
	ctx      grain.Context
	name     string
	Node     *clustergo.Node
	codecc   *codec.CSCodec
	gateUser *gateUser
}

func (u *User) SendToUser(msg *codec.Message) {
	if u.gateUser != nil {
		b := u.codecc.Encode(msg)
		b = buffer.NeWriter(binary.BigEndian).AppendUint64(b, u.gateUser.gateUserID)
		clustergo.SendBinMessage(u.gateUser.gateAddr, 100, b, time.Now().Add(time.Second))
	}
}

func (u *User) ServeEcho(ctx context.Context, replyer *echo.Replyer, arg *echo.EchoReq) {
	time.Sleep(time.Second * 2) //阻塞当前grain
	replyer.Reply(&echo.EchoRsp{
		Msg: fmt.Sprintf("echo response from (%s:%s) msg:%s", u.Node.Addr().LogicAddr().String(), u.ctx.Pid(), arg.Msg),
	})
}

func (u *User) ServeRelaymsg(ctx context.Context, replyer *relaymsg.Replyer, arg *relaymsg.RelaymsgReq) {
	resp := &relaymsg.RelaymsgRsp{
		Ok: false,
	}
	if u.gateUser != nil && u.gateUser.gateAddr == addr.LogicAddr(arg.GateAddr) && u.gateUser.gateUserID == uint64(arg.GateUserID) {
		//只处理网关对象匹配的请求
		resp.Ok = true
		msg := u.codecc.Decode(arg.Msg)
		//处理消息，将响应返回给玩家
		defer u.SendToUser(msg)
	}
	replyer.Reply(resp)
}

func (u *User) ServeLogout(ctx context.Context, replyer *logout.Replyer, arg *logout.LogoutReq) {
	resp := &logout.LogoutRsp{}
	if u.gateUser != nil && u.gateUser.gateAddr == addr.LogicAddr(arg.GateAddr) && u.gateUser.gateUserID == uint64(arg.GateUserID) {
		u.gateUser = nil
		goleans.Log().Debugf("%s logout", u.ctx.Pid())
	}
	replyer.Reply(resp)
}

func (u *User) ServeLogin(ctx context.Context, replyer *login.Replyer, arg *login.LoginReq) {
	resp := &login.LoginRsp{
		Ok: true,
	}
	if u.gateUser == nil {
		u.gateUser = &gateUser{}
	}
	//绑定网关对象
	u.gateUser.gateAddr = addr.LogicAddr(arg.GateAddr)
	u.gateUser.gateUserID = uint64(arg.GateUserID)

	replyer.Reply(resp)
}

func (u *User) Activate(ctx grain.Context) (error, bool) {
	//从数据库加载数据，初始化User
	u.codecc = codec.NewCodec()
	u.ctx = ctx
	//注册rpc方法
	echo.Register(ctx, u)
	login.Register(ctx, u)
	relaymsg.Register(ctx, u)
	logout.Register(ctx, u)
	return nil, false
}

func (u *User) Deactivate() error {
	//将User保存到数据库
	return nil
}
