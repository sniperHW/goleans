
package logout

import (
	"context"
	"github.com/sniperHW/goleans/grain"
	"github.com/sniperHW/goleans/rpc"
	"github.com/sniperHW/goleans"
	"time"
)

type Replyer struct {
	replyer rpc.Replyer
}

func (r *Replyer) Reply(result *LogoutRsp) {
	r.replyer.Reply(result)
}

type Logout interface {
	ServeLogout(context.Context, *Replyer,*LogoutReq)
}

func Register(ctx grain.Context,o Logout) error {
	return ctx.RegisterMethod(5, func(ctx context.Context, r rpc.Replyer, arg *LogoutReq){
		o.ServeLogout(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,pid string,arg *LogoutReq) (*LogoutRsp,error) {
	var resp LogoutRsp
	err := goleans.Call(ctx,pid,5,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid string,arg *LogoutReq,d time.Duration) (*LogoutRsp,error) {
	var resp LogoutRsp
	err := goleans.CallWithTimeout(pid,5,arg,&resp,d)
	return &resp,err
}

