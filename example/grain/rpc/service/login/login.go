
package login

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

func (r *Replyer) Reply(result *LoginRsp) {
	r.replyer.Reply(result)
}

type Login interface {
	ServeLogin(context.Context, *Replyer,*LoginReq)
}

func Register(ctx grain.Context,o Login) error {
	return ctx.RegisterMethod(3, func(ctx context.Context, r rpc.Replyer, arg *LoginReq){
		o.ServeLogin(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,pid string,arg *LoginReq) (*LoginRsp,error) {
	var resp LoginRsp
	err := goleans.Call(ctx,pid,3,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid string,arg *LoginReq,d time.Duration) (*LoginRsp,error) {
	var resp LoginRsp
	err := goleans.CallWithTimeout(pid,3,arg,&resp,d)
	return &resp,err
}

