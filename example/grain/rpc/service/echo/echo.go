
package echo

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

func (r *Replyer) Reply(result *EchoRsp) {
	r.replyer.Reply(result)
}

type Echo interface {
	ServeEcho(context.Context, *Replyer,*EchoReq)
}

func Register(ctx grain.Context,o Echo) error {
	return ctx.RegisterMethod(2, func(ctx context.Context, r rpc.Replyer, arg *EchoReq){
		o.ServeEcho(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,pid string,arg *EchoReq) (*EchoRsp,error) {
	var resp EchoRsp
	err := goleans.Call(ctx,pid,2,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid string,arg *EchoReq,d time.Duration) (*EchoRsp,error) {
	var resp EchoRsp
	err := goleans.CallWithTimeout(pid,2,arg,&resp,d)
	return &resp,err
}

