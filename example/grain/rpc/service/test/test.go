
package test

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

func (r *Replyer) Reply(result *TestRsp) {
	r.replyer.Reply(result)
}

type Test interface {
	ServeTest(context.Context, *Replyer,*TestReq)
}

func Register(ctx grain.Context,o Test) error {
	return ctx.RegisterMethod(1, func(ctx context.Context, r rpc.Replyer, arg *TestReq){
		o.ServeTest(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,pid string,arg *TestReq) (*TestRsp,error) {
	var resp TestRsp
	err := goleans.Call(ctx,pid,1,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid string,arg *TestReq,d time.Duration) (*TestRsp,error) {
	var resp TestRsp
	err := goleans.CallWithTimeout(pid,1,arg,&resp,d)
	return &resp,err
}

