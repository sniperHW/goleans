
package relaymsg

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

func (r *Replyer) Reply(result *RelaymsgRsp) {
	r.replyer.Reply(result)
}

type Relaymsg interface {
	ServeRelaymsg(context.Context, *Replyer,*RelaymsgReq)
}

func Register(ctx grain.Context,o Relaymsg) error {
	return ctx.RegisterMethod(4, func(ctx context.Context, r rpc.Replyer, arg *RelaymsgReq){
		o.ServeRelaymsg(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,pid string,arg *RelaymsgReq) (*RelaymsgRsp,error) {
	var resp RelaymsgRsp
	err := goleans.Call(ctx,pid,4,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid string,arg *RelaymsgReq,d time.Duration) (*RelaymsgRsp,error) {
	var resp RelaymsgRsp
	err := goleans.CallWithTimeout(pid,4,arg,&resp,d)
	return &resp,err
}

