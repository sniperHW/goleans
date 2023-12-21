
package relaymsg

import (
	"context"
	"github.com/sniperHW/goleans"
	"github.com/sniperHW/goleans/pd"
	"time"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *RelaymsgRsp) {
	r.replyer.Reply(result)
}

type Relaymsg interface {
	ServeRelaymsg(context.Context, *Replyer,*RelaymsgReq)
}

func Register(grain *goleans.Grain,o Relaymsg) error {
	return grain.RegisterMethod(4, func(ctx context.Context, r *goleans.Replyer, arg *RelaymsgReq){
		o.ServeRelaymsg(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,pid pd.Pid,arg *RelaymsgReq) (*RelaymsgRsp,error) {
	var resp RelaymsgRsp
	err := goleans.Call(ctx,pid,4,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid pd.Pid,arg *RelaymsgReq,d time.Duration) (*RelaymsgRsp,error) {
	var resp RelaymsgRsp
	err := goleans.CallWithTimeout(pid,4,arg,&resp,d)
	return &resp,err
}

