
package relaymsg

import (
	"context"
	"goleans"
	"goleans/pd"
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


func Call(ctx context.Context,identity pd.GrainIdentity,arg *RelaymsgReq) (*RelaymsgRsp,error) {
	var resp RelaymsgRsp
	err := goleans.Call(ctx,identity,4,arg,&resp)
	return &resp,err
}
