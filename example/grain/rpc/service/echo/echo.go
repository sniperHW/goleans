
package echo

import (
	"context"
	"goleans"
	"goleans/pd"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *EchoRsp) {
	r.replyer.Reply(result)
}

type Echo interface {
	ServeEcho(context.Context, *Replyer,*EchoReq)
}

func Register(grain *goleans.Grain,o Echo) error {
	return grain.RegisterMethod(2, func(ctx context.Context, r *goleans.Replyer, arg *EchoReq){
		o.ServeEcho(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,identity pd.GrainIdentity,arg *EchoReq) (*EchoRsp,error) {
	var resp EchoRsp
	err := goleans.Call(ctx,identity,2,arg,&resp)
	return &resp,err
}
