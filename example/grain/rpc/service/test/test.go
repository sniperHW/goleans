
package test

import (
	"context"
	"goleans"
	"goleans/pd"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *TestRsp) {
	r.replyer.Reply(result)
}

type Test interface {
	ServeTest(context.Context, *Replyer,*TestReq)
}

func Register(grain *goleans.Grain,o Test) error {
	return grain.RegisterMethod(1, func(ctx context.Context, r *goleans.Replyer, arg *TestReq){
		o.ServeTest(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,identity pd.GrainIdentity,arg *TestReq) (*TestRsp,error) {
	var resp TestRsp
	err := goleans.Call(ctx,identity,1,arg,&resp)
	return &resp,err
}
