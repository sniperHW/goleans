
package login

import (
	"context"
	"goleans"
	"goleans/pd"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *LoginRsp) {
	r.replyer.Reply(result)
}

type Login interface {
	ServeLogin(context.Context, *Replyer,*LoginReq)
}

func Register(grain *goleans.Grain,o Login) error {
	return grain.RegisterMethod(3, func(ctx context.Context, r *goleans.Replyer, arg *LoginReq){
		o.ServeLogin(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,identity pd.GrainIdentity,arg *LoginReq) (resp LoginRsp,err error) {
	err = goleans.Call(ctx,identity,3,arg,&resp)
	return resp,err
}
