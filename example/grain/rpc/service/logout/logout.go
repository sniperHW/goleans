
package logout

import (
	"context"
	"goleans"
	"goleans/pd"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *LogoutRsp) {
	r.replyer.Reply(result)
}

type Logout interface {
	ServeLogout(context.Context, *Replyer,*LogoutReq)
}

func Register(grain *goleans.Grain,o Logout) error {
	return grain.RegisterMethod(5, func(ctx context.Context, r *goleans.Replyer, arg *LogoutReq){
		o.ServeLogout(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,identity pd.GrainIdentity,arg *LogoutReq) (*LogoutRsp,error) {
	var resp LogoutRsp
	err := goleans.Call(ctx,identity,5,arg,&resp)
	return &resp,err
}
