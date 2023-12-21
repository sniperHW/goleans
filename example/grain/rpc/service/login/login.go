
package login

import (
	"context"
	"github.com/sniperHW/goleans"
	"github.com/sniperHW/goleans/pd"
	"time"
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


func Call(ctx context.Context,pid pd.Pid,arg *LoginReq) (*LoginRsp,error) {
	var resp LoginRsp
	err := goleans.Call(ctx,pid,3,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid pd.Pid,arg *LoginReq,d time.Duration) (*LoginRsp,error) {
	var resp LoginRsp
	err := goleans.CallWithTimeout(pid,3,arg,&resp,d)
	return &resp,err
}

