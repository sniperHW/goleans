
package logout

import (
	"context"
	"github.com/sniperHW/goleans"
	"github.com/sniperHW/goleans/pd"
	"time"
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


func Call(ctx context.Context,pid pd.Pid,arg *LogoutReq) (*LogoutRsp,error) {
	var resp LogoutRsp
	err := goleans.Call(ctx,pid,5,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid pd.Pid,arg *LogoutReq,d time.Duration) (*LogoutRsp,error) {
	var resp LogoutRsp
	err := goleans.CallWithTimeout(pid,5,arg,&resp,d)
	return &resp,err
}

