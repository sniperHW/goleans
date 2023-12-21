
package echo

import (
	"context"
	"github.com/sniperHW/goleans"
	"github.com/sniperHW/goleans/pd"
	"time"
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


func Call(ctx context.Context,pid pd.Pid,arg *EchoReq) (*EchoRsp,error) {
	var resp EchoRsp
	err := goleans.Call(ctx,pid,2,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid pd.Pid,arg *EchoReq,d time.Duration) (*EchoRsp,error) {
	var resp EchoRsp
	err := goleans.CallWithTimeout(pid,2,arg,&resp,d)
	return &resp,err
}

