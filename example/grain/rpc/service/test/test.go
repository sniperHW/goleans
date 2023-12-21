
package test

import (
	"context"
	"github.com/sniperHW/goleans"
	"github.com/sniperHW/goleans/pd"
	"time"
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


func Call(ctx context.Context,pid pd.Pid,arg *TestReq) (*TestRsp,error) {
	var resp TestRsp
	err := goleans.Call(ctx,pid,1,arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid pd.Pid,arg *TestReq,d time.Duration) (*TestRsp,error) {
	var resp TestRsp
	err := goleans.CallWithTimeout(pid,1,arg,&resp,d)
	return &resp,err
}

