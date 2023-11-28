
package test

import (
	"context"
	"goleans"
	"goleans/pd"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *Response) {
	r.replyer.Reply(result)
}


func Register(grain *goleans.Grain,fn func(context.Context, *Replyer,*Request)) error {
	return grain.RegisterMethod(1, func(ctx context.Context, r *goleans.Replyer, arg *Request){
		fn(ctx,&Replyer{replyer:r},arg)
	})
}

func Call(ctx context.Context,identity pd.GrainIdentity,arg *Request) (*Response,error) {
	var resp Response
	err := goleans.Call(ctx,identity,1,arg,&resp)
	return &resp,err
}
