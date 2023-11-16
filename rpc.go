package actorgo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"google.golang.org/protobuf/proto"
)

const (
	lenSeq         = 8
	lenOneWay      = 1
	lenMethod      = 2
	lenIdentityLen = 2
	lenErrCode     = 2
	lenArg         = 4
	lenReqHdr      = lenSeq + lenOneWay + lenMethod + lenIdentityLen
	lenRspHdr      = lenSeq + lenErrCode
	actor_request  = 1
	actor_response = 2
)

const (
	ErrOk = iota
	ErrPanic
	ErrInvaildArg
)

type RequestMsg struct {
	Seq    uint64
	Method uint16
	Oneway bool
	To     string
	Arg    []byte
}

type ResponseMsg struct {
	Seq     uint64
	ErrCode int
	Ret     []byte
}

func (req *RequestMsg) Encode() []byte {

	buff := make([]byte, lenReqHdr, lenReqHdr+len(req.To)+len(req.Arg))

	binary.BigEndian.PutUint64(buff, req.Seq)

	if req.Oneway {
		buff[8] = byte(1)
	}

	binary.BigEndian.PutUint16(buff, req.Method)

	binary.BigEndian.PutUint16(buff, uint16(len(req.To)))

	buff = append(buff, []byte(req.To)...)

	buff = append(buff, req.Arg...)

	return buff
}

func (req *RequestMsg) Decode(buff []byte) error {
	r := 0
	buffLen := len(buff)
	if buffLen-r < lenSeq {
		return errors.New("invaild request packet")
	}
	req.Seq = binary.BigEndian.Uint64(buff[r:])
	r += lenSeq
	if buffLen-r < lenOneWay {
		return errors.New("invaild request packet")
	}
	if buff[r] == byte(1) {
		req.Oneway = true
	}
	r += lenOneWay
	if buffLen-r < lenMethod {
		return errors.New("invaild request packet")
	}
	req.Method = binary.BigEndian.Uint16(buff[r:])
	r += lenMethod

	if buffLen-r < lenIdentityLen {
		return errors.New("invaild request packet")
	}
	r += lenIdentityLen
	lenIdentity := int(binary.BigEndian.Uint16(buff[r:]))
	if buffLen-r < lenIdentity {
		return errors.New("invaild request packet")
	}
	req.To = string(buff[r : r+lenIdentity])
	r += lenIdentity

	if buffLen-r > 0 {
		req.Arg = make([]byte, 0, buffLen-r)
		req.Arg = append(req.Arg, buff[r:]...)
	}

	return nil
}

func (resp *ResponseMsg) Encode() (buff []byte) {
	buff = make([]byte, 10, lenRspHdr+len(resp.Ret))
	binary.BigEndian.PutUint64(buff, resp.Seq)
	binary.BigEndian.PutUint16(buff, uint16(resp.ErrCode))
	buff = append(buff, resp.Ret...)
	return buff
}

func (resp *ResponseMsg) Decode(buff []byte) error {
	r := 0
	buffLen := len(buff)
	if buffLen-r < lenSeq {
		return errors.New("invaild response packet")
	}
	resp.Seq = binary.BigEndian.Uint64(buff[r:])
	r += lenSeq

	if buffLen-r < lenErrCode {
		return errors.New("invaild response packet")
	}

	resp.ErrCode = int(binary.BigEndian.Uint16(buff[r:]))
	r += lenErrCode

	if buffLen-r > 0 {
		resp.Ret = make([]byte, 0, buffLen-r)
		resp.Ret = append(resp.Ret, buff[r:]...)
	}
	return nil
}

type Replyer struct {
	seq     uint64
	replyed int32
	oneway  bool
	from    addr.LogicAddr
}

func (r *Replyer) Error(errCode int) {
	if !r.oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq:     r.seq,
			ErrCode: errCode,
		}

		if err := clustergo.SendBinMessage(r.from, resp.Encode(), actor_response); err != nil {
			clustergo.Log().Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

func (r *Replyer) Reply(ret proto.Message) {
	if !r.oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq: r.seq,
		}

		if b, err := proto.Marshal(ret); err != nil {
			clustergo.Log().Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		} else {
			resp.Ret = b
		}

		if err := clustergo.SendBinMessage(r.from, resp.Encode(), actor_response); err != nil {
			clustergo.Log().Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

type methodCaller struct {
	argType reflect.Type
	fn      reflect.Value
}

// 接受的method func(context.Context, *Replyer,*Pointer)
func makeMethodCaller(method interface{}) (*methodCaller, error) {
	if method == nil {
		return nil, errors.New("method is nil")
	}

	fnType := reflect.TypeOf(method)
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if fnType.NumIn() != 3 {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if !fnType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if fnType.In(1) != reflect.TypeOf(&Replyer{}) {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	if fnType.In(2).Kind() != reflect.Ptr {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,*Pointer)")
	}

	caller := &methodCaller{
		argType: fnType.In(2).Elem(),
		fn:      reflect.ValueOf(method),
	}

	return caller, nil
}

func (c *methodCaller) call(context context.Context, replyer *Replyer, req *RequestMsg) {
	arg := reflect.New(c.argType).Interface()
	if err := proto.Unmarshal(req.Arg, arg.(proto.Message)); err == nil {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 65535)
				l := runtime.Stack(buf, false)
				clustergo.Log().Errorf("%s ", fmt.Errorf(fmt.Sprintf("%v: %s", r, buf[:l])))
				replyer.Error(ErrPanic)
			}
		}()
		c.fn.Call([]reflect.Value{reflect.ValueOf(context), reflect.ValueOf(replyer), reflect.ValueOf(arg)})
	} else {
		replyer.Error(ErrInvaildArg)
	}
}

//type Server struct {
//	sync.RWMutex

//methods map[string]*methodCaller
//pause   int32
//codec   Codec
//}

//func (s *Server) Register(, method interface{}) error {
//	s.Lock()
//	defer s.Unlock()

/*if name == "" {
	return errors.New("RegisterMethod nams is nil")
} else if caller, err := makeMethodCaller(name, method); err != nil {
	return err
} else {
	if _, ok := s.methods[name]; ok {
		return fmt.Errorf("duplicate method:%s", name)
	} else {
		s.methods[name] = caller
		return nil
	}
}*/
//}

/*func (s *Server) UnRegister(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.methods, name)
}

func (s *Server) method(name string) *methodCaller {
	s.RLock()
	defer s.RUnlock()
	return s.methods[name]
}

func (s *Server) OnMessage(context context.Context, channel Channel, req *RequestMsg) {
	replyer := &Replyer{channel: channel, seq: req.Seq, codec: s.codec, oneway: req.Oneway}
	if caller := s.method(req.Method); caller == nil {
		replyer.Error(NewError(ErrInvaildMethod, fmt.Sprintf("method %s not found", req.Method)))
	} else if atomic.LoadInt32(&s.pause) == 1 {
		replyer.Error(NewError(ErrServerPause, "server pause"))
	} else {
		caller.call(context, s.codec, replyer, req)
	}
}*/
