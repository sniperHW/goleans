package goleans

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"goleans/pd"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"google.golang.org/protobuf/proto"
)

const (
	lenSeq                 = 8
	lenOneWay              = 1
	lenMethod              = 2
	lenIdentityLen         = 2
	lenErrCode             = 2
	lenArg                 = 4
	lenReqHdr              = lenSeq + lenOneWay + lenMethod + lenIdentityLen
	lenRspHdr              = lenSeq + lenErrCode
	Actor_request          = 11311
	Actor_response         = 11312
	Actor_notify_not_exist = 11313
)

const (
	ErrOk = iota
	ErrMethodCallPanic
	ErrInvaildArg
	ErrGrainNotExist
	ErrMethodNotExist
	ErrUserGrainCreateError
	ErrUserGrainInitError
)

var errDesc []error = []error{
	errors.New("Ok"),
	errors.New("Method call Panic"),
	errors.New("Invaild Arg"),
	errors.New("Grain Not Exist"),
	errors.New("Method Not Exist"),
	errors.New("User Grain Create Error"),
	errors.New("User Grain Init Error"),
}

func getDescByErrCode(code uint16) error {
	if int(code) < len(errDesc) {
		return errDesc[code]
	} else {
		return errors.New("Unknow error")
	}
}

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

	binary.BigEndian.PutUint16(buff[9:], req.Method)

	binary.BigEndian.PutUint16(buff[11:], uint16(len(req.To)))

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
	lenIdentity := int(binary.BigEndian.Uint16(buff[r:]))
	r += lenIdentityLen

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
	binary.BigEndian.PutUint16(buff[8:], uint16(resp.ErrCode))
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

////server

type Replyer struct {
	seq      uint64
	replyed  int32
	oneway   bool
	identity string
	from     addr.LogicAddr
	node     *clustergo.Node
}

func (r *Replyer) Error(errCode int) {
	if errCode == ErrGrainNotExist && r.oneway {
		//通告对端identity不在当前节点
		r.node.SendBinMessage(r.from, Actor_notify_not_exist, []byte(r.identity))
	} else if !r.oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq:     r.seq,
			ErrCode: errCode,
		}
		if err := r.node.SendBinMessage(r.from, Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

func (r *Replyer) Reply(ret proto.Message) {
	if !r.oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq: r.seq,
		}

		if b, err := proto.Marshal(ret); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		} else {
			resp.Ret = b
		}

		if err := r.node.SendBinMessage(r.from, Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
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
				logger.Errorf("%s ", fmt.Errorf(fmt.Sprintf("%v: %s", r, buf[:l])))
				replyer.Error(ErrMethodCallPanic)
			}
		}()
		c.fn.Call([]reflect.Value{reflect.ValueOf(context), reflect.ValueOf(replyer), reflect.ValueOf(arg)})
	} else {
		replyer.Error(ErrInvaildArg)
	}
}

/////Client

type callContext struct {
	onResponse   func(interface{}, error)
	fired        int32
	respReceiver proto.Message
}

func (c *callContext) callOnResponse(resp []byte, err int) {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		if err == ErrOk {
			if e := proto.Unmarshal(resp, c.respReceiver); e != nil {
				logger.Errorf("callOnResponse decode error:%v", e)
				c.onResponse(nil, errors.New("invaild respReceiver"))
			} else {
				c.onResponse(c.respReceiver, nil)
			}
		} else {
			c.onResponse(nil, getDescByErrCode(uint16(err)))
		}
	}
}

type RPCClient struct {
	sync.Mutex
	nextSequence    uint32
	timestamp       uint32
	timeOffset      uint32
	startTime       time.Time
	pendingCall     [32]sync.Map
	node            *clustergo.Node
	placementDriver pd.PlacementDriver
}

func NewRPCClient(node *clustergo.Node, placementDriver pd.PlacementDriver) *RPCClient {
	return &RPCClient{
		timeOffset:      uint32(time.Now().Unix() - time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).Unix()),
		startTime:       time.Now(),
		node:            node,
		placementDriver: placementDriver,
	}
}

func (c *RPCClient) getTimeStamp() uint32 {
	return uint32(time.Since(c.startTime)/time.Second) + c.timeOffset
}

func (c *RPCClient) makeSequence() (seq uint64) {
	timestamp := c.getTimeStamp()
	c.Lock()
	if timestamp > c.timestamp {
		c.timestamp = timestamp
		c.nextSequence = 1
	} else {
		c.nextSequence++
	}
	seq = uint64(c.nextSequence)
	c.Unlock()
	return seq
}

func (c *RPCClient) OnRPCResponse(ctx context.Context, resp *ResponseMsg) {
	if ctx, ok := c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).callOnResponse(resp.Ret, resp.ErrCode)
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *RPCClient) Call(ctx context.Context, identity string, method uint16, arg proto.Message, ret proto.Message) error {
	if b, err := proto.Marshal(arg); err != nil {
		return err
	} else {
		reqMessage := &RequestMsg{
			To:     identity,
			Seq:    c.makeSequence(),
			Method: method,
			Arg:    b,
		}

		for {
			remoteAddr, err := c.placementDriver.GetPlacement(ctx, identity)
			if err != nil {
				return err
			}

			if ret != nil {
				waitC := make(chan error, 1)
				pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]

				pending.Store(reqMessage.Seq, &callContext{
					respReceiver: ret,
					onResponse: func(_ interface{}, err error) {
						waitC <- err
					},
				})

				c.node.SendBinMessage(remoteAddr, Actor_request, reqMessage.Encode())

				select {
				case err := <-waitC:
					if err == nil || err != getDescByErrCode(ErrGrainNotExist) {
						return err
					} else {
						//err == ErrGrainNotExist
						c.placementDriver.ClearPlacementCache(identity)
						//continue
					}
				case <-ctx.Done():
					pending.Delete(reqMessage.Seq)
					switch ctx.Err() {
					case context.Canceled:
						return errors.New("canceled")
					case context.DeadlineExceeded:
						return errors.New("timeout")
					default:
						return errors.New("unknow")
					}
				}
			} else {
				reqMessage.Oneway = true
				return c.node.SendBinMessage(remoteAddr, Actor_request, reqMessage.Encode())
			}
		}
	}
}
