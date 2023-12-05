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
	"github.com/sniperHW/netgo"
	"google.golang.org/protobuf/proto"
)

const (
	lenSeq                = 8
	lenOneWay             = 1
	lenMethod             = 2
	lenIdentityLen        = 2
	lenErrCode            = 2
	lenArg                = 4
	lenReqHdr             = lenSeq + lenOneWay + lenMethod + lenIdentityLen
	lenRspHdr             = lenSeq + lenErrCode
	Actor_request         = 11311
	Actor_response        = 11312
	Actor_notify_redirect = 11313
)

var (
	RetryInterval = 100 * time.Millisecond //遇到ErrCodeRetryAgain错误时的重试间隔
)

const (
	ErrCodeOk = iota
	ErrCodeMethodCallPanic
	ErrCodeInvaildArg
	ErrCodeMethodNotExist
	ErrCodeUserGrainCreateError
	ErrCodeUserGrainInitError
	ErrCodeRedirect
	ErrCodeRetryAgain
	ErrCodeInvaildIdentity
)

var errDesc []error = []error{
	ErrCallOK,
	ErrCallMethodPanic,
	ErrCallInvaildArgument,
	ErrCallMethodNotFound,
	ErrCallGrainCreate,
	ErrCallGrainInit,
	ErrCallRedirect,
	ErrCallRetry,
	ErrCallInvaildIdentity,
}

func getDescByErrCode(code uint16) error {
	if int(code) < len(errDesc) {
		return errDesc[code]
	} else {
		return errors.New("unknow error")
	}
}

type RequestMsg struct {
	Seq    uint64
	Method uint16
	Oneway bool
	To     pd.GrainIdentity
	Arg    []byte
}

type ResponseMsg struct {
	Seq          uint64
	ErrCode      int
	RedirectAddr int
	Ret          []byte
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
	req.To = pd.GrainIdentity(buff[r : r+lenIdentity])
	r += lenIdentity

	if buffLen-r > 0 {
		req.Arg = make([]byte, 0, buffLen-r)
		req.Arg = append(req.Arg, buff[r:]...)
	}

	return nil
}

func (resp *ResponseMsg) Encode() (buff []byte) {
	if resp.ErrCode == ErrCodeRedirect {
		buff = make([]byte, lenRspHdr+4)
		binary.BigEndian.PutUint64(buff, resp.Seq)
		binary.BigEndian.PutUint16(buff[8:], uint16(resp.ErrCode))
		binary.BigEndian.PutUint32(buff[10:], uint32(resp.RedirectAddr))
	} else {
		buff = make([]byte, lenRspHdr, lenRspHdr+len(resp.Ret))
		binary.BigEndian.PutUint64(buff, resp.Seq)
		binary.BigEndian.PutUint16(buff[8:], uint16(resp.ErrCode))
		buff = append(buff, resp.Ret...)
	}
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

	if resp.ErrCode == ErrCodeRedirect {
		if buffLen-r >= 4 {
			resp.RedirectAddr = int(binary.BigEndian.Uint32(buff[r:]))
		}
	} else {
		if buffLen-r > 0 {
			resp.Ret = make([]byte, 0, buffLen-r)
			resp.Ret = append(resp.Ret, buff[r:]...)
		}
	}
	return nil
}

////server

type Replyer struct {
	seq      uint64
	replyed  int32
	oneway   bool
	identity pd.GrainIdentity
	from     addr.LogicAddr
	node     *clustergo.Node
}

func (r *Replyer) Redirect(redirectAddr addr.LogicAddr) {
	if r.oneway {
		//通告对端identity不在当前节点
		buff := make([]byte, 4, len(r.identity)+4)
		binary.BigEndian.PutUint32(buff, uint32(redirectAddr))
		buff = append(buff, []byte(r.identity)...)
		r.node.SendBinMessage(r.from, Actor_notify_redirect, buff)
	} else if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq:          r.seq,
			ErrCode:      ErrCodeRedirect,
			RedirectAddr: int(redirectAddr),
		}
		if err := r.node.SendBinMessage(r.from, Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

func (r *Replyer) Error(errCode int) {
	if !r.oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
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

// 接受的method func(context.Context, *Replyer,proto.Message)
func makeMethodCaller(method interface{}) (*methodCaller, error) {
	if method == nil {
		return nil, errors.New("method is nil")
	}

	fnType := reflect.TypeOf(method)
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,proto.Message)")
	}

	if fnType.NumIn() != 3 {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,proto.Message)")
	}

	if !fnType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,proto.Message)")
	}

	if fnType.In(1) != reflect.TypeOf(&Replyer{}) {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,proto.Message)")
	}

	if !fnType.In(2).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
		return nil, errors.New("method should have type func(context.Contex,*Replyer,proto.Message)")
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
				replyer.Error(ErrCodeMethodCallPanic)
			}
		}()
		c.fn.Call([]reflect.Value{reflect.ValueOf(context), reflect.ValueOf(replyer), reflect.ValueOf(arg)})
	} else {
		replyer.Error(ErrCodeInvaildArg)
	}
}

/////Client

type callContext struct {
	respC        chan error
	fired        int32
	respReceiver proto.Message
}

func (c *callContext) callOnResponse(resp *ResponseMsg) {
	if atomic.CompareAndSwapInt32(&c.fired, 0, 1) {
		if resp.ErrCode == ErrCodeOk {
			if e := proto.Unmarshal(resp.Ret, c.respReceiver); e != nil {
				logger.Errorf("callOnResponse decode error:%v", e)
				c.respC <- errors.New("invaild respReceiver")
			} else {
				c.respC <- nil
			}
		} else if resp.ErrCode == ErrCodeRedirect {
			c.respC <- pd.ErrorRedirect{Addr: addr.LogicAddr(resp.RedirectAddr)}
		} else {
			c.respC <- getDescByErrCode(uint16(resp.ErrCode))
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
	seq = uint64(c.timestamp)<<32 + uint64(c.nextSequence)
	c.Unlock()
	return seq
}

func (c *RPCClient) OnRPCResponse(resp *ResponseMsg) {
	if ctx, ok := c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(*callContext).callOnResponse(resp)
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func (c *RPCClient) Call(ctx context.Context, identity pd.GrainIdentity, method uint16, arg proto.Message, ret proto.Message) error {
	if b, err := proto.Marshal(arg); err != nil {
		return err
	} else {
		reqMessage := &RequestMsg{
			To:     identity,
			Seq:    c.makeSequence(),
			Method: method,
			Arg:    b,
		}
		if ret == nil {
			reqMessage.Oneway = true
			req := reqMessage.Encode()
			for {
				var pdErr error
				remoteAddr, err := c.placementDriver.GetPlacement(ctx, identity)
				if err != nil {
					pdErr = err
					time.Sleep(time.Millisecond * 10)
				} else if err = c.node.SendBinMessageWithContext(ctx, remoteAddr, Actor_request, req); err == nil {
					return nil
				} else {
					switch err {
					case clustergo.ErrInvaildNode:
						c.placementDriver.ResetPlacementCache(identity, addr.LogicAddr(0))
					case clustergo.ErrPendingQueueFull, netgo.ErrSendQueueFull, netgo.ErrPushToSendQueueTimeout:
						time.Sleep(time.Millisecond * 10)
					default:
						return err
					}
				}

				select {
				case <-ctx.Done():
					if pdErr != nil {
						logger.Errorf("call grain:%s timeout with pd error:%v", identity, pdErr)
					}
					err = ctx.Err()
					switch err {
					case context.Canceled:
						return ErrCallCancel
					case context.DeadlineExceeded:
						return ErrCallTimeout
					default:
						return err
					}
				default:
				}

			}
		} else {
			req := reqMessage.Encode()
			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]
			callCtx := &callContext{
				respReceiver: ret,
				respC:        make(chan error),
			}
			for {
				pending.Store(reqMessage.Seq, callCtx)
				var pdErr error
				remoteAddr, err := c.placementDriver.GetPlacement(ctx, identity)
				if err != nil {
					pdErr = err
					time.Sleep(time.Millisecond * 10)
				} else if err = c.node.SendBinMessageWithContext(ctx, remoteAddr, Actor_request, req); err == nil {
					//等待响应
					select {
					case err := <-callCtx.respC:
						switch err := err.(type) {
						case pd.ErrorRedirect:
							c.placementDriver.ResetPlacementCache(identity, err.Addr)
							if err.Addr.Empty() {
								time.Sleep(time.Millisecond * 10)
							}
						case error:
							if err == ErrCallRetry {
								time.Sleep(RetryInterval)
							} else {
								return err
							}
						default:
							return nil
						}
					case <-ctx.Done():
						pending.Delete(reqMessage.Seq)
						err = ctx.Err()
						switch err {
						case context.Canceled:
							return ErrCallCancel
						case context.DeadlineExceeded:
							return ErrCallTimeout
						default:
							return err
						}
					}
				} else {
					switch err {
					case clustergo.ErrInvaildNode:
						c.placementDriver.ResetPlacementCache(identity, addr.LogicAddr(0))
					case clustergo.ErrPendingQueueFull, netgo.ErrSendQueueFull, netgo.ErrPushToSendQueueTimeout:
						time.Sleep(time.Millisecond * 10)
					default:
						pending.Delete(reqMessage.Seq)
						return err
					}
				}
				//检查是否可以执行重试
				select {
				case <-ctx.Done():
					pending.Delete(reqMessage.Seq)
					if err == ErrCallRetry {
						return err
					}
					if pdErr != nil {
						logger.Errorf("call grain:%s timeout with pd error:%v", identity, pdErr)
					}
					err = ctx.Err()
					switch err {
					case context.Canceled:
						return ErrCallCancel
					case context.DeadlineExceeded:
						return ErrCallTimeout
					default:
						return err
					}
				default:
				}
			}
		}
	}
}
