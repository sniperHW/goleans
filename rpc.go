package goleans

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/goleans/pd"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/netgo"
	"google.golang.org/protobuf/proto"
)

const (
	lenSeq                = 8
	lenOneWay             = 1
	lenMethod             = 2
	lenPidLen             = 2
	lenErrCode            = 2
	lenArg                = 4
	lenReqHdr             = lenSeq + lenOneWay + lenMethod + lenPidLen
	lenRspHdr             = lenSeq + lenErrCode
	lenAddr               = 4
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
	To     pd.Pid
	Arg    []byte
	arg    proto.Message
}

type ResponseMsg struct {
	Seq          uint64
	ErrCode      int
	RedirectAddr int
	Ret          []byte
}

func (r RequestMsg) GetArg() interface{} {
	return r.arg
}

func (req *RequestMsg) Encode() []byte {

	buff := make([]byte, lenReqHdr, lenReqHdr+len(req.To)+len(req.Arg))

	binary.BigEndian.PutUint64(buff, req.Seq)

	if req.Oneway {
		buff[lenSeq] = byte(1)
	}

	binary.BigEndian.PutUint16(buff[lenSeq+lenOneWay:], req.Method)

	binary.BigEndian.PutUint16(buff[lenSeq+lenOneWay+lenMethod:], uint16(len(req.To)))

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

	if buffLen-r < lenPidLen {
		return errors.New("invaild request packet")
	}
	lenIdentity := int(binary.BigEndian.Uint16(buff[r:]))
	r += lenPidLen

	if buffLen-r < lenIdentity {
		return errors.New("invaild request packet")
	}
	req.To = pd.Pid(buff[r : r+lenIdentity])
	r += lenIdentity

	if buffLen-r > 0 {
		req.Arg = make([]byte, 0, buffLen-r)
		req.Arg = append(req.Arg, buff[r:]...)
	}

	return nil
}

func (resp *ResponseMsg) Encode() (buff []byte) {
	if resp.ErrCode == ErrCodeRedirect {
		buff = make([]byte, lenRspHdr+lenAddr)
		binary.BigEndian.PutUint64(buff, resp.Seq)
		binary.BigEndian.PutUint16(buff[lenSeq:], uint16(resp.ErrCode))
		binary.BigEndian.PutUint32(buff[lenSeq+lenErrCode:], uint32(resp.RedirectAddr))
	} else {
		buff = make([]byte, lenRspHdr, lenRspHdr+len(resp.Ret))
		binary.BigEndian.PutUint64(buff, resp.Seq)
		binary.BigEndian.PutUint16(buff[lenSeq:], uint16(resp.ErrCode))
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
	req     *RequestMsg
	replyed int32
	from    addr.LogicAddr
	node    *clustergo.Node
	hook    func(*RequestMsg)
}

func (r *Replyer) SetReplyHook(hook func(_ *RequestMsg)) {
	r.hook = hook
}

func (r *Replyer) redirect(redirectAddr addr.LogicAddr) {
	if r.req.Oneway {
		//通告对端identity不在当前节点
		buff := make([]byte, lenAddr, len(r.req.To)+lenAddr)
		binary.BigEndian.PutUint32(buff, uint32(redirectAddr))
		buff = append(buff, []byte(r.req.To)...)
		r.node.SendBinMessage(r.from, Actor_notify_redirect, buff)
	} else if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq:          r.req.Seq,
			ErrCode:      ErrCodeRedirect,
			RedirectAddr: int(redirectAddr),
		}
		if err := r.node.SendBinMessage(r.from, Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

func (r *Replyer) error(errCode int) {
	if !r.req.Oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		resp := &ResponseMsg{
			Seq:     r.req.Seq,
			ErrCode: errCode,
		}
		if err := r.node.SendBinMessage(r.from, Actor_response, resp.Encode()); err != nil {
			logger.Errorf("send actor rpc response to (%s) error:%s\n", r.from.String(), err.Error())
		}
	}
}

func (r *Replyer) callHook() {
	if r.hook == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("%s ", fmt.Errorf(fmt.Sprintf("%v: %s", r, debug.Stack())))
		}
	}()
	r.hook(r.req)
}

func (r *Replyer) Reply(ret proto.Message) {
	if !r.req.Oneway && atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		r.callHook()
		resp := &ResponseMsg{
			Seq: r.req.Seq,
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

// ///Client
var respWaitPool = sync.Pool{
	New: func() interface{} { return make(chan *ResponseMsg, 1) },
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
		ctx.(chan *ResponseMsg) <- resp
	} else {
		logger.Infof("onResponse with no reqContext:%d", resp.Seq)
	}
}

func rpcError(err error) error {
	switch err {
	case context.Canceled:
		return ErrCallCancel
	case context.DeadlineExceeded:
		return ErrCallTimeout
	default:
		return err
	}
}

func (c *RPCClient) Call(ctx context.Context, identity pd.Pid, method uint16, arg proto.Message, ret proto.Message) error {
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
				remoteAddr, err := c.placementDriver.GetPlacement(ctx, identity)
				if err != nil {
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
						return rpcError(err)
					}
				}

				select {
				case <-ctx.Done():
					return rpcError(ctx.Err())
				default:
				}
			}
		} else {
			req := reqMessage.Encode()
			pending := &c.pendingCall[int(reqMessage.Seq)%len(c.pendingCall)]
			wait := respWaitPool.Get().(chan *ResponseMsg)
			for {
				//Store不能像rpcgo一样移到for前面，因为case resp := <-wait:之后可能再次重发，如果移动到for前面将丢失上下文
				pending.Store(reqMessage.Seq, wait)
				remoteAddr, err := c.placementDriver.GetPlacement(ctx, identity)
				if err != nil {
					time.Sleep(time.Millisecond * 10)
				} else if err = c.node.SendBinMessageWithContext(ctx, remoteAddr, Actor_request, req); err == nil {
					//等待响应
					select {
					case resp := <-wait:
						respWaitPool.Put(wait)
						switch resp.ErrCode {
						case ErrCodeOk:
							return proto.Unmarshal(resp.Ret, ret)
						case ErrCodeRedirect:
							c.placementDriver.ResetPlacementCache(identity, addr.LogicAddr(resp.RedirectAddr))
							if resp.RedirectAddr == 0 {
								time.Sleep(time.Millisecond * 10)
							}
						case ErrCodeRetryAgain:
							err = ErrCallRetry
							time.Sleep(RetryInterval)
						default:
							return getDescByErrCode(uint16(resp.ErrCode))
						}
					case <-ctx.Done():
						if _, ok := pending.LoadAndDelete(reqMessage.Seq); ok {
							respWaitPool.Put(wait)
						}
						return rpcError(ctx.Err())
					}
				} else {
					//没有发出去
					switch err {
					case clustergo.ErrInvaildNode:
						c.placementDriver.ResetPlacementCache(identity, addr.LogicAddr(0))
					case clustergo.ErrPendingQueueFull, netgo.ErrSendQueueFull, netgo.ErrPushToSendQueueTimeout:
						time.Sleep(time.Millisecond * 10)
					default:
						pending.Delete(reqMessage.Seq)
						respWaitPool.Put(wait)
						return err
					}
				}
				//检查是否可以执行重试
				select {
				case <-ctx.Done():
					pending.Delete(reqMessage.Seq)
					respWaitPool.Put(wait)
					if err == ErrCallRetry {
						return err
					} else {
						return rpcError(ctx.Err())
					}
				default:
				}
			}
		}
	}
}
