package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/sniperHW/clustergo"
	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/goleans/pd"
	"github.com/sniperHW/netgo"
	"google.golang.org/protobuf/proto"
)

const (
	LenSeq                = 8
	LenOneWay             = 1
	LenMethod             = 2
	LenPidLen             = 2
	LenErrCode            = 2
	LenArg                = 4
	LenReqHdr             = LenSeq + LenOneWay + LenMethod + LenPidLen
	LenRspHdr             = LenSeq + LenErrCode
	LenAddr               = 4
	Actor_request         = 11311
	Actor_response        = 11312
	Actor_notify_redirect = 11313
)

var (
	RetryInterval = 100 * time.Millisecond //遇到ErrCodeRetryAgain错误时的重试间隔
)

type RequestMsg struct {
	Seq      uint64
	Method   uint16
	Oneway   bool
	To       string
	Arg      []byte
	Argument proto.Message
}

type ResponseMsg struct {
	Seq          uint64
	ErrCode      int
	RedirectAddr int
	Ret          []byte
}

func (req *RequestMsg) Encode() []byte {

	buff := make([]byte, LenReqHdr, LenReqHdr+len(req.To)+len(req.Arg))

	binary.BigEndian.PutUint64(buff, req.Seq)

	if req.Oneway {
		buff[LenSeq] = byte(1)
	}

	binary.BigEndian.PutUint16(buff[LenSeq+LenOneWay:], req.Method)

	binary.BigEndian.PutUint16(buff[LenSeq+LenOneWay+LenMethod:], uint16(len(req.To)))

	buff = append(buff, []byte(req.To)...)

	buff = append(buff, req.Arg...)

	return buff
}

func (req *RequestMsg) Decode(buff []byte) error {
	r := 0
	buffLen := len(buff)
	if buffLen-r < LenSeq {
		return errors.New("invaild request packet")
	}
	req.Seq = binary.BigEndian.Uint64(buff[r:])

	r += LenSeq
	if buffLen-r < LenOneWay {
		return errors.New("invaild request packet")
	}
	if buff[r] == byte(1) {
		req.Oneway = true
	}
	r += LenOneWay
	if buffLen-r < LenMethod {
		return errors.New("invaild request packet")
	}
	req.Method = binary.BigEndian.Uint16(buff[r:])
	r += LenMethod

	if buffLen-r < LenPidLen {
		return errors.New("invaild request packet")
	}
	lenIdentity := int(binary.BigEndian.Uint16(buff[r:]))
	r += LenPidLen

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
	if resp.ErrCode == ErrCodeRedirect {
		buff = make([]byte, LenRspHdr+LenAddr)
		binary.BigEndian.PutUint64(buff, resp.Seq)
		binary.BigEndian.PutUint16(buff[LenSeq:], uint16(resp.ErrCode))
		binary.BigEndian.PutUint32(buff[LenSeq+LenErrCode:], uint32(resp.RedirectAddr))
	} else {
		buff = make([]byte, LenRspHdr, LenRspHdr+len(resp.Ret))
		binary.BigEndian.PutUint64(buff, resp.Seq)
		binary.BigEndian.PutUint16(buff[LenSeq:], uint16(resp.ErrCode))
		buff = append(buff, resp.Ret...)
	}
	return buff
}

func (resp *ResponseMsg) Decode(buff []byte) error {
	r := 0
	buffLen := len(buff)
	if buffLen-r < LenSeq {
		return errors.New("invaild response packet")
	}
	resp.Seq = binary.BigEndian.Uint64(buff[r:])
	r += LenSeq

	if buffLen-r < LenErrCode {
		return errors.New("invaild response packet")
	}

	resp.ErrCode = int(binary.BigEndian.Uint16(buff[r:]))
	r += LenErrCode

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

type Replyer interface {
	SetReplyHook(func(*RequestMsg))
	Reply(ret proto.Message)
}

type MethodCaller struct {
	ArgType reflect.Type
	Fn      reflect.Value
}

// 接受的method func(context.Context, *Replyer,proto.Message)
func MakeMethodCaller(method interface{}) (*MethodCaller, error) {
	if method == nil {
		return nil, errors.New("method is nil")
	}

	fnType := reflect.TypeOf(method)
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("method should have type func(context.Contex,Replyer,proto.Message)")
	}

	if fnType.NumIn() != 3 {
		return nil, errors.New("method should have type func(context.Contex,Replyer,proto.Message)")
	}

	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, errors.New("method should have type func(context.Contex,Replyer,proto.Message)")
	}

	if fnType.In(1) != reflect.TypeOf((*Replyer)(nil)).Elem() {
		return nil, errors.New("method should have type func(context.Contex,Replyer,proto.Message)")
	}

	if !fnType.In(2).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
		return nil, errors.New("method should have type func(context.Contex,Replyer,proto.Message)")
	}

	caller := &MethodCaller{
		ArgType: fnType.In(2).Elem(),
		Fn:      reflect.ValueOf(method),
	}

	return caller, nil
}

// ///Client
var respWaitPool = sync.Pool{
	New: func() interface{} { return make(chan *ResponseMsg, 1) },
}

type Client struct {
	sync.Mutex
	nextSequence    uint32
	timestamp       uint32
	timeOffset      uint32
	startTime       time.Time
	pendingCall     [32]sync.Map
	node            *clustergo.Node
	placementDriver pd.PlacementDriver
}

func NewClient(node *clustergo.Node, placementDriver pd.PlacementDriver) *Client {
	return &Client{
		timeOffset:      uint32(time.Now().Unix() - time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).Unix()),
		startTime:       time.Now(),
		node:            node,
		placementDriver: placementDriver,
	}
}

func (c *Client) getTimeStamp() uint32 {
	return uint32(time.Since(c.startTime)/time.Second) + c.timeOffset
}

func (c *Client) makeSequence() (seq uint64) {
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

func (c *Client) OnRPCResponse(resp *ResponseMsg) {
	if ctx, ok := c.pendingCall[int(resp.Seq)%len(c.pendingCall)].LoadAndDelete(resp.Seq); ok {
		ctx.(chan *ResponseMsg) <- resp
	} else {
		fmt.Println("onResponse with no reqContext:", resp.Seq)
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

func (c *Client) CallWithTimeout(pid string, method uint16, arg proto.Message, ret proto.Message, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return c.Call(ctx, pid, method, arg, ret)
}

func (c *Client) Call(ctx context.Context, pid string, method uint16, arg proto.Message, ret proto.Message) error {
	if b, err := proto.Marshal(arg); err != nil {
		return err
	} else {
		reqMessage := &RequestMsg{
			To:     pid,
			Seq:    c.makeSequence(),
			Method: method,
			Arg:    b,
		}
		if ret == nil {
			reqMessage.Oneway = true
			req := reqMessage.Encode()
			for {
				remoteAddr, err := c.placementDriver.GetPlacement(ctx, pid)
				if err != nil {
					time.Sleep(time.Millisecond * 10)
				} else if err = c.node.SendBinMessageWithContext(ctx, remoteAddr, Actor_request, req); err == nil {
					return nil
				} else {
					switch err {
					case clustergo.ErrInvaildNode:
						c.placementDriver.ResetPlacementCache(pid, addr.LogicAddr(0))
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
				remoteAddr, err := c.placementDriver.GetPlacement(ctx, pid)
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
							c.placementDriver.ResetPlacementCache(pid, addr.LogicAddr(resp.RedirectAddr))
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
						c.placementDriver.ResetPlacementCache(pid, addr.LogicAddr(0))
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
