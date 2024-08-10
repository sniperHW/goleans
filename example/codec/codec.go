package codec

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/sniperHW/clustergo/codec/buffer"
	"github.com/sniperHW/netgo"
)

const (
	sizeLen = 4
	sizeCmd = 2
	sizeSeq = 4
	hrdSize = sizeLen + sizeCmd + sizeSeq
)

var (
	MaxPacketSize = 1024 * 4
)

type Message struct {
	Cmd     uint16
	Seq     uint32
	Payload []byte
}

// 接收一个length|payload类型的数据包
type LengthPayloadPacketReceiver struct {
	MaxPacketSize int
	Buff          []byte
	w             int
	r             int
}

func (ss *LengthPayloadPacketReceiver) read(readable netgo.ReadAble, deadline time.Time) (int, error) {
	if err := readable.SetReadDeadline(deadline); err != nil {
		return 0, err
	} else {
		return readable.Read(ss.Buff[ss.w:])
	}
}

// 接收一个length|payload类型的数据包,返回payload
func (ss *LengthPayloadPacketReceiver) Recv(readable netgo.ReadAble, deadline time.Time) (pkt []byte, err error) {
	for {
		unpackSize := ss.w - ss.r
		if unpackSize >= sizeLen {
			//fmt.Println(unpackSize, ss.Buff[ss.r:ss.w])
			payload := int(binary.BigEndian.Uint32(ss.Buff[ss.r:]))
			totalSize := payload + sizeLen
			if payload == 0 {
				return nil, fmt.Errorf("zero payload")
			} else if totalSize > ss.MaxPacketSize {
				return nil, fmt.Errorf("packet too large:%d", totalSize)
			} else if totalSize <= unpackSize {
				ss.r += sizeLen
				pkt := ss.Buff[ss.r : ss.r+payload]
				ss.r += payload
				if ss.r == ss.w {
					ss.r = 0
					ss.w = 0
				}
				return pkt, nil
			} else {
				if totalSize > cap(ss.Buff) {
					buff := make([]byte, totalSize)
					copy(buff, ss.Buff[ss.r:ss.w])
					ss.Buff = buff
				} else if ss.r > 0 {
					copy(ss.Buff, ss.Buff[ss.r:ss.w])
				}
				ss.w = ss.w - ss.r
				ss.r = 0
			}
		}

		var n int
		n, err = ss.read(readable, deadline)
		if n > 0 {
			ss.w += n
		}
		if nil != err {
			return
		}
	}
}

type CSCodec struct {
	LengthPayloadPacketReceiver
	reader buffer.BufferReader
}

func NewCodec() *CSCodec {
	return &CSCodec{
		LengthPayloadPacketReceiver: LengthPayloadPacketReceiver{
			Buff:          make([]byte, 4096),
			MaxPacketSize: MaxPacketSize,
		},
		reader: buffer.NewReader(binary.BigEndian, nil),
	}
}

func (cs *CSCodec) Encode(o *Message) []byte {

	totalLen := sizeCmd + sizeSeq + len(o.Payload)
	b := make([]byte, hrdSize, hrdSize+len(o.Payload))
	//写payload大小
	binary.BigEndian.PutUint32(b, uint32(totalLen))
	//写cmd
	binary.BigEndian.PutUint16(b[4:], o.Cmd)
	//写seq
	binary.BigEndian.PutUint32(b[6:], o.Seq)
	return append(b, o.Payload...)

}

func (cs *CSCodec) Decode(payload []byte) *Message {
	cs.reader.Reset(payload)
	msg := &Message{}
	msg.Cmd = cs.reader.GetUint16()
	msg.Seq = cs.reader.GetUint32()
	remain := cs.reader.GetAll()
	msg.Payload = make([]byte, len(remain))
	copy(msg.Payload, remain)
	return msg
}
