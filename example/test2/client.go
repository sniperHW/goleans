package main

import (
	"fmt"
	"net"
	"time"

	"github.com/sniperHW/goleans/example/codec"

	"github.com/sniperHW/netgo"
)

func main() {
	dialer := &net.Dialer{}
	var s netgo.Socket
	codecc := codec.NewCodec()

	for {
		if conn, err := dialer.Dial("tcp", "127.0.0.1:18113"); nil != err {
			time.Sleep(time.Second)
		} else {
			conn.Write([]byte("sniperHW12345678"))
			s = netgo.NewTcpSocket(conn.(*net.TCPConn), codecc)
			break
		}
	}

	for i := 1; ; i++ {
		req := &codec.Message{
			Cmd:     1,
			Seq:     uint32(i),
			Payload: []byte(fmt.Sprintf("hello from client:%d", i)),
		}
		b := codecc.Encode(req)

		n, err := s.Send(b, time.Now().Add(time.Second))

		if err != nil || n != len(b) {
			fmt.Println(n, err)
			return
		}

		resp, err := s.Recv(time.Now().Add(time.Second))
		if err != nil {
			fmt.Println(n, err)
			return
		}

		fmt.Println(string(codecc.Decode(resp).Payload))

		time.Sleep(time.Second)
	}
}
