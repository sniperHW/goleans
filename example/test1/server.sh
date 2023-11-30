#! /bin/sh
export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore
./discoverySvr --addr=localhost:6210 goleans > /dev/null 2>&1 &
./pdSvr --addr=localhost:6211 goleans > /dev/null 2>&1 &
./node1.1.1 --pdAddr=localhost:6211 --discoveryAddr=localhost:6210 goleans > /dev/null 2>&1 &