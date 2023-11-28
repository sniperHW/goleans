#! /bin/sh
export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore
./node1.2.1 --pdAddr=localhost:6211 --discoveryAddr=localhost:6210 example