#! /bin/sh
./discoverySvr --addr=localhost:6210 goleans > /dev/null 2>&1 &
./pdSvr --addr=localhost:6211 goleans > /dev/null 2>&1 &
#./gameserver --pdAddr=localhost:6211 --discoveryAddr=localhost:6210 goleans > /dev/null 2>&1 &
#./gateway --pdAddr=localhost:6211 --discoveryAddr=localhost:6210 goleans > /dev/null 2>&1 &