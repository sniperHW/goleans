all:
	go build -o discoverySvr discovery/discovery.go 
	go build -o pdSvr placement/server/main.go 
	go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=ignore" -o node1.1.1 node1.1.1.go 
	go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=ignore" -o node1.2.1 node1.2.1.go