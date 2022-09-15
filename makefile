.PHONY: all hello SDSS-ctl proto clean

all: proto
	go build -o bin/metaserver metaserver/main.go
	go build -o bin/dataserver dataserver/main.go
	go build -o bin/client client/main.go

hello: proto
	go build -o bin/hello_server cmsrd/hello/server/main.go
	go build -o bin/hello_client cmd/hello/client/main.go

proto:
	go generate src/protos/gen.go

clean:
	rm bin/*
