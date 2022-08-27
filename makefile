.PHONY: all hello SDSS-ctl proto clean

SDSS-ctl:
	go build -o bin/SDSS-ctl src/ctl/main.go

all: proto
	go build -o bin/metaserver.exe metaserver/main.go
	go build -o bin/dataserver.exe dataserver/main.go
	go build -o bin/client.exe client/main.go

hello: proto
	go build -o bin/hello_server cmsrd/hello/server/main.go
	go build -o bin/hello_client cmd/hello/client/main.go

proto:
	go generate src/protos/gen.go

clean:
	rm bin/*
