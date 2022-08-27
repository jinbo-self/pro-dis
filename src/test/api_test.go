package test

import (
	"testing"

	"github.com/jinbo-self/project5-myproject/client"
	"github.com/jinbo-self/project5-myproject/dataserver"
)

func TestSimplePut(t *testing.T) {
	go dataserver.NewDataServer(("localhost:9000")).Setup()
	go client.Put("新建文本文档.txt", "localhost:9000")
	go client.Get("新建文本文档.txt", "localhost:9000")
}
