package main

import (
	"flag"
	"fmt"

	"github.com/jinbo-self/project5-myproject/src/dataserver"
	"github.com/sirupsen/logrus"
)

var dataServerAddr = flag.String("addr", "localhost:9000", "input this datanode address")
var metaAddr = flag.String("m", "", "a meta addr to connect")
var storePath = flag.String("p", "", "a path to store the data")

func main() {
	flag.Parse()
	logrus.Info("start this dataserver", *dataServerAddr)
	fmt.Println(*dataServerAddr, *metaAddr, *storePath)
	dataserver.NewDataServer(*dataServerAddr, *metaAddr, *storePath).Setup()
}
