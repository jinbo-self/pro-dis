package main

import (
	"flag"

	"github.com/jinbo-self/project5-myproject/src/dataserver"
	"github.com/sirupsen/logrus"
)

var dataServerAddr = flag.String("addr", "localhost:9000", "input this datanode address")
var metaAddr = flag.String("m", "", "a meta addr to connect")

func main() {
	flag.Parse()
	logrus.Info("start this dataserver")
	dataserver.NewDataServer(*dataServerAddr, *metaAddr).Setup()
}
