package main

import (
	"context"
	"flag"
	"strings"
	"sync"

	"github.com/jinbo-self/project5-myproject/src/metaserver"
	"github.com/sirupsen/logrus"
)

var dataServerAddr = flag.String("s", "localhost:9000,localhost:9001,localhost:9002", "hear this dataserver")
var localAddress = flag.String("m", "localhost:12345", "create a listener for outside")
var wg sync.WaitGroup

func main() {
	flag.Parse()
	dataServers := strings.Split(*dataServerAddr, ",")
	metas := metaserver.NewMeta(dataServers, *localAddress)
	wg.Add(1)
	go metas.Setup()

	for i := 0; i < len(dataServers); i++ {
		logrus.Infof("start to listen:%v", dataServers[i])
		go metas.HearHeart(context.Background(), i)
	}
	wg.Wait()
}
