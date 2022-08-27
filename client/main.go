package main

import (
	"flag"

	"github.com/jinbo-self/project5-myproject/src/client"
)

var metaAddress = flag.String("m", "localhost:12345", "a meta address")
var command = flag.String("c", "", "a command method like put/get/del/ and so on")
var GetFileName = flag.String("f", "", "a filename to get or put,delete and so on")
var Account = flag.String("n", "12345", "use your account for a identifier,this account is unique,defalut 12345")
var Version = flag.Int64("v", 0, "Select get a version of the file, default latest version") //-1是删除标志，-2是查询标志,0默认最新

func main() {
	flag.Parse()
	cli := client.Setup(*metaAddress, *Account, *GetFileName, *Version)
	if *command == "put" {
		cli.ChooseDataServer()
		cli.Put()
	}
	if *command == "get" {
		cli.Get()
	}
	if *command == "del" {
		cli.Del()
	}

}
