package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jinbo-self/project5-myproject/src/protos"
	"github.com/klauspost/reedsolomon"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type client struct {
	addr     string //数据服务的地址
	metaAddr string //元数据服务的地址
	account  string //账户名，将作为kv存储的k结构中的一员
	filename string //文件名
	hash     string //文件hash值，会传给数据服务，被数据服务进行数据校验，然后作为文件名存储文件，然后由数据服务传递给元数据服务存储
	version  int64  //请求文件时的版本号，默认最新版本
	putHash  string //在put时调用hash传给元数据服务，查看该文件是否已经存在，若存在，返回puthash（与hash值相同，只是为了区分来源不同）
	Id       int64  //put时的id
	flag     int    //标志位
	size     int64  //文件总大小，校验时使用
}

const ALL_SHARDS = 6    //切片总数量，四个数据片，两个校验片，RS纠删码
const DADA_SHARDS = 4   //四个数据片
const PARITY_SHARDS = 2 //2个校验片
//执行put操作,先判断文件存不存在，防止过多占用数据磁盘
func (c *client) Put() {
	addres := strings.Split(c.addr, ",")
	AllFile, ferr := os.ReadFile(c.filename)
	if ferr != nil {
		logrus.Panic(ferr)
		return
	}
	size := len(AllFile)

	preHash := fmt.Sprintf("%x", sha256.Sum256(AllFile))
	logrus.Infof("大小为：%x", preHash)
	c.hash = preHash
	c.flag = 1
	c.GetAddrFromMeta()
	if c.putHash == c.hash && c.hash != "" {
		logrus.Info("file already existed")
		return
	}

	files := CodeFile(AllFile)
	for i := 0; i < ALL_SHARDS; i++ {
		c.addr = addres[i]
		logrus.Infof("start to connect: %v", c.addr)
		conn, err := grpc.Dial(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logrus.Panic("did not connect: %v", err)
		}
		defer conn.Close()
		client := protos.NewDataNodeClient(conn)
		fileName := c.filename
		file := files[i]
		c.hash = fmt.Sprintf("%x", sha256.Sum256(file))

		_, cerr := client.WriteFile(context.Background(), &protos.WriteRequest{
			Filename: fileName,
			Data:     file,
			Hash:     c.hash,
			Account:  c.account,
			Prehash:  preHash,
			Id:       int64(i),
			Size:     int64(size)})

		if cerr != nil {
			logrus.Panic(cerr)
			return
		}
		logrus.Infof("successfully put %v to %v", fileName, c.addr)
	}
}

//执行get操作，先从元数据服务拿到文件地址
func (c *client) Get() {
	Filename := c.filename
	c.GetAddrFromMeta()
	logrus.Info(c.hash)
	if c.hash == "" {
		logrus.Info("this file is not exist,please check your input")
		return
	}
	logrus.Infof("start to connect: %v", c.addr)
	adds := strings.Split(c.addr, ",")

	hashs := strings.Split(c.hash, ",")
	tmpFile := make([][]byte, 0)
	tmpFiles := make([]byte, 0)
	for i := 0; i < ALL_SHARDS; i++ {
		c.addr = adds[i]
		c.hash = hashs[i]
		conn, err := grpc.Dial(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logrus.Info("did not connect: %v", err)
			continue
		}
		defer conn.Close()
		client := protos.NewDataNodeClient(conn)
		file, ferr := client.ReadFile(context.Background(), &protos.ReadRequest{Hash: c.hash})
		if ferr != nil {
			logrus.Info(ferr)
			continue
		}

		tmpFile = append(tmpFile, file.GetData())
		tmpFiles = append(tmpFiles, file.GetData()...)

	}
	c.hash = fmt.Sprintf("%x", sha256.Sum256(tmpFiles[:c.size]))
	c.flag = 1
	c.GetAddrFromMeta()
	logrus.Infof("大小为：%x", c.hash)
	if c.putHash != c.hash {
		logrus.Info("文件获取失败，修复中")
		tmpFiles = Decoder(tmpFile)
		if tmpFiles == nil {
			return
		}
		logrus.Info("修复成功")
	}

	localFile, err := os.Create(Filename)
	if err != nil {
		logrus.Panic(err)
		return
	}
	localFile.Write(tmpFiles)
	logrus.Infof("successfully get %v", Filename)
}

//删除操作，为了防止误删，这里只是将元数据服务的最新版本hash置为0，
//即最新版本无法检索，但还是可以获取之前版本的data
func (c *client) Del() {
	logrus.Infof("start to delete the file:%v", c.filename)
	conn, e := grpc.Dial(c.metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		logrus.Panic("did not connect: %v", e)
		return
	}
	client := protos.NewMetaServerClient(conn)
	_, er := client.StoreStatus(context.Background(), &protos.StoreStatusReply{Account: c.account, Name: c.filename, Version: -1})
	if er != nil {
		logrus.Panic("del failed")
		return
	}
	logrus.Info("del successfully")
}

//从元数据服务选择一个数据节点来存储文件
func (c *client) ChooseDataServer() {
	logrus.Infof("start to get the data address from metaServer: %v", c.metaAddr)
	conn, err := grpc.Dial(c.metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		logrus.Panic(err)
		return
	}
	defer conn.Close()
	meta := protos.NewMetaServerClient(conn)

	reply, err := meta.ChooseDataServer(context.Background(), &protos.MetaRequest{})
	if err != nil {
		logrus.Panic(err)
		return
	}
	c.addr = reply.GetAddr()
	if c.addr != "" {
		logrus.Infof("successfully get the dataAddress: %v", c.addr)
	} else {
		logrus.Infof("get the dataAddress failed")
	}
}

//从元数据服务获取文件所在数据节点的地址和文件的hash,存储在client结构体中,也可用hash作为参数查询文件是否存在
func (c *client) GetAddrFromMeta() {
	FileName := c.filename
	logrus.Infof("start to get the %v address。the version is %v", FileName, c.version)
	conn, err := grpc.Dial(c.metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*12)))
	if err != nil {
		log.Panic(err)
		return
	}
	logrus.Infof("successfully to conncet the meta from :%v", c.metaAddr)
	MetaClient := protos.NewMetaServerClient(conn)

	//c.flag=1，查询是否存在同样文件,传文件hash就行

	if c.flag == 1 {

		reply, err := MetaClient.GetMetaInfo(context.Background(), &protos.GetMetaInfoRequest{Hash: c.hash, Flag: 1})
		if err != nil {
			log.Panic(err)
			return
		}

		c.putHash = reply.Hash
		return
	}
	reply, err := MetaClient.GetMetaInfo(context.Background(), &protos.GetMetaInfoRequest{FileName: FileName, Account: c.account, Version: c.version})
	if err != nil {
		log.Panic(err)
		return
	}
	c.addr = reply.GetAddr()
	c.hash = reply.GetHash()
	c.size = reply.GetSize()
}

//将数据分成几个切片，每个切片当作一个文件存放到各数据节点
func CodeFile(file []byte) [][]byte {
	var enc reedsolomon.Encoder
	enc, _ = reedsolomon.New(DADA_SHARDS, PARITY_SHARDS)

	shards, _ := enc.Split(file)
	enc.Encode(shards)
	return shards

}

//如果传输回来的文件数量大于DATA_SHARDS,小于ALL_SHARDS，则执行修复
func Decoder(file [][]byte) []byte {
	var enc reedsolomon.Encoder
	enc, _ = reedsolomon.New(4, 2)
	err := enc.Reconstruct(file)

	if err != nil {
		logrus.Info("修复失败，请重新下载")
		return nil
	}

	c := make([]byte, 0)
	for i := 0; i < DADA_SHARDS; i++ {
		c = append(c, file[i]...)
	}

	return c
}

//初始化一个client结构体，方便外部调用方法
func Setup(metaAddr, acc, name string, v int64) *client {
	return &client{addr: "", metaAddr: metaAddr, account: acc, filename: name, version: v}
}
