package dataserver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"

	"github.com/jinbo-self/project5-myproject/src/protos"
	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type dataServer struct {
	protos.UnimplementedDataNodeServer
	addr     string //本地监听的地址
	metaaddr string //元数据服务的地址，每次存储成功都需要返回文件的元数据
	path     string //data本地存储路径

}

//返回给meta的元数据信息
type ReturnMetaInfo struct {
	FileName string //文件名
	DataAddr string //本地地址
	Account  string //账户名
	Hash     string //hash值
	preHash  string //文件切片前的hash，由client传来
	id       int64  //分片id
	size     int64  //原文件大小
}

//写入文件
func (s *dataServer) WriteFile(ctx context.Context, in *protos.WriteRequest) (*protos.WriteReply, error) {
	//先进行hash校验
	logrus.Info("start to check the hash")
	hash := fmt.Sprintf("%x", (sha256.Sum256(in.GetData())))
	if strings.Compare(hash, in.GetHash()) != 0 {
		logrus.Info("Failed to verify file hash value")
		return &protos.WriteReply{}, fmt.Errorf("failed to verify %v hash value,please try to put again", in.GetFilename())
	}
	logrus.Info("successfully check the hash")
	//再进行账户检查，由于没有账号库，这里只判断是否为空,文件名也不能为空
	if in.GetAccount() == "" || in.GetFilename() == "" {
		logrus.Info("please input correct account and password or filename")
		return &protos.WriteReply{}, fmt.Errorf("please input correct account and password")
	}
	filename := in.GetHash()
	logrus.Infof("start to create the file: %v", filename)
	FilePath := s.path + filename
	file, e := os.Create(FilePath)
	if e != nil {
		logrus.Infof("failed to create a file:%v", e)
		return &protos.WriteReply{}, e
	}

	defer file.Close()

	var metaInf *ReturnMetaInfo
	_, FileStatusErr := file.Write(in.GetData())

	if FileStatusErr != nil {
		logrus.Info("failed to store the file")
	} else {
		metaInf = &ReturnMetaInfo{FileName: in.GetFilename(),
			DataAddr: s.addr,
			Account:  in.GetAccount(),
			Hash:     in.GetHash(),
			preHash:  in.GetPrehash(),
			id:       in.GetId(),
			size:     in.GetSize()}
	}
	logrus.Info("successfult store the file")
	s.ReturnMeta(metaInf)

	return &protos.WriteReply{}, nil
}

//读取文件
func (s *dataServer) ReadFile(ctx context.Context, in *protos.ReadRequest) (*protos.ReadReply, error) {
	filename := in.GetHash()
	logrus.Infof("start to open the file: %v", filename)
	f, e := os.ReadFile(s.path + filename)

	if e != nil {
		logrus.Infof("read a nil file:%v", e)
		return &protos.ReadReply{}, e
	}
	logrus.Infof("successffuly read the file: %v", filename)
	return &protos.ReadReply{Data: f}, nil
}

//向元数据服务返回存储成功的文件元数据信息
func (s *dataServer) ReturnMeta(metaInfo *ReturnMetaInfo) {
	logrus.Infof("start to store the meta info to metaServe:%v", s.metaaddr)
	conn, err := grpc.Dial(s.metaaddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Infof("failed to connect the metaServer:%v", err)
	}
	client := protos.NewMetaServerClient(conn)
	_, StatusErr := client.StoreStatus(context.Background(), &protos.StoreStatusReply{
		Name:    metaInfo.FileName,
		Addr:    metaInfo.DataAddr,
		Hash:    metaInfo.Hash,
		Account: metaInfo.Account,
		Prehash: metaInfo.preHash,
		Id:      metaInfo.id,
		Size:    metaInfo.size})

	if StatusErr != nil {
		logrus.Info("Call function StoreStatus from metaServer failed")
		return
	}
	logrus.Info("successfult return the metainfo")
}

//监听心跳,并返回剩余磁盘量
func (s *dataServer) HearHeart(ctx context.Context, in *protos.HeartRequest) (*protos.HeartResponse, error) {
	mu, _ := os.Getwd()
	usage := du.NewDiskUsage(mu)
	return &protos.HeartResponse{Size: usage.Available()}, nil
}

func NewDataServer(addr, metaaddr, path string) *dataServer {

	return &dataServer{
		addr:     addr,
		metaaddr: metaaddr,
		path:     path,
	}
}

//定位文件，判断文件是否存在
func (s *dataServer) LocalObject(ctx context.Context, in *protos.LocalRequest) (*protos.LocalReply, error) {
	_, err := os.Stat(s.path + in.GetFilename())
	if err != nil {
		logrus.Infof("failed to locate the file:%v", err)
		return &protos.LocalReply{Success: ""}, err
	}
	return &protos.LocalReply{Success: s.addr}, nil
}

//初始化监听服务
func (s *dataServer) Setup() {

	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		logrus.Infof("failed to listen the program")
	}
	idleClosed := make(chan struct{})
	go func() {
		sign := make(chan os.Signal, 1)
		signal.Notify(sign, os.Interrupt)
		<-sign

		lis.Close()
		close(idleClosed)
		close(sign)

	}()

	server := grpc.NewServer()
	protos.RegisterDataNodeServer(server, s)
	server.Serve(lis)
	<-idleClosed //阻塞

}
