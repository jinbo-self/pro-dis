package metaserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/jinbo-self/project5-myproject/src/protos"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const ALL_SHARDS = 6                                       //返回地址总数量，也是客户端切片数量
var ALL_SIZE = [6]float64{123, 321, 445, 657, 5465, 31231} //假定磁盘容量不同，因为我这只有一台机器

type meta struct {
	protos.UnimplementedMetaServerServer
	addr           map[int]string //各个数据服务的地址
	mutx           sync.Mutex
	DataServerSize map[string]float64 //存储各个data服务的剩余磁盘量
	address        string             //本地监听的地址
	metaFile       *leveldb.DB        //存储文件的元数据信息，k为文件名，v为地址，hash值，等等
}

//kv存储中的k，解码器只支持单字符命名
type k struct {
	A string //账户名
	N string //文件名
}

//kv存储中的v
type v struct {
	V int64                      //版本号
	H map[int64]map[int64]string //hash存储，键值对为[版本号],分片hash值，[版本号][分片id]hash
	A map[int64]map[int64]string //文件存储的地址,考虑到可能不同文件在不同数据节点上
	S map[int64]int64            //文件总大小，用来校验的
}

//监听datasever的心跳，如果没有，就删除
func (m *meta) HearHeart(ctx context.Context, id int) {
	logrus.Infof("start to heart :%v", m.addr[id])
	conn, err := grpc.Dial(m.addr[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Infof("did not connect: %v", err)
	}
	defer conn.Close()
	client := protos.NewDataNodeClient(conn)
	for {
		select {
		case <-ctx.Done():
			logrus.Infof("metaHeart stop the hearHeart")
			return
		case <-time.After(3 * time.Second):
			reply, HearErr := client.HearHeart(context.Background(), &protos.HeartRequest{})
			if HearErr != nil {
				logrus.Infof("failed to get HearHeart: %v", HearErr)
				m.mutx.Lock()
				delete(m.DataServerSize, m.addr[id])
				delete(m.addr, id)
				m.mutx.Unlock()
				return
			}
			m.mutx.Lock()
			m.DataServerSize[m.addr[id]] = float64(reply.Size)
			m.mutx.Unlock()
		}

	}

}

//为客户端选择一个数据服务，提供一个数据服务的地址，采用最大容量的方案，只返回容量最大的ALL_SHAREDS个地址
func (m *meta) ChooseDataServer(ctx context.Context, in *protos.MetaRequest) (*protos.MetaReply, error) {
	logrus.Info("start to choose a datanode address")
	var dataServers = make(map[float64]string)
	Size := make([]float64, 0)
	i := -1

	//这里假定磁盘容量是ALL_SIZE，而不是我们监听心跳获取的容量，因为设计时我电脑就只有一块硬盘
	for k, _ := range m.DataServerSize {
		i++
		dataServers[ALL_SIZE[i]] = k
		Size = append(Size, ALL_SIZE[i])
		if i == ALL_SHARDS {
			break
		}
	}

	sort.Float64s(Size)
	reAdd := make([]byte, 0)
	for i := 1; i <= ALL_SHARDS; i++ {
		ChooseDataSize := Size[len(Size)-i]
		Add := dataServers[ChooseDataSize] + ","
		reAdd = append(reAdd, []byte(Add)...)
	}

	reAdd = reAdd[:len(reAdd)-1]
	retAdd := string(reAdd)
	logrus.Info("successfully choose a datanode address")
	return &protos.MetaReply{Addr: retAdd}, nil
}

//元数据服务本地存储文件的元数据信息，由数据服务调用,执行del时也由客户端调用
func (m *meta) StoreStatus(ctx context.Context, in *protos.StoreStatusReply) (*protos.Status, error) {
	logrus.Info("start to store the metadata")
	m.mutx.Lock()
	defer m.mutx.Unlock()
	db, err := leveldb.OpenFile("path/to/db", nil)

	if err != nil {
		logrus.Infof("failed to open a leveldb database:%v", err)
		return &protos.Status{}, err
	}
	m.metaFile = db
	defer db.Close()

	tmp := "1"

	PutErr2 := m.metaFile.Put([]byte(in.GetPrehash()), []byte(tmp), nil) //存储文件分片前hash值的表，只是用来去重的
	if PutErr2 != nil {
		logrus.Infof("faile to put the meta to leveldb: %v", PutErr2)
		return &protos.Status{}, PutErr2
	}
	logrus.Info("successfully store the prehash")
	key := &k{
		A: in.GetAccount(),
		N: in.GetName(),
	}
	space1 := make(map[int64]map[int64]string)
	space2 := make(map[int64]map[int64]string)
	space3 := make(map[int64]int64)
	value := &v{
		V: 1,
		H: space1,
		A: space2,
		S: space3,
	}
	tmmHV := make(map[int64]string)
	tmmAV := make(map[int64]string)

	lv, lve := m.metaFile.Get(EncodeStruct(key), nil)
	if lve != nil {
		tmmHV[in.GetId()] = in.GetHash()
		tmmAV[in.GetId()] = in.GetAddr()
		value.H[value.V] = tmmHV
		value.A[value.V] = tmmAV
		value.S[value.V] = in.GetSize()
		m.metaFile.Put(EncodeStruct(key), EncodeStruct(value), nil)
	} else {

		levelV := v{}
		levelV = VDecodeStruct(lv, levelV)
		_, PutErr3 := m.metaFile.Get([]byte(in.GetPrehash()), nil) //查询是否已经存在文件切片前的hash,已经存在则版本不变
		if PutErr3 != nil {
			levelV.V++
		}

		//-1为删除标志，该版本hash置为空
		if in.GetVersion() == -1 {
			levelV.H[levelV.V] = nil
			m.metaFile.Delete([]byte(in.GetPrehash()), nil)

		} else {
			value.S[value.V] = in.GetSize()
			tmmHV = levelV.H[levelV.V]
			tmmAV = levelV.A[levelV.V]
			tmmHV[in.GetId()] = in.GetHash()
			tmmAV[in.GetId()] = in.GetAddr()
			levelV.H[levelV.V] = tmmHV
			levelV.A[levelV.V] = tmmAV
		}
		PutErr1 := m.metaFile.Put(EncodeStruct(key), EncodeStruct(levelV), nil)
		if PutErr1 != nil {
			logrus.Infof("faile to put the meta to leveldb: %v", PutErr1)
			return &protos.Status{}, PutErr1
		}

	}
	logrus.Info("successfult store the metadata")

	return &protos.Status{}, nil
}

//获取元数据信息，由客户端get调用或者put调用查看文件是否存在
func (m *meta) GetMetaInfo(ctx context.Context, in *protos.GetMetaInfoRequest) (*protos.StoreStatusReply, error) {

	logrus.Info("start to get the metainfo from levelDB")
	m.mutx.Lock()
	defer m.mutx.Unlock()
	db, err := leveldb.OpenFile("path/to/db", nil)
	if err != nil {
		logrus.Infof("failed to open a leveldb database:%v", err)
		return &protos.StoreStatusReply{}, err
	}
	m.metaFile = db
	defer db.Close()

	//调用查询文件是否存在
	if in.GetFlag() == 1 {
		logrus.Info("Check whether the file already exists")
		_, PutEr := m.metaFile.Get([]byte(in.GetHash()), nil)
		if PutEr != nil {
			logrus.Info("file not exist")
			return &protos.StoreStatusReply{}, nil
		}
		logrus.Info("file already exist")
		return &protos.StoreStatusReply{Hash: in.GetHash()}, nil
	}

	GetV := k{
		A: in.GetAccount(),
		N: in.GetFileName(),
	}

	LevelData, FileAddrErr := m.metaFile.Get(EncodeStruct(GetV), nil)
	if FileAddrErr != nil {
		logrus.Infof("can not find the FileMeta:%v", FileAddrErr)
		return &protos.StoreStatusReply{}, FileAddrErr
	}
	var Value v
	Value = VDecodeStruct(LevelData, Value)
	addrs := make([]byte, 0)
	hashs := make([]byte, 0)
	for i := 0; i < ALL_SHARDS; i++ {
		addrs = append(addrs, []byte(Value.A[Value.V][int64(i)])...)
		addrs = append(addrs, []byte(",")...)
		hashs = append(hashs, []byte(Value.H[Value.V][int64(i)])...)
		hashs = append(hashs, []byte(",")...)
	}
	addrs = addrs[:len(addrs)-1]
	hashs = hashs[:len(hashs)-1]
	if in.GetVersion() > 0 {
		logrus.Infof("successfully get the %vth version of the file", in.GetVersion())
		return &protos.StoreStatusReply{Hash: string(hashs), Addr: string(addrs)}, nil
	}

	logrus.Info("successfully get the latest version of the file")
	return &protos.StoreStatusReply{Hash: string(hashs), Addr: string(addrs), Size: Value.S[Value.V]}, nil
}

//初始化一个meta结构体，将数据服务地址存储到meta中
func NewMeta(addr []string, address string) *meta {
	addrMake := make(map[int]string, 0)
	size := make(map[string]float64, 0)
	for k, v := range addr {
		addrMake[k] = v
	}
	return &meta{addr: addrMake, address: address, DataServerSize: size}
}

//一个编码器，将结构体转化为[]byte，方便kv存储
func EncodeStruct(rfState any) []byte {
	var bytesState bytes.Buffer
	enc := gob.NewEncoder(&bytesState)
	err := enc.Encode(rfState)
	if err != nil {
		logrus.Info("encode struct failed")
		return nil
	}
	return bytesState.Bytes()
}

//k解码器，将[]byte转化为结构体k，方便kv读取
func KDecodeStruct(in []byte, res k) k {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	err := dec.Decode(&res)
	fmt.Println(res)
	if err != nil {
		logrus.Info("encode struct failed")
		return k{}
	}
	return res
}

//V解码器，将[]byte转化为结构体V，方便kv读取
func VDecodeStruct(in []byte, res v) v {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	err := dec.Decode(&res)
	fmt.Println(res)
	if err != nil {
		logrus.Info("decode struct failed")
		return v{}
	}
	return res
}

var wg sync.WaitGroup

//初始化，创建本地监听
func (s *meta) Setup() {
	defer wg.Done()
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		logrus.Infof("failed to listen:%v", err)
	}
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(1024 * 1024 * 12),
		grpc.MaxSendMsgSize(1024 * 1024 * 12),
	}
	server := grpc.NewServer(options...)
	protos.RegisterMetaServerServer(server, s)
	server.Serve(lis)
}
