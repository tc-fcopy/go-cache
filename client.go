package gocache

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	pb "github.com/tc-fcopy/go-cache/pb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type Client struct {
	addr    string
	svcName string
	etcdCli *clientv3.Client
	conn    *grpc.ClientConn
	grpcCli pb.GoCacheClient
}

var _ Peer = (*Client)(nil)

func NewClient(addr string, svcName string, etcdCli *clientv3.Client) (*Client, error) {
	var err error
	if etcdCli == nil {
		etcdCli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("create etcd client failed: %v", err)
		}
	}
	// 1. 创建10秒超时上下文，管控整体建连超时，必须defer cancel释放资源
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 2. 新版gRPC核心：NewClient替代废弃的Dial，原DialOption完全复用
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // 测试用非加密传输
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),     // 调用时等待连接就绪，原逻辑不变
	)
	if err != nil {
		return nil, fmt.Errorf("grpc new client failed: %v", err)
	}

	// 3. 核心修复：无参调用Connect，触发建连动作（该版本API无参、无返回值）
	conn.Connect()

	// 4. 阻塞等待连接状态变更，实现「10秒阻塞建连」核心需求
	// 入参：ctx=10秒超时，targetState=Connecting（等待从「连接中」变为其他状态）
	// 返回false = 超时/上下文取消；返回true = 状态发生变更
	if !conn.WaitForStateChange(ctx, connectivity.Connecting) {
		return nil, fmt.Errorf("connect to grpc server %s timeout (10s), current state: %s",
			addr, conn.GetState().String())
	}

	// 5. 二次兜底校验：确保连接最终处于「就绪」状态（可正常调用）
	if conn.GetState() != connectivity.Ready {
		return nil, fmt.Errorf("grpc server %s connect failed, final state: %s",
			addr, conn.GetState().String())
	}

	// 初始化protobuf自动生成的gRPC业务客户端
	grpcClient := pb.NewGoCacheClient(conn)

	// 封装Client实例并返回
	client := &Client{
		addr:    addr,
		svcName: svcName,
		etcdCli: etcdCli,
		conn:    conn,
		grpcCli: grpcClient,
	}
	return client, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) Get(group string, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.grpcCli.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("grpc get failed: %v", err)
	}
	return resp.GetValue(), nil
}
func (c *Client) Set(ctx context.Context, group string, key string, value []byte) error {
	resp, err := c.grpcCli.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("grpc set failed: %v", err)
	}
	logrus.Infof("grpc set request resp:%v", resp)
	return nil
}

func (c *Client) Delete(group string, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := c.grpcCli.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})

	if err != nil {
		return false, fmt.Errorf("grpc delete failed: %v", err)
	}
	return resp.GetValue(), err
}
