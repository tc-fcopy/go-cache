// Package gocache 定义了基于 etcd 实现的分布式缓存服务相关核心结构
package gocache

import (
	"crypto/tls"
	"fmt"
	pb "github.com/tc-fcopy/go-cache/pb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"sync"
	"time"
)

// Server 分布式缓存服务的核心服务实例结构体
type Server struct {
	addr       string           // 服务监听地址（如: ":8080"）
	svcName    string           // 服务名称（用于etcd服务注册的标识）
	groups     *sync.Map        // 缓存分组映射表，key为分组名，value为具体缓存分组实例（并发安全）
	grpcServer *grpc.Server     // grpc服务器
	etcdCli    *clientv3.Client // etcd 客户端实例，用于和etcd集群交互
	stopCh     chan error       // 服务停止信号通道，用于优雅关闭服务
	opts       *ServerOptions   // 服务启动的配置选项
}

// ServerOption 分布式缓存服务的配置选项结构体
// 用于初始化 Server 时传入自定义配置，如etcd连接信息、网络配置等
type ServerOptions struct {
	EtcdEndpoints []string      // etcd 集群端点列表（如: ["127.0.0.1:2379", "127.0.0.1:2380"]）
	DialTimeout   time.Duration // 与etcd集群建立连接的超时时间
	MaxMsgSize    int           // 网络通信的最大消息大小（字节），用于限制请求/响应的大小
	TLS           bool          // 是否启用TLS加密通信（true=启用，false=禁用）
	CertFile      string        // TLS证书文件路径（启用TLS时必填）
	KeyFile       string        // TLS私钥文件路径（启用TLS时必填）
}

var DefaultServerOption = &ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	MaxMsgSize:    4 << 20, // 4MB
}

type ServerOption func(*ServerOptions)

func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := DefaultServerOption
	for _, opt := range opts {
		opt(options)
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   options.EtcdEndpoints,
		DialTimeout: options.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 创建gRPC服务器
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &sync.Map{},
		grpcServer: grpc.NewServer(serverOpts...),
		etcdCli:    etcdCli,
		stopCh:     make(chan error),
		opts:       options,
	}

	pb.RegisterGoCacheServer(srv.grpcServer, srv)

}

// Start
func (s *Server) Start() error {
	// 启动GRPC
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 注册到etcd
	stopCh := make(chan error)
	go func() {
		if err := registry.Register(); err != nil {
		}
	}()
}

// loadTLSCredentials
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	// return
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}},
	), nil
}
