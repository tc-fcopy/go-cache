// Package gocache 定义了基于 etcd 实现的分布式缓存服务相关核心结构
package gocache

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/sirupsen/logrus"
	pb "github.com/tc-fcopy/go-cache/pb"
	"github.com/tc-fcopy/go-cache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"net"
	"sync"
	"time"
)

// Server 分布式缓存服务的核心服务实例结构体
type Server struct {
	pb.UnimplementedGoCacheServer
	addr       string           // 服务监听地址（如: ":8080"）
	svcName    string           // 服务名称（用于etcd服务注册的标识）
	groups     *sync.Map        // 缓存分组映射表，key为分组名，value为具体缓存分组实例（并发安全）
	grpcServer *grpc.Server     // grpc服务器
	etcdCli    *clientv3.Client // etcd 客户端实例，用于和etcd集群交互
	stopCh     chan error       // 服务停止信号通道，用于优雅关闭服务
	opts       *ServerOptions   // 服务启动的配置选项
}

func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("no such group: %s", req.Group)
	}

	err := group.Delete(ctx, req.Key)
	return &pb.ResponseForDelete{Value: err == nil}, err
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
	// 注册服务
	pb.RegisterGoCacheServer(srv.grpcServer, srv)

	// 注册健康检查服务
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)
	healthServer.SetServingStatus(srv.svcName, healthpb.HealthCheckResponse_SERVING)
	return srv, nil
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
		if err := registry.Register(s.svcName, s.addr, stopCh); err != nil {
			logrus.Errorf("failed to register service: %v", err)
			close(stopCh)
			return
		}
	}()
	logrus.Infof("server starting at %s", s.addr)
	return s.grpcServer.Serve(lis)
}

// loadTLSCredentials
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	// 加载X.509格式的证书文件和私钥文件，生成tls.Certificate实例
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	// 基于加载的证书创建TLS配置，并封装为gRPC标准的传输凭证
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}},
	), nil
}

func (s Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("no such group: %s", req.Group)
	}

	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	return &pb.ResponseForGet{Value: view.ByteSlice()}, nil
}

func (s *Server) Stop() {
	close(s.stopCh)
	s.grpcServer.GracefulStop()
	if s.etcdCli != nil {
		s.etcdCli.Close()
	}
}

func (s *Server) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("no such group: %s", req.Group)
	}

	// 从context里面获取标记, 如果没有，就创建新的context
	fromPeer := ctx.Value("from_peer")
	if fromPeer == nil {
		ctx = context.WithValue(ctx, "from_peer", true)
	}

	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		return nil, err
	}
	return &pb.ResponseForGet{Value: req.Value}, nil
}

func WithDialTimeout(dialTimeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = dialTimeout
	}
}

func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}
