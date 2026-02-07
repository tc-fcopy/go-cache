package gocache

import (
	"context"
	"fmt"
	"github.com/tc-fcopy/go-cache/consistenthash"
	"github.com/tc-fcopy/go-cache/registry"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "gocache"

type PeerPicker interface {
	// 返回值：peer(选中的节点)、ok(是否找到节点)、self(是否选中了自己)
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error // 释放选择器所有资源
}

// Peer 定义了「缓存节点」的核心接口（与之前gRPC客户端的Client结构体实现的接口一致）
type Peer interface {
	Get(group string, key string) ([]byte, error)                          // 获取缓存
	Set(ctx context.Context, group string, key string, value []byte) error // 设置缓存
	Delete(group string, key string) (bool, error)                         // 删除缓存
	Close() error                                                          // 释放节点资源
}

// ClientPicker 是PeerPicker接口的核心实现
type ClientPicker struct {
	selfAddr string              // 本节点的地址（如127.0.0.1:8080），用于跳过自身
	svcName  string              // 服务名称，etcd中服务注册的前缀（默认gocache）
	mu       sync.RWMutex        // 读写互斥锁，保证并发安全（多读少写场景最优）
	consHash *consistenthash.Map // 一致性哈希实例，核心做key->节点地址的映射
	clients  map[string]*Client  // 远程节点的gRPC客户端映射：节点地址->Client实例
	etcdCli  *clientv3.Client    // etcd客户端实例，用于服务发现（查询/监听节点）
	ctx      context.Context     // 根上下文，控制服务发现协程的生命周期
	cancel   context.CancelFunc  // 上下文取消函数，调用后触发ctx.Done()，终止所有子协程
}

// PickerOption 定义ClientPicker的配置选项类型（函数式选项模式）
// 优势：无需修改NewClientPicker参数，即可灵活扩展配置，兼容后续新增字段
type PickerOption func(*ClientPicker)

// WithServiceName 具体的配置选项：设置服务名称
// 调用方式：NewClientPicker(addr, WithServiceName("my-gocache"))
func WithServiceName(name string) PickerOption {
	// 返回一个修改ClientPicker的函数，闭包持有配置值name
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

// PrintPeers 打印当前已发现的所有远程节点（仅用于调试/测试）
func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()         // 读锁：仅遍历，不修改，支持多协程同时读
	defer p.mu.RUnlock() // 函数结束释放读锁，避免死锁

	log.Printf("当前已发现的节点:")
	// 遍历clients映射，打印所有远程节点地址
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

// NewClientPicker 创建ClientPicker实例（构造函数）
// 参数：addr(本节点地址)、opts(可变配置选项)
// 返回：ClientPicker实例、错误（创建失败时）
func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	// 创建可取消的根上下文：用于控制服务发现的监听协程，Close时调用cancel终止
	ctx, cancel := context.WithCancel(context.Background())
	// 初始化ClientPicker基础字段
	picker := &ClientPicker{
		selfAddr: addr,                     // 赋值本节点地址
		svcName:  defaultSvcName,           // 默认服务名称gocache
		clients:  make(map[string]*Client), // 初始化远程客户端映射
		consHash: consistenthash.New(),     // 初始化一致性哈希实例
		ctx:      ctx,                      // 根上下文
		cancel:   cancel,                   // 取消函数
	}

	// 遍历所有配置选项，执行配置函数，修改picker的字段（如自定义服务名称）
	for _, opt := range opts {
		opt(picker)
	}

	// 创建etcd客户端实例，使用registry包的默认配置（端点、超时等）
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.DefaultConfig.Endpoints,   // etcd地址，如["localhost:2379"]
		DialTimeout: registry.DefaultConfig.DialTimeout, // etcd建连超时
	})
	if err != nil { // etcd客户端创建失败，释放资源并返回错误
		cancel() // 终止上下文
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	picker.etcdCli = cli // 赋值etcd客户端到picker

	// 启动服务发现（全量查询+增量监听）
	if err := picker.startServiceDiscovery(); err != nil {
		cancel()        // 终止上下文
		cli.Close()     // 关闭etcd客户端
		return nil, err // 返回服务发现启动失败错误
	}

	return picker, nil // 初始化成功，返回picker实例
}

// startServiceDiscovery 启动服务发现核心逻辑
// 步骤：1. 全量查询etcd中已注册的所有节点，初始化本地映射；2. 启动协程增量监听节点变化
func (p *ClientPicker) startServiceDiscovery() error {
	// 第一步：全量更新——查询etcd中该服务下的所有节点，加载到本地
	if err := p.fetchAllServices(); err != nil {
		return err
	}

	// 第二步：增量更新——启动独立协程，监听etcd中节点的增/删变化，实时更新本地
	go p.watchServiceChanges()
	return nil
}

// watchServiceChanges 监听etcd中服务节点的变化（增量更新）
// 运行在独立协程中，由根上下文ctx控制生命周期
func (p *ClientPicker) watchServiceChanges() {
	// 创建etcd的watcher实例，用于监听key的变化
	watcher := clientv3.NewWatcher(p.etcdCli)
	// 启动监听：监听 /services/[svcName] 前缀的所有key，返回监听通道
	// clientv3.WithPrefix() 表示监听该前缀下的所有key
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	// 无限循环，持续监听事件
	for {
		select {
		case <-p.ctx.Done(): // 上下文被取消（如调用Close），释放资源并退出协程
			watcher.Close() // 关闭etcd watcher
			return
		case resp := <-watchChan: // 监听到etcd的事件（节点增/删）
			// 处理监听到的所有事件
			p.handleWatchEvents(resp.Events)
		}
	}
}

// handleWatchEvents 处理etcd监听到的事件（核心：更新本地节点映射）
// 参数：events 监听到的事件列表（EventTypePut=新增/更新，EventTypeDelete=删除）
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mu.Lock()         // 写锁：要修改clients和consHash，独占锁保证并发安全
	defer p.mu.Unlock() // 函数结束释放写锁

	// 遍历所有事件，逐个处理
	for _, event := range events {
		// event.Kv.Value 是etcd中存储的节点地址（如127.0.0.1:8081）
		addr := string(event.Kv.Value)
		if addr == p.selfAddr { // 跳过自身节点，不创建自己的gRPC客户端
			continue
		}

		// 根据事件类型处理
		switch event.Type {
		case clientv3.EventTypePut: // 新增/更新节点事件
			// 如果本地还没有该节点的客户端，就添加
			if _, exists := p.clients[addr]; !exists {
				p.set(addr)                                        // 新增节点：创建gRPC客户端+加入一致性哈希
				logrus.Infof("New service discovered at %s", addr) // 日志记录
			}
		case clientv3.EventTypeDelete: // 删除节点事件
			// 如果本地有该节点的客户端，就移除
			if client, exists := p.clients[addr]; exists {
				client.Close()                              // 关闭该节点的gRPC连接，释放资源
				p.remove(addr)                              // 移除节点：从一致性哈希删除+从clients映射删除
				logrus.Infof("Service removed at %s", addr) // 日志记录
			}
		}
	}
}

// fetchAllServices 全量查询etcd中该服务下的所有节点（服务发现初始化用）
func (p *ClientPicker) fetchAllServices() error {
	// 创建带3秒超时的子上下文：基于根上下文p.ctx，超时后自动取消，避免etcd查询阻塞
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel() // 函数结束释放上下文，避免资源泄漏

	// 从etcd查询 /services/[svcName] 前缀的所有key-value
	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName, clientv3.WithPrefix())
	if err != nil { // 查询失败，包装错误并返回
		return fmt.Errorf("failed to get all services: %v", err)
	}

	p.mu.Lock() // 写锁：修改本地节点映射，保证并发安全
	defer p.mu.Unlock()

	// 遍历etcd返回的所有key-value对
	for _, kv := range resp.Kvs {
		addr := string(kv.Value) // 解析节点地址
		// 地址非空且不是自身节点，才添加
		if addr != "" && addr != p.selfAddr {
			p.set(addr)                                    // 新增节点
			logrus.Infof("Discovered service at %s", addr) // 日志记录
		}
	}
	return nil
}

// set 新增单个节点（私有方法，仅内部调用）
// 逻辑：创建该节点的gRPC客户端 → 加入一致性哈希 → 加入clients映射
func (p *ClientPicker) set(addr string) {
	// 调用之前实现的NewClient，创建远程节点的gRPC客户端（复用etcd客户端，避免重复创建）
	if client, err := NewClient(addr, p.svcName, p.etcdCli); err == nil {
		p.consHash.Add(addr)     // 将节点地址加入一致性哈希，参与key的映射
		p.clients[addr] = client // 将gRPC客户端加入映射，后续可直接调用
		logrus.Infof("Successfully created client for %s", addr)
	} else { // 创建gRPC客户端失败，记录错误日志（不中断流程，其他节点正常工作）
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
	}
}

// remove 移除单个节点（私有方法，仅内部调用）
// 逻辑：从一致性哈希删除 → 从clients映射删除（连接已在调用处关闭）
func (p *ClientPicker) remove(addr string) {
	p.consHash.Remove(addr) // 从一致性哈希中删除该节点，后续key不会映射到它
	delete(p.clients, addr) // 从clients映射中删除，释放内存
}

// PickPeer PeerPicker接口的核心实现：根据缓存key选择对应的节点
// 核心逻辑：一致性哈希根据key获取节点地址 → 从clients映射取对应的gRPC客户端 → 返回结果
func (p *ClientPicker) PickPeer(key string) (Peer, bool, bool) {
	p.mu.RLock() // 读锁：仅查询，不修改，支持多协程同时调用（高并发场景最优）
	defer p.mu.RUnlock()

	// 第一步：一致性哈希根据key获取对应的节点地址
	if addr := p.consHash.Get(key); addr != "" {
		// 第二步：从clients映射中获取该地址的gRPC客户端
		if client, ok := p.clients[addr]; ok {
			// 返回：客户端实例、找到节点（ok=true）、是否是自身（addr==selfAddr）
			return client, true, addr == p.selfAddr
		}
	}
	// 未找到节点，返回默认值
	return nil, false, false
}

// Close 释放ClientPicker的所有资源（优雅关闭）
// 逻辑：终止上下文 → 关闭所有远程节点的gRPC连接 → 关闭etcd客户端 → 汇总错误
func (p *ClientPicker) Close() error {
	p.cancel() // 第一步：调用取消函数，终止根上下文，触发监听协程退出

	p.mu.Lock() // 写锁：修改clients映射，保证并发安全
	defer p.mu.Unlock()

	var errs []error // 收集所有关闭失败的错误，最后统一返回

	// 遍历所有远程节点的gRPC客户端，逐个关闭连接
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			// 包装错误，记录哪个节点关闭失败
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}

	// 关闭etcd客户端
	if err := p.etcdCli.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	// 如果有错误，汇总后返回；无错误返回nil
	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

// parseAddrFromKey 从etcd的key中解析节点地址（目前代码中未使用，预留方法）
// 例如：key="/services/gocache/127.0.0.1:8081" → 解析出"127.0.0.1:8081"
func parseAddrFromKey(key, svcName string) string {
	// 构造etcd的key前缀：/services/[svcName]/
	prefix := fmt.Sprintf("/services/%s/", svcName)
	// 如果key以该前缀开头，就截取前缀后的部分（节点地址）
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return "" // 前缀不匹配，返回空
}
