package gocache

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tc-fcopy/go-cache/singleflight"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// 加载KV的回调函数接口
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type GetterFunc func(ctx context.Context, key string) ([]byte, error)

func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// const 用于声明编译期常量（如数字、字符串）
//而errors.New()返回的是运行期创建的 error 接口实例（变量），不能用 const 声明

// 键不能为空
var ErrKeyRequired = errors.New("key is required")

// 值不能为空
var ErrValueRequired = errors.New("value is required")

// 组已关闭
var ErrGroupClosed = errors.New("group closed")

type Group struct {
	name       string
	getter     Getter
	mainCache  *Cache
	peers      PeerPicker
	loader     *singleflight.Group
	expiration time.Duration
	closed     int32
	stats      groupStats
}

type groupStats struct {
	loads        int64
	localHits    int64
	localMisses  int64
	peerHits     int64
	peerMisses   int64
	loaderHits   int64
	loaderErrors int64
	loadDuration int64
}

type GroupOption func(*Group)

func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	group := &Group{
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheOpts),
		loader:    &singleflight.Group{},
	}

	// 应用选项
	for _, opt := range opts {
		opt(group)
	}

	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, ok := groups[group.name]; ok {
		log.Printf("group %s already exists", group.name)
	}

	groups[group.name] = group
	logrus.Infof("group [%s] created with cacheBytes=%d, expiration=%v", group.name, cacheBytes, group.expiration)
	return group

}

func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}

	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	view, ok := g.mainCache.Get(ctx, key)

	if ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}

	atomic.AddInt64(&g.stats.localMisses, 1)

	return g.load(ctx, key)
}

func (g *Group) load(ctx context.Context, key string) (ByteView, error) {
	// 使用singleflight 确保并发请求只加载一次
	startTime := time.Now()
	viewi, err := g.loader.Do(ctx, key, func() (interface{}, error) {
		return g.loadData(ctx, key)
	})

	// 记录加载时间
	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration, loadDuration)
	atomic.AddInt64(&g.stats.loads, 1)

	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return ByteView{}, err
	}

	view := viewi.(ByteView)

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	return view, nil
}

func (g *Group) loadData(ctx context.Context, key string) (ByteView, error) {
	// 尝试从远程节点获取
	if g.peers != nil {
		peer, ok, isSelf := g.peers.PickPeer(key)
		if ok && !isSelf {
			value, err := g.getFromPeer(ctx, peer, key)
			if err == nil {
				atomic.AddInt64(&g.stats.peerHits, 1)
				return value, nil
			}
			atomic.AddInt64(&g.stats.peerMisses, 1)
			logrus.Warnf("[GoCache]failed to get from peer: %v", err)
		}
	}

	// 从数据源获得
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get data: %v", err)
	}

	atomic.AddInt64(&g.stats.loaderHits, 1)
	return ByteView{b: cloneBytes(bytes)}, err
}

// 注册PeerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("[GoCache] registered peers for group [%s]", g.name)
}

func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	// 检查是否从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	// 创建缓存视图
	view := ByteView{b: cloneBytes(value)}

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	// 如果不是从其他节点同步过来的请求, 且启用了分布式模式, 同步到其他节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "set", key, value)
	}

	return nil
}

func (g *Group) syncToPeers(ctx context.Context, op string, key string, value []byte) {
	if g.peers == nil {
		return
	}

	// 选择对等节点
	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok || isSelf {
		return
	}

	// 创建同步请求上下文
	syncCtx := context.WithValue(ctx, "from_peer", true)

	var err error
	switch op {
	case "set":
		err = peer.Set(syncCtx, g.name, key, value)
	case "delete":
		_, err = peer.Delete(g.name, key)
	}
	if err != nil {
		logrus.Errorf("[GoCache] failed to sync %s to peer: %v", op, err)
	}
}

func (g *Group) Clear() {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mainCache.Clear()
	logrus.Infof("[GoCache] clear cache for group [%s]", g.name)
}
func (g *Group) Close() error {
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	// 关闭本地缓存
	if g.mainCache != nil {
		g.mainCache.Close()
	}

	// 从全局组映射移除
	groupsMu.Lock()
	delete(groups, g.name)
	groupsMu.Unlock()

	logrus.Infof("[GoCache] close cache for group [%s]", g.name)
	return nil
}

func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / (float64(time.Millisecond))
	}

	// 添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}
	return stats
}

func (g *Group) getFromPeer(ctx context.Context, peer Peer, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get from peer :%w", err)
	}
	return ByteView{b: bytes}, err
}

func (g *Group) Delete(ctx context.Context, key string) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	// 从本地删除缓存
	g.mainCache.Delete(key)

	// 检查是否是其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	// 如果不是其他节点同步过来的请求，且启用了分布式，同步到其他节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "delete", key, nil)
	}
	return nil
}
