package gocache

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/tc-fcopy/go-cache/store"
	"sync"
	"sync/atomic"
	"time"
)

type Cache struct {
	mu          sync.RWMutex // 读写锁：读多写少场景核心，多个协程可同时读，写入/初始化时加写锁
	store       store.Store  // 底层存储层，由store包根据CacheType创建（如LRU2）
	opts        CacheOptions // 缓存配置：类型、容量、分桶、清理策略等
	hits        int64        // 缓存命中数：原子统计
	misses      int64        // 缓存未命中数：原子统计
	initialized int32        // 原子状态：0=未初始化，1=已初始化
	closed      int32        // 原子状态：0=正常，1=已关闭，防止对关闭的缓存操作
}

type CacheOptions struct {
	CacheType    store.CacheType
	MaxBytes     int64
	BucketCount  uint16
	CapPerBucket uint16
	Level2Cap    uint16
	CleanupTime  time.Duration
	OnEvicted    func(key string, value store.Value)
}

func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     8 << 20,
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  1 * time.Minute,
		OnEvicted:    nil,
	}
}

func NewCache(opts CacheOptions) *Cache {
	return &Cache{
		opts: opts,
	}
}

func (c *Cache) ensureInitialized() {
	if atomic.LoadInt32(&c.initialized) == 1 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized == 0 {
		storeOptons := store.Options{
			MaxBytes:        c.opts.MaxBytes,
			BucketCount:     c.opts.BucketCount,
			CapPerBucket:    c.opts.CapPerBucket,
			Level2Cap:       c.opts.Level2Cap,
			CleanupInterval: c.opts.CleanupTime,
			OnEvicted:       c.opts.OnEvicted,
		}

		c.store = store.NewStore(c.opts.CacheType, storeOptons)

		atomic.StoreInt32(&c.initialized, 1)
		logrus.Infof("cache initialized with type %s, max bytes: %d", c.opts.CacheType, c.opts.MaxBytes)
	}
}

func (c *Cache) Add(key string, value ByteView) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s ", key)
		return
	}
	// 懒加载初始化底层store（首次调用时执行）
	c.ensureInitialized()

	if err := c.store.Set(key, value); err != nil {
		logrus.Warnf("failed to add key %s to cache: %v", key, value)
	}
}

func (c *Cache) Get(ctx context.Context, key string) (value ByteView, ok bool) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}

	// 如果cache未初始化，直接返回未命中
	if atomic.LoadInt32(&c.initialized) == 0 {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// 从底层存储获取
	val, found := c.store.Get(key)
	if !found {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	// 计算命中数
	atomic.AddInt64(&c.hits, 1)

	// 转换并返回
	if bv, ok := val.(ByteView); ok {
		return bv, true
	}
	//类型断言失败
	logrus.Warnf("Type assertion failed for key %s, expected ByteView", key)
	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
}

func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s ", key)
		return
	}

	c.ensureInitialized()

	// 计算过期时间
	expiration := time.Until(expirationTime)
	if expiration <= 0 {
		logrus.Debugf("Key %s already expired. not adding to cache", key)
		return
	}

	// 设置到底层存储
	if err := c.store.SetWithExpiration(key, value, expiration); err != nil {
		logrus.Warnf("failed to add key %s to cache: %v", key, value)
	}
}

func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()

	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

func (c *Cache) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}

	atomic.StoreInt32(&c.initialized, 0)

	logrus.Debugf("Cache closed, hits: %d, misses: %d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Len()
}

// 返回缓存统计信息
func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"closed":      atomic.LoadInt32(&c.closed) == 1,
		"misses":      atomic.LoadInt64(&c.misses),
		"hits":        atomic.LoadInt64(&c.hits),
	}

	if atomic.LoadInt32(&c.initialized) == 1 {
		stats["size"] = c.Len()

		// 计算命中率
		totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}

	return stats
}

func (c *Cache) Delete(key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.store.Delete(key)
}
