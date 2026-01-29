package store

import (
	"container/list"
	"sync"
	"time"
)

type lruCache struct {
	mu              sync.RWMutex
	list            *list.List
	items           map[string]*list.Element
	expires         map[string]time.Time
	maxBytes        int64
	usedBytes       int64
	onEvicted       func(key string, value Value)
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{}
}

type lruEntry struct {
	key string
	val Value
}

func newLRUCache(opts Options) *lruCache {
	// 设置默认清理间隔
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	c := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	// 启动定期清理协程
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()

	return c
}

func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}

	// 检测是否过期
	if exptime, hasExpired := c.expires[key]; hasExpired && time.Now().After(exptime) {
		c.mu.RUnlock()

		// 异步删除过期项，避免在读锁内操作
		go c.Delete(key)

		return nil, false
	}

	// 获取值并且释放读锁
	entry := elem.Value.(*lruEntry)
	value := entry.val
	c.mu.RUnlock()

	c.mu.Lock()
	// 再次检测元素是否存在,可能在获取读锁期间呗其他协程删除
	if _, ok := c.items[key]; ok {
		c.list.MoveToFront(elem)
	}

	c.mu.Unlock()
	return value, true
}

func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 计算过期时间
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	} else {
		delete(c.expires, key)
	}

	// 如果key存在，更新value
	if elem, ok := c.items[key]; ok {
		oldEntry := elem.Value.(*lruEntry)
		c.usedBytes += int64(value.Len() - oldEntry.val.Len())
		oldEntry.val = value
		c.list.MoveToFront(elem)
		return nil
	}

	// 添加新项
	entry := &lruEntry{key, value}
	elem := c.list.PushFront(entry)
	c.items[key] = elem
	c.usedBytes += int64(len(key) + value.Len())

	// 检测是否淘汰旧项
	c.evict()

	return nil
}

func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

func (c *lruCache) evict() {
	// 先清理过期项
	now := time.Now()
	for key, expTime := range c.expires {
		if now.After(expTime) {
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}
}

func (c *lruCache) removeElement(e *list.Element) {
	entry := e.Value.(*lruEntry)
	c.list.Remove(e)
	delete(c.items, entry.key)
	delete(c.expires, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.val.Len())

	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.val)
	}
}

func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.closeCh:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

// GetExpiration 获取键的过期时间
func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expTime, ok := c.expires[key]
	return expTime, ok
}

// UpdateExpiration 更新过期时间
func (c *lruCache) UpdateExpiration(key string, expiration time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; !ok {
		return false
	}

	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key)
	}

	return true
}

// UsedBytes 返回当前使用的字节数
func (c *lruCache) UsedBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

// MaxBytes 返回最大允许字节数
func (c *lruCache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes 设置最大允许字节数并触发淘汰
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict()
	}
}
