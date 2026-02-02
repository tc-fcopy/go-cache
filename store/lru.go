package store

import (
	"container/list"
	"sync"
	"time"
)

type lruCache struct {
	mu              sync.RWMutex                  // 读写锁：读多写少场景优化，读共享、写互斥
	list            *list.List                    // 双向链表：实现LRU，头部是最近使用，尾部是最少使用
	items           map[string]*list.Element      // 哈希表：key映射到链表节点，实现O(1)查找
	expires         map[string]time.Time          // 过期表：key映射到过期时间，管理缓存过期
	maxBytes        int64                         // 缓存最大允许占用字节数，0表示无限制
	usedBytes       int64                         // 缓存当前已占用字节数
	onEvicted       func(key string, value Value) // 淘汰回调：缓存被淘汰时触发（可选）
	cleanupInterval time.Duration                 // 定期清理过期项的间隔
	cleanupTicker   *time.Ticker                  // 定时器：触发定期清理
	closeCh         chan struct{}                 // 关闭通道：优雅停止定期清理协程
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
	// 特殊处理：value为nil，等价于删除key
	if value == nil {
		c.Delete(key)
		return nil
	}

	// 加写锁：写入操作互斥，防止并发修改
	c.mu.Lock()
	defer c.mu.Unlock()

	// 步骤1：处理过期时间
	var expTime time.Time
	if expiration > 0 { // 传了过期时间，设置并写入expires表
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	} else { // 永不过期，从expires表中删除key
		delete(c.expires, key)
	}

	// 步骤2：key已存在，更新值（LRU标记+字节数更新）
	if elem, ok := c.items[key]; ok {
		oldEntry := elem.Value.(*lruEntry)
		// 计算字节数变化：新值长度 - 旧值长度，更新已用字节
		c.usedBytes += int64(value.Len() - oldEntry.val.Len())
		oldEntry.val = value     // 替换值
		c.list.MoveToFront(elem) // LRU核心：更新后移到头部
		return nil
	}

	// 步骤3：key不存在，新增缓存项
	entry := &lruEntry{key, value}
	elem := c.list.PushFront(entry) // 新节点插入链表头部
	c.items[key] = elem             // 哈希表映射key到节点
	// 统计字节数：key的长度 + value的长度（Value接口Len()）
	c.usedBytes += int64(len(key) + value.Len())

	// 步骤4：触发淘汰逻辑（如果已用字节超过最大值，淘汰最少使用/过期项）
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

	// 再根据内存限制清理最久未使用的项
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		elem := c.list.Front()
		if elem != nil {
			c.removeElement(elem)
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
		case <-c.cleanupTicker.C:
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

// 清空缓存
func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key, entry.val)
		}
	}

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}
