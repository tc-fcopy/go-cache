package gocache

import (
	"context"
	"errors"
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

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// const 用于声明编译期常量（如数字、字符串）
//而errors.New()返回的是运行期创建的 error 接口实例（变量），不能用 const 声明

// 键不能为空
var ErrKeyNotFound = errors.New("key not found")

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
		return ByteView{}, ErrKeyNotFound
	}

	view, ok := g.mainCache.Get(ctx, key)
}
