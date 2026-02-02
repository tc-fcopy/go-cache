package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Map struct {
	mu            sync.RWMutex
	config        *Config
	keys          []int
	hashMap       map[int]string
	nodeReplicas  map[string]int
	nodeCounts    map[string]int64
	totalRequests int64
	lastRebalance int64 // 新增：上一次重平衡的时间戳（毫秒），原子操作
}

type Option func(*Map)

func New(opts ...Option) *Map {
	m := &Map{
		config:        DefaultConfig,
		hashMap:       make(map[int]string),
		nodeReplicas:  make(map[string]int),
		nodeCounts:    make(map[string]int64),
		lastRebalance: 0,
	}

	for _, opt := range opts {
		opt(m)
	}

	m.startBalancer()
	return m
}

func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, node := range nodes {
		if node == "" {
			continue
		}

		m.addNode(node, m.config.DefaultReplicas)
	}

	// 重新排序
	sort.Ints(m.keys)
	return nil
}

func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("no replicas for node %s", node)
	}

	// 移除节点的所有虚拟节点
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		//for j := 0; j < len(m.keys); j++ {
		//	if m.keys[j] == hash {
		//		m.keys = append(m.keys[:j], m.keys[j+1:]...)
		//		break
		//	}
		//}
		// 优化
		// 二分查找找到hash的索引
		idx := sort.SearchInts(m.keys, hash)
		if idx < len(m.keys) && m.keys[idx] == hash {
			m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
		}
	}
	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

// Get 获取节点
func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.config.HashFunc([]byte(key)))
	// 二分查找
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// 处理边界情况
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	count := m.nodeCounts[node]
	m.nodeCounts[node] = count + 1
	atomic.AddInt64(&m.totalRequests, 1)
	return node
}

func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
}

func (m *Map) checkAndRebalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return
	}

	// 新增：前置条件2——冷却时间未到，不检测（冷却时间30秒，可加入Config）
	const rebalanceCoolDown = 30 * 1000 // 30秒冷却
	last := atomic.LoadInt64(&m.lastRebalance)
	now := time.Now().UnixMilli()
	if now-last < rebalanceCoolDown {
		return
	}

	// 计算负载情况
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))
	var maxDiff float64

	for _, count := range m.nodeCounts {
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}

	if maxDiff > m.config.LoadBalanceThreshold {
		m.rebalanceNodes()
	}
}

// rebalanceNodes 重新平衡节点
func (m *Map) rebalanceNodes() {
	m.mu.Lock() // 写操作：修改哈希环/虚拟节点数，加写锁
	defer m.mu.Unlock()
	// 步骤1：计算当前平均负载
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	//
	dampingFactor := m.config.dampingFactor
	// 步骤2：遍历所有节点，根据负载调整虚拟节点数
	for node, count := range m.nodeCounts {
		currentReplicas := m.nodeReplicas[node]
		// 计算节点负载比例：节点负载 / 平均负载
		loadRatio := float64(count) / avgLoad
		if loadRatio == 0 {
			loadRatio = 1
		}
		var newReplicas float64

		// 负载过高（比例>1）：减少虚拟节点数（负载越高，减少越多）
		if loadRatio > 1 {
			// 负载过高：原公式 * 阻尼系数
			adjustRatio := 1.0 / loadRatio
			newReplicas = float64(currentReplicas) * (1 - dampingFactor*(1-adjustRatio))
		} else {
			// 负载过低：原公式 * 阻尼系数
			adjustRatio := 2 - loadRatio
			newReplicas = float64(currentReplicas) * (1 + dampingFactor*(adjustRatio-1))
		}
		newReplicasInt := int(math.Round(newReplicas))

		// 边界限制（原有逻辑不变）
		if newReplicasInt < m.config.MinReplicas {
			newReplicasInt = m.config.MinReplicas
		}
		if newReplicasInt > m.config.MaxReplicas {
			newReplicasInt = m.config.MaxReplicas
		}
		// 步骤4：若副本数有变化，先删除节点再重新添加（重建虚拟节点）
		// 只有变化幅度≥5%，才调整（避免微小变化带来的开销，可配置）
		changeRatio := math.Abs(float64(newReplicasInt-currentReplicas)) / float64(currentReplicas)
		if newReplicasInt != currentReplicas && changeRatio > 0.05 {
			if err := m.Remove(node); err != nil {
				continue
			}
			m.addNode(node, newReplicasInt)
		}
	}

	// 步骤5：重置负载统计：重平衡后重新统计，避免历史数据影响
	// 【关键修改】不再重置统计！保留历史数据，保证判断连续性
	//for node := range m.nodeCounts {
	//	m.nodeCounts[node] = 0
	//}
	//atomic.StoreInt64(&m.totalRequests, 0)
	// 步骤6：重新排序虚拟节点哈希值，保证二分查找有效
	sort.Ints(m.keys)
}

// GetStats 获取负载统计信息
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}
	// 计算每个节点的负载占比
	for node, count := range m.nodeCounts {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

// 将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			m.checkAndRebalance()
		}
	}()
}
