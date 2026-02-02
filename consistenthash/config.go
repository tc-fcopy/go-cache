package consistenthash

import "hash/crc32"

type Config struct {
	// DefaultReplicas 虚拟节点默认副本数
	DefaultReplicas int
	// MinReplicas 虚拟节点最小副本数
	MinReplicas int
	// MaxReplicas 虚拟节点最大副本数
	MaxReplicas int
	// HashFunc 自定义哈希函数
	HashFunc func(data []byte) uint32
	// LoadBalanceThreshold 节点负载均衡阈值（浮点型，建议0.7~0.9）
	// 用于判断节点是否过载的阈值，通常为「节点当前负载/集群平均负载」的比值
	LoadBalanceThreshold float64
	// 新增：阻尼系数本次只调整整体的
	dampingFactor float64
}

var DefaultConfig = &Config{
	DefaultReplicas:      50,
	MinReplicas:          100,
	MaxReplicas:          200,
	HashFunc:             crc32.ChecksumIEEE,
	LoadBalanceThreshold: 0.25, // 25%负载不均衡触发调整
	dampingFactor:        0.5,
}
