package singleflight

import (
	"context"
	"fmt"
	"sync"
)

// call 表示单个请求的执行体，包含结果、同步器和上下文取消能力
type call struct {
	wg     sync.WaitGroup     // 协程等待组，实现多协程等待单请求结果
	val    interface{}        // 业务函数执行结果
	err    error              // 业务函数执行错误
	ctx    context.Context    // 请求专属上下文，感知取消/超时
	cancel context.CancelFunc // 上下文取消函数，实现取消广播
}

// Group 管理所有正在进行的请求，key为请求唯一标识，并发安全
type Group struct {
	m sync.Map // key:请求标识(string), value:*call
}

// Do 核心方法：带context的请求合并，相同key请求合并为一次执行
// ctx: 上下文，用于取消/超时控制；key: 请求唯一标识；fn: 实际执行的业务逻辑
func (g *Group) Do(ctx context.Context, key string, fn func() (interface{}, error)) (interface{}, error) {
	// 检查是否已有同key的请求在执行，有则等待结果（同时监听取消）
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call)
		select {
		case <-ctx.Done(): // 当前协程的上下文被取消/超时
			return nil, ctx.Err()
		case <-c.ctx.Done(): // 同key的全局请求被取消/超时
			return nil, c.err
		default: // 无取消信号，阻塞等待请求完成
			c.wg.Wait()
			return c.val, c.err
		}
	}

	// 无同key请求，创建新call并初始化上下文（带取消能力）
	callCtx, cancel := context.WithCancel(ctx)
	c := &call{ctx: callCtx, cancel: cancel}
	c.wg.Add(1)       // 等待组+1，标记有任务在执行
	g.m.Store(key, c) // 将call存入map，供后续同key请求识别

	// 延迟清理：无论成功/失败/panic/取消，都保证资源释放和信号广播
	defer func() {
		c.wg.Done()     // 唤醒所有等待的协程
		c.cancel()      // 触发上下文取消，广播取消信号
		g.m.Delete(key) // 从map中移除，避免内存泄漏
		// panic防护：捕获fn的panic，转为error返回给所有等待协程
		if r := recover(); r != nil {
			c.err = fmt.Errorf("fn panic: %v", r)
		}
	}()

	// 执行前检查是否已被取消，避免无意义的业务执行
	select {
	case <-callCtx.Done():
		c.err = callCtx.Err()
		return nil, c.err
	default:
		// 唯一一次执行业务逻辑，结果存入call供所有协程复用
		c.val, c.err = fn()
		return c.val, c.err
	}
}

// DoWithoutCtx 兼容旧版本调用：无context的请求合并，内部封装默认上下文
func (g *Group) DoWithoutCtx(key string, fn func() (interface{}, error)) (interface{}, error) {
	return g.Do(context.Background(), key, fn)
}
