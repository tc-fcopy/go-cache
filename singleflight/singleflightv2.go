package singleflight

import (
	"context"
	"fmt"
	"sync"
)

// call 单个请求的执行体，包含结果、协程同步、上下文取消能力
type call2 struct {
	wg     sync.WaitGroup     // 协程等待组，实现多协程等待单请求结果
	val    interface{}        // 业务函数执行结果
	err    error              // 业务函数执行错误
	ctx    context.Context    // 请求专属上下文，感知取消/超时
	cancel context.CancelFunc // 上下文取消函数，实现取消广播
}

// Group 管理所有正在进行的请求，key为请求唯一标识，基于sync.Map保证并发安全
type Group2 struct {
	m sync.Map // key: 请求标识(string), value: *call
}

// Do 核心方法：带context的原子请求合并，相同key请求严格仅执行一次
// ctx: 上下文（控制取消/超时）；key: 请求唯一标识；fn: 实际执行的业务逻辑（如查库/查远端缓存）
func (g *Group) Do2(ctx context.Context, key string, fn func() (interface{}, error)) (interface{}, error) {
	// 1. 初始化新call，派生带取消能力的子上下文（继承入参ctx的生命周期）
	callCtx, cancel := context.WithCancel(ctx)
	c := &call{
		ctx:    callCtx,
		cancel: cancel,
	}
	c.wg.Add(1) // 等待组+1，标记任务开始执行

	// 2. 原子操作LoadOrStore：不存在则存储当前call，存在则返回已有call
	// loaded=true 表示key已存在，已有其他协程在执行；loaded=false 表示当前协程是第一个执行的
	existing, loaded := g.m.LoadOrStore(key, c)
	if loaded {
		// 3. 已有同key请求在执行：释放当前新建call的资源，等待已有call的结果
		c.cancel()           // 取消当前无用的call上下文，避免资源泄漏
		c = existing.(*call) // 切换为已有call，复用其执行结果

		// 等待结果同时监听取消信号（当前协程/全局请求取消），优先级高于等待
		select {
		case <-ctx.Done():
			return nil, ctx.Err() // 当前协程的上下文被取消/超时
		case <-c.ctx.Done():
			return nil, c.err // 同key的全局请求被取消/超时
		default:
			c.wg.Wait()         // 无取消信号，阻塞等待请求完成
			return c.val, c.err // 复用已有执行结果
		}
	}

	// 4. 当前协程是第一个执行的：执行业务逻辑，完成后统一清理资源
	defer func() {
		c.wg.Done()     // 唤醒所有等待的协程
		c.cancel()      // 触发上下文取消，广播执行完成/异常信号
		g.m.Delete(key) // 从map中移除key，允许后续新请求重新执行

		// panic防护：捕获fn的panic，转为error返回给所有等待协程，避免协程永久阻塞
		if r := recover(); r != nil {
			c.err = fmt.Errorf("fn panic: %v", r)
		}
	}()

	// 执行前最后检查：避免执行前已被取消，减少无意义的业务操作
	select {
	case <-c.ctx.Done():
		c.err = c.ctx.Err()
		return nil, c.err
	default:
		c.val, c.err = fn() // 唯一一次执行业务逻辑
		return c.val, c.err
	}
}

// DoWithoutCtx 兼容旧版本调用：无context的请求合并，内部封装标准根上下文
// 原有代码直接替换 Do 为 DoWithoutCtx 即可，无需其他修改，行为与基础版一致
func (g *Group) DoWithoutCtx2(key string, fn func() (interface{}, error)) (interface{}, error) {
	return g.Do(context.Background(), key, fn)
}
