// Package cluster 提供多副本部署下的协作原语。
// 当前包含基于 Redis 分布式锁的 Leader 选举（LeaderElector），用于让“恰好一次”类任务
// （回填、每日/每周任务重置、事件去重清理、Keyspace 通知配置等）在集群中仅由一个 Pod 执行。
package cluster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Locker 选主所需的锁原语。*cache.Manager 已实现该接口（Lock/RefreshLock/Unlock）。
// 定义为接口便于在单测中以内存桩替换 Redis。
type Locker interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	RefreshLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, key string) error
}

// LeaderElector 基于租约的选主器。
// 工作模型：周期性尝试获取（未持有）或续期（已持有）一把带 TTL 的 Redis 锁；
// 持有锁即认为自己是 Leader。TTL 到期后若未续期（进程崩溃/网络中断），锁自动释放，
// 其他副本可抢占，从而实现故障转移。
//
// 容错说明：扫描/回填等被保护任务本身已通过业务层乐观锁保证幂等，故选主为“尽力而为”
// 的负载分摊手段——即便短暂出现双 Leader，也不会产生错误结果，仅多执行一次幂等操作。
type LeaderElector struct {
	locker  Locker
	key     string
	ttl     time.Duration
	renewal time.Duration

	mu       sync.RWMutex
	leader   bool
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewLeaderElector 创建选主器。renewal<=0 时取 ttl/2（默认不小于 1s）。
func NewLeaderElector(locker Locker, key string, ttl, renewal time.Duration) *LeaderElector {
	if renewal <= 0 {
		renewal = ttl / 2
	}
	if renewal <= 0 {
		renewal = time.Second
	}
	return &LeaderElector{
		locker:  locker,
		key:     key,
		ttl:     ttl,
		renewal: renewal,
		stopCh:  make(chan struct{}),
	}
}

// Start 启动后台选主循环（非阻塞）。ctx 取消或 Stop 调用后退出并释放锁。
func (e *LeaderElector) Start(ctx context.Context) {
	go e.loop(ctx)
}

func (e *LeaderElector) loop(ctx context.Context) {
	ticker := time.NewTicker(e.renewal)
	defer ticker.Stop()

	e.tryAcquireOrRenew(ctx) // 立即尝试一次，缩短成为 Leader 的延迟
	for {
		select {
		case <-ctx.Done():
			e.release()
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.tryAcquireOrRenew(ctx)
		}
	}
}

// tryAcquireOrRenew 未持有锁则尝试获取；已持有则续期。网络瞬时错误时保守保持现状，
// 不因一次失败就丢弃领导权（避免抖动）。
func (e *LeaderElector) tryAcquireOrRenew(ctx context.Context) {
	e.mu.RLock()
	isLeader := e.leader
	e.mu.RUnlock()

	var (
		ok  bool
		err error
	)
	if isLeader {
		ok, err = e.locker.RefreshLock(ctx, e.key, e.ttl)
	} else {
		ok, err = e.locker.Lock(ctx, e.key, e.ttl)
	}
	if err != nil {
		zap.L().Warn("leader election tick failed (kept current state)",
			zap.String("key", e.key), zap.Bool("was_leader", isLeader), zap.Error(err))
		return
	}
	if ok != isLeader {
		zap.L().Info("leader election state changed",
			zap.String("key", e.key), zap.Bool("is_leader", ok))
	}
	e.setLeader(ok)
}

func (e *LeaderElector) setLeader(v bool) {
	e.mu.Lock()
	e.leader = v
	e.mu.Unlock()
}

// IsLeader 返回本进程当前是否为 Leader（并发安全）。
// 接收者为 nil 时（未启用选主 / L2 不可用降级）返回 true，语义等价于
// 「无选主约束，各 Pod 正常执行」，避免 nil 底层指针接口场景下 RL　ock 踩 nil panic（13 §3.47）。
func (e *LeaderElector) IsLeader() bool {
	if e == nil {
		return true
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leader
}

// Stop 停止选主循环并释放租约（优雅关闭用）。
func (e *LeaderElector) Stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
		e.release()
	})
}

func (e *LeaderElector) release() {
	// 忽略错误：释放失败不影响进程退出；锁会在 TTL 到期后自动释放。
	_ = e.locker.Unlock(context.Background(), e.key)
	e.setLeader(false)
}
