package cache

import (
	"context"
	"time"
)

// AccountLockStore 账号连续登录失败状态的存储抽象。
// 多实例部署下，账号锁定必须在各 Pod 间共享，否则每个节点各自计数、锁定不跨 Pod 生效。
//   - cache.Manager（Redis）实现：跨 Pod 共享，单 key Hash + Lua 原子脚本，key 自动过期即清理；
//   - auth.NewMemoryAccountLockStore：进程内实现，用于单实例或测试回退（见 internal/service/auth）。
//
// 该接口定义置于 cache 包，使 cache.Manager 与其实现者（auth 子包）共享同一契约而不产生
// cache↔service/auth 的循环依赖（auth 已依赖 cache，cache 不应反向依赖 auth）。
type AccountLockStore interface {
	// GetFailureCount 返回当前连续失败次数（诊断用）。
	GetFailureCount(ctx context.Context, email string) (int, error)
	// RecordFailure 原子记录一次登录失败：递增计数；达到 maxFailures 时在同一次操作中写入锁定。
	// 返回递增后的计数与本次是否触发锁定。countTTL/lockTTL 分别控制计数窗口与锁定时长。
	RecordFailure(ctx context.Context, email string, maxFailures int, countTTL, lockTTL time.Duration) (count int, locked bool, err error)
	// GetLockUntil 返回锁定截止时间（零值表示未锁定）。
	GetLockUntil(ctx context.Context, email string) (time.Time, error)
	// Clear 清除失败计数与锁定（登录成功时）。
	Clear(ctx context.Context, email string) error
}
