// Package service 业务逻辑层，编排领域服务与基础设施。
package auth

import (
	"context"
	"github.com/cdcdx/hc-framework-go/internal/cache"
	"sync"
	"time"
)

// 注：cache.AccountLockStore 接口已下沉到 cache 包（internal/cache/accountlock.go），
// 由 cache.Manager 与 auth.NewMemoryAccountLockStore 共同实现，避免 cache↔service/auth 循环依赖。本文件仅保留进程内实现。

// memoryAccountLockStore 进程内账号锁定存储（单实例 / 测试回退）。
// 保留原进程内 map 的语义，并自带后台清理协程，避免内存无限增长。
var _ cache.AccountLockStore = (*memoryAccountLockStore)(nil)

type memoryAccountLockStore struct {
	mu  sync.Mutex
	trk map[string]*loginFailEntry
}

type loginFailEntry struct {
	count       int
	lockedUntil time.Time
	lastSeen    time.Time
}

// NewMemoryAccountLockStore 创建进程内账号锁定存储，并启动后台清理协程。
// 清理协程随 ctx 取消而退出（select ctx.Done()），避免 goroutine 永久泄漏
// （见 §3.65）：旧实现 cleanup() 为无退出路径的 for range tick.C，每次创建都泄漏一个后台 goroutine，
// 且进程优雅关闭时无法停止。调用方应传入生命周期 ctx（如启动期的 bgCtx）。
func NewMemoryAccountLockStore(ctx context.Context) cache.AccountLockStore {
	s := &memoryAccountLockStore{trk: make(map[string]*loginFailEntry)}
	go s.cleanup(ctx)
	return s
}

// cleanup 清理长时间无活动且已解除锁定的失败计数条目。ctx 取消后立即退出（释放 goroutine）。
func (s *memoryAccountLockStore) cleanup(ctx context.Context) {
	tick := time.NewTicker(10 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			now := time.Now()
			s.mu.Lock()
			for k, v := range s.trk {
				expired := v.lockedUntil.IsZero() || now.After(v.lockedUntil)
				if expired && now.Sub(v.lastSeen) > time.Hour {
					delete(s.trk, k)
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *memoryAccountLockStore) GetFailureCount(_ context.Context, email string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.trk[email]; ok {
		return e.count, nil
	}
	return 0, nil
}

// RecordFailure 原子记录一次失败：递增计数，达到 maxFailures 时写入锁定（lockTTL 时长）。
// 内存实现与 Redis 实现语义一致：计数达阈值必锁定。
func (s *memoryAccountLockStore) RecordFailure(_ context.Context, email string, maxFailures int, _ time.Duration, lockTTL time.Duration) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.trk[email]
	if !ok {
		e = &loginFailEntry{}
		s.trk[email] = e
	}
	e.count++
	e.lastSeen = time.Now()
	locked := maxFailures > 0 && e.count >= maxFailures
	if locked {
		e.lockedUntil = time.Now().Add(lockTTL)
	}
	return e.count, locked, nil
}

func (s *memoryAccountLockStore) GetLockUntil(_ context.Context, email string) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.trk[email]; ok && !e.lockedUntil.IsZero() && time.Now().Before(e.lockedUntil) {
		return e.lockedUntil, nil
	}
	return time.Time{}, nil
}

func (s *memoryAccountLockStore) Clear(_ context.Context, email string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.trk, email)
	return nil
}
