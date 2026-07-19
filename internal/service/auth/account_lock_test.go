package auth

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// loadAuthTestManager 构造最小可用配置管理器（security.account_lock.max_failures=3）。
func loadAuthTestManager(t *testing.T) *config.Manager {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := `
server:
  port: 8080
  mode: test
security:
  account_lock:
    max_failures: 3
    lock_duration: 10m
  login_fail_delays: []
`
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	m, err := config.LoadManager(p)
	if err != nil {
		t.Fatalf("load manager: %v", err)
	}
	return m
}

// TestMemoryAccountLockStore 验证进程内存储实现满足 cache.AccountLockStore 契约。
func TestMemoryAccountLockStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 取消后清理协程退出，避免测试 goroutine 泄漏（见 §3.65）
	store := NewMemoryAccountLockStore(ctx)
	email := "user@example.com"

	if n, _ := store.GetFailureCount(ctx, email); n != 0 {
		t.Fatalf("initial count = %d, want 0", n)
	}

	// 连续失败，max_failures=3：前两次不锁，第三次锁定。
	var locked bool
	for i := 1; i <= 3; i++ {
		c, lk, err := store.RecordFailure(ctx, email, 3, time.Hour, 10*time.Minute)
		if err != nil {
			t.Fatalf("RecordFailure: %v", err)
		}
		if c != i {
			t.Fatalf("count after %d failures = %d, want %d", i, c, i)
		}
		if i < 3 && lk {
			t.Fatalf("locked too early at failure %d", i)
		}
		locked = lk
	}
	if !locked {
		t.Fatal("should be locked after 3 failures")
	}
	got, err := store.GetLockUntil(ctx, email)
	if err != nil {
		t.Fatalf("GetLockUntil: %v", err)
	}
	if got.IsZero() || got.Before(time.Now()) {
		t.Fatalf("lock until = %v, want future time", got)
	}

	// max_failures=0 永不锁定（功能关闭）。
	if _, lk, _ := store.RecordFailure(ctx, "never@lock.com", 0, time.Hour, time.Minute); lk {
		t.Fatal("max_failures=0 should never lock")
	}

	if err := store.Clear(ctx, email); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if n, _ := store.GetFailureCount(ctx, email); n != 0 {
		t.Fatalf("count after clear = %d, want 0", n)
	}
	if l, _ := store.GetLockUntil(ctx, email); !l.IsZero() {
		t.Fatalf("lock after clear = %v, want zero", l)
	}
}

// fakeLockStore 用于验证 AuthService 是否正确委派到 cache.AccountLockStore。
type fakeLockStore struct {
	mu     sync.Mutex
	counts map[string]int
	locks  map[string]time.Time
	setCnt int
}

func (f *fakeLockStore) GetFailureCount(_ context.Context, email string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[email], nil
}

func (f *fakeLockStore) RecordFailure(_ context.Context, email string, maxFailures int, _ time.Duration, lockTTL time.Duration) (int, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts[email]++
	c := f.counts[email]
	locked := maxFailures > 0 && c >= maxFailures
	if locked {
		f.locks[email] = time.Now().Add(lockTTL)
		f.setCnt++
	}
	return c, locked, nil
}

func (f *fakeLockStore) GetLockUntil(_ context.Context, email string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.locks[email], nil
}

func (f *fakeLockStore) Clear(_ context.Context, email string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.counts, email)
	delete(f.locks, email)
	return nil
}

// TestMemoryAccountLockStore_CleanupStopsOnCtxCancel 守护 §3.65：清理协程在 ctx 取消后必须退出，
// 不再永久泄漏 goroutine；且取消后存储主路径（计数/锁定/清除）仍可正常服务，不 panic/死锁。
// 结构性修复（cleanup 增加 ctx.Done() 退出路径）由本测试锁死——若回退为无退出路径的 for range tick.C，
// 本测试仍会通过（行为上无害），故配合人工/CI goroutine 泄漏检测（goleak）效果最佳；此处至少保证
// 「取消 ctx 不破坏存储可用性」这一契约不被破坏。
func TestMemoryAccountLockStore_CleanupStopsOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := NewMemoryAccountLockStore(ctx)

	// 注入若干条目并验证计数/锁定正常
	for i := 1; i <= 3; i++ {
		if _, _, err := store.RecordFailure(ctx, "u@x.com", 3, time.Hour, 10*time.Minute); err != nil {
			t.Fatalf("RecordFailure: %v", err)
		}
	}
	if n, _ := store.GetFailureCount(ctx, "u@x.com"); n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
	if l, _ := store.GetLockUntil(ctx, "u@x.com"); l.IsZero() {
		t.Fatal("expected locked")
	}

	// 取消生命周期 ctx：清理协程应随之退出（无泄漏）
	cancel()

	// 取消后存储主路径仍可用（清理协程退出不影响读写）
	if err := store.Clear(ctx, "u@x.com"); err != nil {
		t.Fatalf("Clear after cancel: %v", err)
	}
	if n, _ := store.GetFailureCount(ctx, "u@x.com"); n != 0 {
		t.Fatalf("count after clear = %d, want 0", n)
	}
}

// TestAuthService_AccountLockFlow 验证 AuthService 的失败计数 / 锁定阈值 / 清除流程。
func TestAuthService_AccountLockFlow(t *testing.T) {
	mgr := loadAuthTestManager(t)
	store := &fakeLockStore{counts: map[string]int{}, locks: map[string]time.Time{}}
	// userRepo/logSvc/blacklist/producer 在锁定流程中不被使用，传 nil 即可。
	// userRepo/logSvc/blacklist/producer/mailer 在锁定流程中不被使用，传 nil 即可。
	svc := NewAuthService(mgr, nil, nil, nil, nil, store, nil)
	ctx := context.Background()
	email := "a@b.com"

	if svc.isAccountLocked(ctx, email) {
		t.Fatal("initially should not be locked")
	}

	// 前两次失败不应锁定（max_failures=3）
	for i := 1; i <= 2; i++ {
		svc.recordLoginFailure(ctx, email)
		if svc.isAccountLocked(ctx, email) {
			t.Fatalf("locked after %d failures, want not locked", i)
		}
	}

	// 第三次失败达到阈值，应锁定并写入 lock
	svc.recordLoginFailure(ctx, email)
	if !svc.isAccountLocked(ctx, email) {
		t.Fatal("should be locked after 3 failures")
	}
	if store.setCnt == 0 {
		t.Fatal("lock should have been set when threshold reached")
	}
	if store.counts[email] != 3 {
		t.Fatalf("recorded count = %d, want 3", store.counts[email])
	}

	// 登录成功后清除，应解除锁定
	svc.clearLoginFailure(ctx, email)
	if svc.isAccountLocked(ctx, email) {
		t.Fatal("should be unlocked after clear")
	}
	if store.counts[email] != 0 {
		t.Fatalf("count after clear = %d, want 0", store.counts[email])
	}
}
