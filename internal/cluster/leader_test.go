package cluster

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeLocker 内存锁桩，模拟 Redis 分布式锁（含续期失败场景）。
type fakeLocker struct {
	mu        sync.Mutex
	held      bool
	refreshOK bool // RefreshLock 是否成功（模拟租约续期是否被他人抢占）
}

func (f *fakeLocker) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held {
		return false, nil
	}
	f.held = true
	return true, nil
}

func (f *fakeLocker) RefreshLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshOK, nil
}

func (f *fakeLocker) Unlock(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held = false
	return nil
}

// waitLeader 轮询直到领导权状态符合预期或超时。
func waitLeader(e *LeaderElector, want bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.IsLeader() == want {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// TestLeaderElector_Acquires 验证无竞争者时本进程能获取领导权并持有锁。
func TestLeaderElector_Acquires(t *testing.T) {
	fl := &fakeLocker{}
	e := NewLeaderElector(fl, "idle:leader", 100*time.Millisecond, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	if !waitLeader(e, true, time.Second) {
		t.Fatal("should become leader")
	}
	fl.mu.Lock()
	held := fl.held
	fl.mu.Unlock()
	if !held {
		t.Fatal("lock should be held by leader")
	}
}

// TestLeaderElector_LosesOnRefreshFail 验证租约续期失败（被其他副本抢占）时主动让出领导权。
func TestLeaderElector_LosesOnRefreshFail(t *testing.T) {
	fl := &fakeLocker{refreshOK: true}
	e := NewLeaderElector(fl, "idle:leader", 100*time.Millisecond, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	if !waitLeader(e, true, time.Second) {
		t.Fatal("should become leader first")
	}

	// 模拟锁被其他副本抢占：续期返回失败且本地标记已丢失
	fl.mu.Lock()
	fl.refreshOK = false
	fl.held = false
	fl.mu.Unlock()

	if !waitLeader(e, false, 2*time.Second) {
		t.Fatal("should lose leadership when refresh fails")
	}
}

// TestLeaderElector_StopReleases 验证 Stop 释放租约（优雅关闭）。
func TestLeaderElector_StopReleases(t *testing.T) {
	fl := &fakeLocker{}
	e := NewLeaderElector(fl, "idle:leader", 100*time.Millisecond, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	if !waitLeader(e, true, time.Second) {
		t.Fatal("should become leader")
	}
	e.Stop()
	fl.mu.Lock()
	held := fl.held
	fl.mu.Unlock()
	if held {
		t.Fatal("lock should be released after Stop")
	}
}
