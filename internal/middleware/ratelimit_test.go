package middleware

import (
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_isExemptPath(t *testing.T) {
	rl := &rateLimiter{exemptPrefixes: []string{"/api/v1/idle/heartbeat", "/health"}}

	cases := []struct {
		path string
		want bool
	}{
		{"/api/v1/idle/heartbeat", true},
		{"/api/v1/idle/heartbeat/", true},
		{"/api/v1/idle/heartbeat/x/y", true}, // 前缀匹配，子路径同样豁免
		{"/health", true},
		{"/api/v1/user/profile", false},
		{"/api/v1/idle/start", false}, // 同为 idle 组但非心跳，不豁免
		{"", false},
	}
	for _, c := range cases {
		if got := rl.isExemptPath(c.path); got != c.want {
			t.Errorf("isExemptPath(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestRateLimiter_isExemptPath_emptyPrefix(t *testing.T) {
	rl := &rateLimiter{exemptPrefixes: []string{""}}
	if rl.isExemptPath("/api/v1/idle/heartbeat") {
		t.Fatal("empty prefix must not exempt any path")
	}
}

// TestStopRateLimiter_Idempotent 验证多次调用不 panic（幂等安全网）。
func TestStopRateLimiter_Idempotent(t *testing.T) {
	StopRateLimiter()
	StopRateLimiter()
	// 如有已初始化的 rlInstance 则 ticker 被停止；nil 时纯 no-op。
}

// TestRateLimiter_cleanupGoroutineExitsOnStop 守护「限流器后台 cleanup goroutine 泄漏」修复：
// 此前为 `for range cleanupTick.C`，Ticker.Stop 不关闭 channel，Stop 后 goroutine 永久阻塞泄漏；
// 改为 select 监听 stopCh 后，close(stopCh) 必须使 goroutine 真正退出（done 被关闭）。
func TestRateLimiter_cleanupGoroutineExitsOnStop(t *testing.T) {
	rl := &rateLimiter{
		cleanupTick: time.NewTicker(time.Hour), // 长间隔，避免自动 tick 干扰判定
		stopCh:      make(chan struct{}),
	}
	done := make(chan struct{})
	go rl.runCleanup(done)

	// 模拟 StopRateLimiter 的核心动作：停止 ticker 并关闭 stopCh。
	rl.cleanupTick.Stop()
	close(rl.stopCh)

	select {
	case <-done:
		// goroutine 已退出，泄漏修复生效。
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup goroutine did not exit after stopCh closed (goroutine leak regression)")
	}
}

// TestRateLimiter_cleanup_NoPanicOnDirtyEntry 验证 cleanup() 对脏 entry
// 不会 panic（§3.43 已为 cleanup 中 Load 加 `, ok` 守卫）。
func TestRateLimiter_cleanup_NoPanicOnDirtyEntry(t *testing.T) {
	rl := &rateLimiter{
		perUser: sync.Map{},
		perIP:   sync.Map{},
	}
	rl.perUser.Store("dirty_user", "not_a_bucket")
	rl.perIP.Store("dirty_ip", 42)
	// cleanup 不应 panic
	rl.cleanup()
}
