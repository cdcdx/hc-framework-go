package degrade

import "testing"

func TestGlobalDegrade(t *testing.T) {
	// 初始非降级
	if IsActive() {
		t.Fatalf("initial IsActive = true, want false")
	}

	// 单个熔断器打开 → 降级
	OnBreakerOpen()
	if !IsActive() {
		t.Fatalf("after open: IsActive = false, want true")
	}

	// 多个熔断器（模拟不同路由）打开，全部恢复前仍降级
	OnBreakerOpen()
	OnBreakerClosed() // 第一个恢复，但仍有 1 个打开
	if !IsActive() {
		t.Fatalf("after one close (still open): IsActive = false, want true")
	}

	// 最后一个恢复 → 解除降级
	OnBreakerClosed()
	if IsActive() {
		t.Fatalf("after all closed: IsActive = true, want false")
	}
}
