package svc

import (
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/config"
)

// TestStartIdleScan_StopsOnClose 验证后台扫描 goroutine 在 stop 通道关闭后能优雅退出，
// 不会泄漏（此前为 for range ticker.C 的死循环，无法退出）。
func TestStartIdleScan_StopsOnClose(t *testing.T) {
	svcCtx := &ServiceContext{
		Config: config.Config{
			Idle: config.IdleConfig{
				ScanInterval:   10 * time.Millisecond,
				TimeoutMinutes: 5,
			},
		},
		stop: make(chan struct{}),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go startIdleScan(svcCtx, svcCtx.stop, &wg)

	// 让扫描至少跑一次
	time.Sleep(30 * time.Millisecond)

	close(svcCtx.stop)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 期望：Close 后 goroutine 已退出
	case <-time.After(2 * time.Second):
		t.Fatal("idle scan goroutine did not stop after close(stop)")
	}
}
