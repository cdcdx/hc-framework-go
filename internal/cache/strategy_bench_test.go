package cache

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkSpinWait_TimeAfter 复现优化前击穿防护自旋：循环内每次 time.After
// 都创建一个 time.Timer（逃逸到堆，触发前不可回收）。高频并发抢锁失败时，每个
// 请求自旋 10 次 = 10 个短命 timer 堆积，加重 timer 堆与 GC。
func BenchmarkSpinWait_TimeAfter(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < lockSpinTimes; j++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Nanosecond):
			}
		}
	}
}

// BenchmarkSpinWait_TimerReuse 优化后：复用单个 time.Timer + Reset，整个自旋
// 过程仅一次 timer 分配。
func BenchmarkSpinWait_TimerReuse(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		t := time.NewTimer(time.Nanosecond)
		for j := 0; j < lockSpinTimes; j++ {
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(time.Nanosecond)
		}
		t.Stop()
	}
}

// BenchmarkSourceCoalesce 对比「改前无合并（L2==nil 时防护整段被跳过，每个请求各自回源）」
// 与「改后 singleflight 合并」在并发缓存未命中下对 DB 的压力。
// 每个 bench op = 一次 concurrency 规模的同 key 并发未命中。
//   - NoCoalesce：复现旧实现，并发请求全部穿透到 DB（缓存击穿）。
//   - Coalesce  ：当前实现，singleflight 在本进程把并发同 key 回源合并为一次。
//
// 核心指标为 loader_calls/op（DB 命中次数）：Coalesce 应恒为 1，NoCoalesce 约为 concurrency。
// 同时报告 allocs/op 以体现单飞在回源合并之外省下的 goroutine/锁开销。
func BenchmarkSourceCoalesce(b *testing.B) {
	const concurrency = 50
	// 模拟轻量 DB 往返耗时，使「DB 命中次数」的时间成本在 ns/op 中可见
	const dbLatency = time.Millisecond

	b.Run("NoCoalesce", func(b *testing.B) {
		l1 := newFakeL1()
		s := newTestStrategy(l1, false)
		var calls int64
		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt64(&calls, 1)
			time.Sleep(dbLatency)
			return "v", nil
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			l1.reset()
			key := strconv.Itoa(i) // 每迭代用新 key，保证本迭代为缓存未命中
			var wg sync.WaitGroup
			start := make(chan struct{})
			for j := 0; j < concurrency; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					// 旧路径：缓存未命中后直接回源，无合并（L2==nil 时锁块被跳过）
					_, _ = s.loadAndFill(context.Background(), key, time.Minute, loader, false)
				}()
			}
			close(start)
			wg.Wait()
		}
		b.ReportMetric(float64(atomic.LoadInt64(&calls))/float64(b.N), "loader_calls/op")
	})

	b.Run("Coalesce", func(b *testing.B) {
		l1 := newFakeL1()
		s := newTestStrategy(l1, false)
		var calls int64
		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt64(&calls, 1)
			time.Sleep(dbLatency)
			return "v", nil
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			l1.reset()
			key := strconv.Itoa(i)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for j := 0; j < concurrency; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, _ = s.Get(context.Background(), key, time.Minute, loader)
				}()
			}
			close(start)
			wg.Wait()
		}
		b.ReportMetric(float64(atomic.LoadInt64(&calls))/float64(b.N), "loader_calls/op")
	})
}
