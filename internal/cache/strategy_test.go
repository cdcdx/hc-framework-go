package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"go.uber.org/zap"
)

// fakeL1 仅 Get/Set 具备真实语义的内存实现，用于单测缓存策略回源路径。
type fakeL1 struct {
	mu   sync.Mutex
	data map[string]interface{}
}

func newFakeL1() *fakeL1 { return &fakeL1{data: make(map[string]interface{})} }

func (f *fakeL1) Get(_ context.Context, key string) (interface{}, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	return v, ok, nil
}
func (f *fakeL1) Set(_ context.Context, key string, value interface{}, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	return nil
}
func (f *fakeL1) Delete(_ context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.data, k)
	}
	return nil
}
func (f *fakeL1) Exists(_ context.Context, _ string) (bool, error) { return false, nil }
func (f *fakeL1) GetMulti(_ context.Context, keys []string) (map[string]interface{}, error) {
	out := make(map[string]interface{})
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		if v, ok := f.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}
func (f *fakeL1) SetMulti(_ context.Context, items map[string]interface{}, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range items {
		f.data[k] = v
	}
	return nil
}
func (f *fakeL1) Size() int64       { return 0 }
func (f *fakeL1) HitRatio() float64 { return 0 }
func (f *fakeL1) Evictions() uint64 { return 0 }
func (f *fakeL1) Close()            {}

// reset 清空内存数据，供 benchmark 每次迭代重置（避免 key 跨迭代命中 L1）。
func (f *fakeL1) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = make(map[string]interface{})
}

// newTestStrategy 构造仅含 L1（L2=nil，模拟 Redis 不可用/仅 L1 部署）的策略实例。
// degrade=true 时开启读降级（Degrade.Enabled + ReadSkipL2），用于验证降级路径合并。
func newTestStrategy(l1 L1Cache, degrade bool) *CacheStrategy {
	cm := config.NewManager(&config.Config{
		Cache: config.CacheConfig{
			Degrade: config.DegradeConfig{Enabled: degrade, ReadSkipL2: degrade},
		},
	})
	return &CacheStrategy{
		L1:  l1,
		cm:  cm,
		log: zap.NewNop(),
	}
}

// TestStrategy_SingleflightCoalesce 验证 L2 不可用时，并发同 key 回源仅触发一次 loader。
// 这是 singleflight 改造修复的核心击穿漏洞：原实现在此场景下完全无防护，所有并发
// 缺失 key 会直接打到 DB。
func TestStrategy_SingleflightCoalesce(t *testing.T) {
	l1 := newFakeL1()
	s := newTestStrategy(l1, false)

	var calls int64
	release := make(chan struct{})
	loader := func(ctx context.Context) (interface{}, error) {
		atomic.AddInt64(&calls, 1)
		<-release // 阻塞直到测试放行，期间所有并发 Get 应被 singleflight 合并
		return "v", nil
	}

	const n = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]interface{}, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = s.Get(context.Background(), "k", time.Minute, loader)
		}(i)
	}
	close(start)
	// 等待所有 goroutine 进入 singleflight 并被 leader 阻塞（而非各自回源）
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("loader call count = %d, want 1 (singleflight 应合并并发回源)", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d error: %v", i, errs[i])
		}
		if results[i] != "v" {
			t.Fatalf("goroutine %d result = %v, want v", i, results[i])
		}
	}
}

// TestStrategy_L1HitNoLoad 验证 L1 命中时完全不触发 loader（不穿透）。
func TestStrategy_L1HitNoLoad(t *testing.T) {
	l1 := newFakeL1()
	s := newTestStrategy(l1, false)
	if err := l1.Set(context.Background(), "k", "cached", time.Minute); err != nil {
		t.Fatal(err)
	}

	var calls int64
	loader := func(ctx context.Context) (interface{}, error) {
		atomic.AddInt64(&calls, 1)
		return "loaded", nil
	}
	v, err := s.Get(context.Background(), "k", time.Minute, loader)
	if err != nil {
		t.Fatal(err)
	}
	if v != "cached" {
		t.Fatalf("got %v, want cached", v)
	}
	if atomic.LoadInt64(&calls) != 0 {
		t.Fatalf("loader called %d times, want 0", atomic.LoadInt64(&calls))
	}
}

// TestStrategy_ReadDegradedCoalesce 验证读降级路径同样用 singleflight 合并并发回源，
// 避免降级时大量同 key 请求全部穿透到 DB（降级恰恰是 Redis 压力最大之时）。
func TestStrategy_ReadDegradedCoalesce(t *testing.T) {
	l1 := newFakeL1()
	s := newTestStrategy(l1, true)

	var calls int64
	release := make(chan struct{})
	loader := func(ctx context.Context) (interface{}, error) {
		atomic.AddInt64(&calls, 1)
		<-release
		return "dv", nil
	}

	const n = 30
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = s.Get(context.Background(), "dk", time.Minute, loader)
		}()
	}
	close(start)
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("degraded loader call count = %d, want 1", got)
	}
}

// TestStrategy_LeaderCancelDoesNotFailWaiters 验证 singleflight leader 的回源 ctx
// 已与单个请求的取消解耦（context.WithoutCancel）：首个到达请求即便已被取消，其回源
// 仍会正常完成，所有同 key 等待者都能拿到结果，而不是共享一次回源失败。
// 这是 singleflight 的标准陷阱——leader 使用首个请求的 ctx，若它取消会毒化全部等待者。
func TestStrategy_LeaderCancelDoesNotFailWaiters(t *testing.T) {
	l1 := newFakeL1()
	s := newTestStrategy(l1, false)

	var calls int64
	leaderEntered := make(chan struct{})
	leaderCtxErr := make(chan error, 1) // 记录 leader 回源时拿到的 ctx 是否已被取消
	release := make(chan struct{})
	loader := func(ctx context.Context) (interface{}, error) {
		atomic.AddInt64(&calls, 1)
		leaderCtxErr <- ctx.Err() // leader 回源 ctx 应未被取消（WithCancel 已解耦）
		close(leaderEntered)
		<-release
		return "v", nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 首个到达的请求 ctx 已取消

	// 先启动「已取消」的请求，它将成为 singleflight leader 并阻塞在回源中
	go func() {
		_, _ = s.Get(ctx, "k", time.Minute, loader)
	}()
	<-leaderEntered // 确认 leader 已进入回源（此时 sf 已持有该 key）

	// 其余请求用正常 ctx，作为等待者加入
	const n = 30
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]interface{}, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = s.Get(context.Background(), "k", time.Minute, loader)
		}(i)
	}
	close(start)
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("loader call count = %d, want 1 (leader 回源应被复用)", got)
	}
	if err := <-leaderCtxErr; err != nil {
		t.Fatalf("leader source ctx already cancelled: %v (应被 context.WithoutCancel 解耦)", err)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("waiter %d error: %v", i, errs[i])
		}
		if results[i] != "v" {
			t.Fatalf("waiter %d result = %v, want v", i, results[i])
		}
	}
}
