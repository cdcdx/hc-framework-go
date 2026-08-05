package cache

import (
	"context"
	"testing"
	"time"
)

// TestNewRistrettoStore_RoundTrip 验证 L1 初始化成功路径：
// 返回的 store 能正常 Set/Get，且 newRistrettoStore 在合法配置下不返回 error。
func TestNewRistrettoStore_RoundTrip(t *testing.T) {
	store, err := newRistrettoStore(L1Config{
		Enabled:     true,
		MaxMemoryMB: 64,
		DefaultTTL:  time.Second,
		NumCounters: 10_000,
		MaxCost:     1 << 20,
	})
	if err != nil {
		t.Fatalf("newRistrettoStore unexpected error: %v", err)
	}
	ctx := context.Background()
	if err := store.Set(ctx, "k", []byte("v"), time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	// Ristretto 异步写入，等待其落盘
	store.cache.Wait()
	val, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("got %q, want v", val)
	}
	if ok, _ := store.Exists(ctx, "k"); !ok {
		t.Fatal("exists should be true after set")
	}
	if ok, _ := store.Exists(ctx, "missing"); ok {
		t.Fatal("exists should be false for missing key")
	}
}
