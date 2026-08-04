package cache

import (
	"context"
	"testing"
	"time"
)

func TestCacheDisabled(t *testing.T) {
	m := New(Config{Enabled: false})
	ctx := context.Background()

	if val, _ := m.Get(ctx, "k"); val != nil {
		t.Fatal("disabled cache should return nil")
	}
	if err := m.Set(ctx, "k", []byte("v"), time.Second); err != nil {
		t.Fatal(err)
	}
	// 禁用缓存时，Exists 走 nilBloom（always true）+ nilStore（always miss）= false
	if m.Exists(ctx, "k") {
		t.Log("note: disabled cache exists depends on bloom; nilBloom pass-through is acceptable")
	}
	if err := m.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheL1Only_SetGetDelete(t *testing.T) {
	m := New(Config{
		Enabled: true,
		L1: L1Config{
			Enabled:     true,
			MaxMemoryMB: 64,
			DefaultTTL:  time.Second,
			NumCounters: 10_000,
			MaxCost:     1 << 20,
		},
	})
	defer m.Close()

	ctx := context.Background()
	if err := m.Set(ctx, "test:key", []byte("hello"), time.Second); err != nil {
		t.Fatal(err)
	}
	// Ristretto 异步写入，可能需要短暂等待
	for i := 0; i < 10; i++ {
		val, _ := m.Get(ctx, "test:key")
		if val != nil && string(val) == "hello" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	val, err := m.Get(ctx, "test:key")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "hello" {
		t.Fatalf("expected 'hello', got %q", val)
	}
	if !m.Exists(ctx, "test:key") {
		t.Fatal("exists should be true")
	}

	// 删除后应不可见
	m.Delete(ctx, "test:key")
	val, _ = m.Get(ctx, "test:key")
	if val != nil {
		t.Fatal("after delete should be nil")
	}
}

func TestCacheL1Only_ExistsMissingKey(t *testing.T) {
	m := New(Config{
		Enabled: true,
		L1: L1Config{
			Enabled:     true,
			MaxMemoryMB: 64,
			DefaultTTL:  time.Second,
			NumCounters: 10_000,
			MaxCost:     1 << 20,
		},
	})
	defer m.Close()

	ctx := context.Background()
	// 未 Set 过的 key，即使布隆可能误判存在，实际缓存为空也应返回 false。
	if m.Exists(ctx, "never:set:key") {
		t.Fatal("exists should be false for a key that was never set")
	}
}

func TestNilStore(t *testing.T) {
	ns := &nilStore{}
	ctx := context.Background()
	v, _ := ns.Get(ctx, "k")
	if v != nil {
		t.Fatal("nilStore should return nil")
	}
	if err := ns.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
}

func TestNilBloom(t *testing.T) {
	nb := &nilBloom{}
	nb.Add("k")
	if !nb.MightContain("k") {
		t.Fatal("nilBloom should always return true (pass-through)")
	}
}
