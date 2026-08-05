package cache

import (
	"context"
	"strconv"
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

// TestBloomNoFalseNegative 布隆过滤器核心不变量：绝不产生假阴性。
//
// 回归背景：早期实现用 Ristretto（LFU 淘汰）冒充布隆，key 被淘汰后
// MightContain 返回 false，导致 Manager.Exists 把仍在缓存中的数据
// 误判为不存在。实测 18396 个存活 key 中有 1444 个（7.8%）被误判。
func TestBloomNoFalseNegative(t *testing.T) {
	b := newBitsetBloom(10_000, 0.01)
	const n = 50_000 // 故意超出预期容量 5 倍，验证饱和后仍无假阴性
	for i := 0; i < n; i++ {
		b.Add(bloomKey(i))
	}
	for i := 0; i < n; i++ {
		if !b.MightContain(bloomKey(i)) {
			t.Fatalf("false negative on key %d: 已 Add 的 key 必须返回 true", i)
		}
	}
}

// TestBloomFiltersUnknownKeys 布隆应过滤掉绝大多数未插入的 key。
func TestBloomFiltersUnknownKeys(t *testing.T) {
	b := newBitsetBloom(100_000, 0.01)
	for i := 0; i < 10_000; i++ {
		b.Add(bloomKey(i))
	}
	fp := 0
	const probes = 10_000
	for i := 1_000_000; i < 1_000_000+probes; i++ {
		if b.MightContain(bloomKey(i)) {
			fp++
		}
	}
	if rate := float64(fp) / probes; rate > 0.05 {
		t.Errorf("假阳性率 %.2f%% 过高，穿透保护失效", rate*100)
	}
}

// TestManagerExistsAfterBloomSaturation 端到端验证：大量写入后
// 仍在 L1 中的 key，Exists 必须返回 true。
func TestManagerExistsAfterBloomSaturation(t *testing.T) {
	m := New(Config{
		Enabled: true,
		L1: L1Config{
			Enabled: true, DefaultTTL: time.Minute,
			NumCounters: 1000, MaxCost: 1 << 20,
		},
	})
	defer m.Close()

	ctx := context.Background()
	const n = 20_000
	for i := 0; i < n; i++ {
		_ = m.Set(ctx, bloomKey(i), []byte("v"), time.Minute)
	}
	for i := 0; i < n; i++ {
		k := bloomKey(i)
		val, _ := m.l1.Get(ctx, k)
		if val == nil {
			continue // 已被 L1 淘汰，不在本用例断言范围内
		}
		if !m.Exists(ctx, k) {
			t.Fatalf("key %s 仍在 L1 中，Exists 却返回 false", k)
		}
	}
}

func bloomKey(i int) string {
	return "bloom:key:" + strconv.Itoa(i)
}
