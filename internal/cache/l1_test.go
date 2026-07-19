package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"

	stdjson "encoding/json"
)

func newTestL1(t *testing.T) *RistrettoCache {
	t.Helper()
	c, err := NewRistrettoCache(&config.L1CacheConfig{
		Enabled:     true,
		MaxCost:     100_000_000,
		NumCounters: 10_000_000,
	}, 30*time.Second, 0) // ttlJitter=0 使测试可预测
	if err != nil {
		t.Fatalf("NewRistrettoCache: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestL1_GetSetRoundtrip(t *testing.T) {
	l1 := newTestL1(t)
	ctx := context.Background()

	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	val := &user{Name: "alice", Age: 30}
	if err := l1.Set(ctx, "u:1", val, 0); err != nil {
		t.Fatal(err)
	}

	got, ok, err := l1.Get(ctx, "u:1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected hit")
	}
	if got == nil {
		t.Fatal("got nil")
	}
	// L1 中反序列化为 map[string]interface{}
	m, ok := got.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["name"] != "alice" {
		t.Errorf("name = %v", m["name"])
	}
	if m["age"] != float64(30) { // JSON numbers are float64
		t.Errorf("age = %v", m["age"])
	}
}

func TestL1_GetMiss(t *testing.T) {
	l1 := newTestL1(t)
	_, ok, err := l1.Get(context.Background(), "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected miss")
	}
}

func TestL1_SetDeleteRoundtrip(t *testing.T) {
	l1 := newTestL1(t)
	ctx := context.Background()

	if err := l1.Set(ctx, "k", "hello", 0); err != nil {
		t.Fatal(err)
	}
	if err := l1.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	_, ok, _ := l1.Get(ctx, "k")
	if ok {
		t.Fatal("expected miss after delete")
	}
}

func TestL1_Exists(t *testing.T) {
	l1 := newTestL1(t)
	ctx := context.Background()

	l1.Set(ctx, "k", "v", 0)
	ok, err := l1.Exists(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected exists")
	}

	ok, _ = l1.Exists(ctx, "missing")
	if ok {
		t.Fatal("expected not exists")
	}
}

func TestL1_HitMissCounters(t *testing.T) {
	l1 := newTestL1(t)
	ctx := context.Background()

	// 初始全 0
	if l1.Hits() != 0 || l1.Misses() != 0 {
		t.Fatal("expected zero initial counters")
	}
	if l1.HitRatio() != 0 {
		t.Fatal("expected zero ratio initially")
	}

	l1.Set(ctx, "k", "v", 0) // Set 不参与计数
	l1.Get(ctx, "k")         // hit
	l1.Get(ctx, "missing")   // miss

	if l1.Hits() != 1 {
		t.Errorf("Hits = %d, want 1", l1.Hits())
	}
	if l1.Misses() != 1 {
		t.Errorf("Misses = %d, want 1", l1.Misses())
	}

	ratio := l1.HitRatio()
	if ratio <= 0.49 || ratio >= 0.51 {
		t.Errorf("HitRatio = %.4f, want ~0.5", ratio)
	}
}

func TestL1_SetBytes(t *testing.T) {
	l1 := newTestL1(t)
	data := []byte(`{"x":1}`)
	l1.SetBytes("b", data, 10*time.Second)

	got, ok, err := l1.Get(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected hit after SetBytes")
	}
	m := got.(map[string]interface{})
	if m["x"] != float64(1) {
		t.Errorf("x = %v", m["x"])
	}
}

func TestL1_GetMulti(t *testing.T) {
	l1 := newTestL1(t)
	ctx := context.Background()
	l1.Set(ctx, "a", "1", 0)
	l1.Set(ctx, "b", "2", 0)

	result, err := l1.GetMulti(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result["a"] != "1" || result["b"] != "2" {
		t.Error("unexpected values in GetMulti")
	}
}

func TestL1_SetMulti(t *testing.T) {
	l1 := newTestL1(t)
	ctx := context.Background()
	items := map[string]interface{}{
		"x": 100,
		"y": "hello",
	}
	if err := l1.SetMulti(ctx, items, 0); err != nil {
		t.Fatal(err)
	}
	_, ok, _ := l1.Get(ctx, "x")
	if !ok {
		t.Fatal("x not found")
	}
	_, ok, _ = l1.Get(ctx, "y")
	if !ok {
		t.Fatal("y not found")
	}
}

func TestL1_JitterTTL(t *testing.T) {
	l1 := newTestL1(t) // jitter=0，返回原值
	base := 10 * time.Second
	got := l1.jitterTTL(base)
	if got != base {
		t.Errorf("jitterTTL with offset=0: got %v, want %v", got, base)
	}
}

func TestL1_JitterTTL_NonZero(t *testing.T) {
	c, err := NewRistrettoCache(&config.L1CacheConfig{
		Enabled:     true,
		MaxCost:     100_000_000,
		NumCounters: 10_000_000,
	}, 10*time.Second, 0.2) // 20% jitter
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	base := 10 * time.Second
	for i := 0; i < 20; i++ {
		got := c.jitterTTL(base)
		if got < 8*time.Second || got > 12*time.Second {
			t.Errorf("jitterTTL(%v) = %v, out of [8s, 12s]", base, got)
		}
	}
}

func TestL1_SetJSONBytes_ReadRoundtrip(t *testing.T) {
	// Set 后 GetBytes 应返回相同的原始 JSON
	l1 := newTestL1(t)
	ctx := context.Background()
	obj := map[string]string{"hello": "world"}
	if err := l1.Set(ctx, "jsonkt", obj, 0); err != nil {
		t.Fatal(err)
	}
	raw, ok := l1.GetBytes("jsonkt")
	if !ok {
		t.Fatal("expected GetBytes hit")
	}
	var decoded map[string]string
	if err := stdjson.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["hello"] != "world" {
		t.Error("roundtrip mismatch")
	}
}

func TestL1_Evictions(t *testing.T) {
	l1 := newTestL1(t)
	if l1.Evictions() != 0 {
		t.Log("evictions may be non-zero due to ristretto internal buffers")
	}
}

func TestL1_ConcurrentGetSet(t *testing.T) {
	l1 := newTestL1(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			l1.Set(ctx, "k", n, 0)
			_, _, _ = l1.Get(ctx, "k")
		}(i)
	}
	wg.Wait()
	// 无 panic 即通过
}

func TestL1_DisabledReturnsError(t *testing.T) {
	_, err := NewRistrettoCache(&config.L1CacheConfig{
		Enabled: false,
	}, 0, 0)
	if err == nil {
		t.Fatal("expected error for disabled config")
	}
}

func TestL1_SizeReturnsNonNegative(t *testing.T) {
	l1 := newTestL1(t)
	s := l1.Size()
	if s < 0 {
		t.Errorf("size = %d, want >= 0", s)
	}
}
