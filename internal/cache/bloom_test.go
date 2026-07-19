package cache

import (
	"context"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// TestOptimalBloomParams_Defaults 验证无效输入时使用合理的默认值。
func TestOptimalBloomParams_Defaults(t *testing.T) {
	cases := []struct {
		name string
		n    int64
		p    float64
	}{
		{"zero N", 0, 0.01},
		{"negative N", -1, 0.01},
		{"invalid P (0)", 1000, 0.0},
		{"invalid P (1)", 1000, 1.0},
		{"invalid P (negative)", 1000, -0.5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, k := optimalBloomParams(c.n, c.p)
			if m <= 0 {
				t.Errorf("m = %d, want > 0", m)
			}
			if k < 1 {
				t.Errorf("k = %d, want >= 1", k)
			}
		})
	}
}

// TestOptimalBloomParams_KnownFormula 验证布隆公式计算一致性。
func TestOptimalBloomParams_KnownFormula(t *testing.T) {
	// n=10000, p=0.01 → m ≈ 95850, k ≈ 7
	m, k := optimalBloomParams(10000, 0.01)
	if m < 90000 || m > 100000 {
		t.Errorf("m = %d, want ≈ 95850", m)
	}
	if k < 5 || k > 9 {
		t.Errorf("k = %d, want ≈ 7", k)
	}
}

// TestFNVHash_Deterministic 验证相同输入产生相同哈希。
func TestFNVHash_Deterministic(t *testing.T) {
	data := []byte("hello-bloom")
	h1a, h2a := fnvHash(data)
	h1b, h2b := fnvHash(data)
	if h1a != h1b || h2a != h2b {
		t.Fatal("fnvHash not deterministic")
	}
}

// TestFNVHash_DifferentInputs 验证不同输入产生不同哈希（概率性）。
func TestFNVHash_DifferentInputs(t *testing.T) {
	h1a, _ := fnvHash([]byte("key-a"))
	h1b, _ := fnvHash([]byte("key-b"))
	if h1a == h1b {
		t.Fatal("different inputs should produce different hashes (collision happened)")
	}
}

// TestRedisBloom_HashPositions 验证哈希位置在有效范围内且数量正确。
func TestRedisBloom_HashPositions(t *testing.T) {
	b := &RedisBloom{
		bitSize:   100000,
		hashCount: 7,
	}
	pos := b.hashPositions([]byte("test-key"))
	if len(pos) != int(b.hashCount) {
		t.Fatalf("got %d positions, want %d", len(pos), b.hashCount)
	}
	for _, p := range pos {
		if p < 0 || p >= b.bitSize {
			t.Errorf("position %d out of range [0, %d)", p, b.bitSize)
		}
	}
}

// TestRedisBloom_HashPositions_DifferentKeys 验证不同 key 产生不同位置集。
func TestRedisBloom_HashPositions_DifferentKeys(t *testing.T) {
	b := &RedisBloom{
		bitSize:   100000,
		hashCount: 7,
	}
	p1 := b.hashPositions([]byte("key-x"))
	p2 := b.hashPositions([]byte("key-y"))
	// 两个集合应至少有一个不同位置
	same := true
	for i := range p1 {
		if p1[i] != p2[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different keys should produce different position sets")
	}
}

// TestNewRedisBloom_WithValidConfig 验证有效配置创建正确的布隆实例。
func TestNewRedisBloom_WithValidConfig(t *testing.T) {
	cfg := &config.BloomConfig{
		Capacity:  1000000,
		ErrorRate: 0.01,
		RedisKey:  "test:bloom",
	}
	b := NewRedisBloom(nil, cfg) // client 可为 nil，仅测试结构构造
	if b.redisKey != "test:bloom" {
		t.Errorf("redisKey = %s, want test:bloom", b.redisKey)
	}
	if b.bitSize <= 0 {
		t.Errorf("bitSize = %d, want > 0", b.bitSize)
	}
	if b.hashCount < 1 {
		t.Errorf("hashCount = %d, want >= 1", b.hashCount)
	}
	bs, hc := b.Info()
	if bs != b.bitSize || hc != b.hashCount {
		t.Fatal("Info() mismatch")
	}
}

// TestRedisBloom_AddMulti_EmptySlice 验证空切片安全返回。
func TestRedisBloom_AddMulti_EmptySlice(t *testing.T) {
	b := &RedisBloom{bitSize: 1000, hashCount: 3}
	if err := b.AddMulti(context.Background(), nil); err != nil {
		t.Fatalf("AddMulti(nil) should not error: %v", err)
	}
	if err := b.AddMulti(context.Background(), []string{}); err != nil {
		t.Fatalf("AddMulti(empty) should not error: %v", err)
	}
}
