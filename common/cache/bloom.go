// 布隆过滤器：用于缓存穿透保护。
//
// 核心不变量：**绝不产生假阴性**。
// MightContain(k) == false 必须意味着 k 从未被 Add 过，否则依赖它做
// "短路返回 false" 的 Manager.Exists 会把真实存在的数据误判为不存在。
//
// 实现为标准的位数组 + k 个哈希（double hashing），只置位、不淘汰，
// 因此天然满足上述不变量；代价是存在可控的假阳性率（默认 ~1%）。
package cache

import (
	"hash/fnv"
	"math"
	"sync"
)

// bitsetBloom 标准布隆过滤器（位数组实现，并发安全）。
type bitsetBloom struct {
	mu   sync.RWMutex
	bits []uint64 // 位数组，每个 uint64 承载 64 bit
	m    uint64   // 总位数
	k    uint64   // 哈希函数个数
	n    uint64   // 已插入元素计数（用于观测饱和度）
}

// newBitsetBloom 按预期元素量 n 与目标假阳性率 p 构造布隆过滤器。
// 位数 m = -n*ln(p)/(ln2)^2，哈希数 k = (m/n)*ln2。
func newBitsetBloom(expectedN uint64, p float64) *bitsetBloom {
	if expectedN == 0 {
		expectedN = 1_000_000
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}

	m := uint64(math.Ceil(-float64(expectedN) * math.Log(p) / (math.Ln2 * math.Ln2)))
	if m < 64 {
		m = 64
	}
	k := uint64(math.Round(float64(m) / float64(expectedN) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 16 {
		k = 16
	}

	return &bitsetBloom{
		bits: make([]uint64, (m+63)/64),
		m:    m,
		k:    k,
	}
}

// hashPair 生成 double hashing 所需的两个基础哈希值。
func hashPair(key string) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(key))
	a := h1.Sum64()

	// 第二个哈希：对 a 做一轮 splitmix64 扰动，避免再跑一次字符串哈希
	b := a
	b ^= b >> 30
	b *= 0xbf58476d1ce4e5b9
	b ^= b >> 27
	b *= 0x94d049bb133111eb
	b ^= b >> 31
	if b == 0 {
		b = 0x9e3779b97f4a7c15 // 保证步长非零，否则 k 个位置会退化为同一个
	}
	return a, b
}

func (b *bitsetBloom) Add(key string) {
	h1, h2 := hashPair(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := uint64(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		b.bits[idx/64] |= 1 << (idx % 64)
	}
	b.n++
}

func (b *bitsetBloom) MightContain(key string) bool {
	h1, h2 := hashPair(key)
	b.mu.RLock()
	defer b.mu.RUnlock()
	for i := uint64(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		if b.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

// EstimatedFPR 返回当前饱和度下的预估假阳性率，用于监控。
// 公式: (1 - e^(-k*n/m))^k
func (b *bitsetBloom) EstimatedFPR() float64 {
	b.mu.RLock()
	n, k, m := b.n, float64(b.k), float64(b.m)
	b.mu.RUnlock()
	if n == 0 {
		return 0
	}
	return math.Pow(1-math.Exp(-k*float64(n)/m), k)
}

// ---- nilBloom: 缓存禁用时的 no-op 实现 ----
type nilBloom struct{}

func (n *nilBloom) Add(key string)               {}
func (n *nilBloom) MightContain(key string) bool { return true } // 无布隆时不过滤
