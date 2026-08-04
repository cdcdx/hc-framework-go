package cache

// ---- Ristretto-based BloomFilter ----
// Ristretto 本身支持成本淘汰，对高频 key 天然保留。
// 这里用独立的 ristretto 实例作为布隆替代品：
// 只存 key（value 为空），利用 Ristretto 的 LFU 淘汰实现近似布隆。
// 好处：无需额外依赖，内存可控，统计特性接近布隆。
// 限制：存在极小误判率（Ristretto 淘汰导致），对缓存穿透保护足够。

import (
	"github.com/dgraph-io/ristretto/v2"
)

type ristrettoBloom struct {
	cache *ristretto.Cache[string, struct{}]
}

func newRistrettoBloom(cfg L1Config) *ristrettoBloom {
	// 布隆用 1/10 的 MaxCost，仅存 key 标记
	cost := cfg.MaxCost / 10
	if cost < 1<<20 { // 最少 1MB
		cost = 1 << 20
	}
	c, err := ristretto.NewCache(&ristretto.Config[string, struct{}]{
		NumCounters: cfg.NumCounters / 10,
		MaxCost:     cost,
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}
	return &ristrettoBloom{cache: c}
}

func (b *ristrettoBloom) Add(key string) {
	b.cache.SetWithTTL(key, struct{}{}, 1, 0) // cost=1, 永不过期
}

func (b *ristrettoBloom) MightContain(key string) bool {
	_, ok := b.cache.Get(key)
	return ok
}

// ---- nilBloom: 缓存禁用时的 no-op 实现 ----
type nilBloom struct{}

func (n *nilBloom) Add(key string)               {}
func (n *nilBloom) MightContain(key string) bool { return true } // 无布隆时不过滤
