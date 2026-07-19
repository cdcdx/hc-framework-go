package cache

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/dgraph-io/ristretto/v2"
)

// RistrettoCache L1 本地缓存实现
type RistrettoCache struct {
	cache      *ristretto.Cache[string, []byte]
	cfg        *config.L1CacheConfig
	hitCount   atomic.Uint64 // 命中计数（原子，避免热路径抢锁）
	missCount  atomic.Uint64 // 未命中计数（原子）
	defaultTTL time.Duration
	ttlJitter  float64
}

// NewRistrettoCache 创建 Ristretto 缓存
func NewRistrettoCache(cfg *config.L1CacheConfig, defaultTTL time.Duration, ttlJitter float64) (*RistrettoCache, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("l1 cache disabled")
	}

	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: cfg.NumCounters,
		MaxCost:     cfg.MaxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, fmt.Errorf("create ristretto cache: %w", err)
	}

	return &RistrettoCache{
		cache:      cache,
		cfg:        cfg,
		defaultTTL: defaultTTL,
		ttlJitter:  ttlJitter,
	}, nil
}

// jitterTTL 添加 TTL 随机偏移（防雪崩）
func (c *RistrettoCache) jitterTTL(baseTTL time.Duration) time.Duration {
	if c.ttlJitter <= 0 {
		return baseTTL
	}
	jitter := time.Duration(float64(baseTTL) * c.ttlJitter * (rand.Float64()*2 - 1))
	result := baseTTL + jitter
	if result <= 0 {
		return baseTTL
	}
	return result
}

// Get 获取缓存值
func (c *RistrettoCache) Get(_ context.Context, key string) (interface{}, bool, error) {
	data, ok := c.cache.Get(key)
	if !ok {
		c.missCount.Add(1)
		return nil, false, nil
	}

	c.hitCount.Add(1)

	// 尝试 JSON 反序列化
	var result interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		// 可能是原始字符串
		return string(data), true, nil
	}
	return result, true, nil
}

// GetBytes 获取原始字节
func (c *RistrettoCache) GetBytes(key string) ([]byte, bool) {
	return c.cache.Get(key)
}

// Set 设置缓存值
func (c *RistrettoCache) Set(_ context.Context, key string, value interface{}, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	ttl = c.jitterTTL(ttl)

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	cost := int64(len(data))
	c.cache.SetWithTTL(key, data, cost, ttl)
	c.cache.Wait()
	return nil
}

// SetBytes 设置原始字节（跳过序列化，直接写入 ristretto）。
// 适用于调用方已有序列化结果的场景。TTL<=0 时使用默认 TTL。
// 注意：ristretto 在容量满时可能静默丢弃写入（write-buffer 异步模型），
// 故本方法不返回 error；调用方应结合 HitRatio/Evictions 指标判断容量是否充足。
func (c *RistrettoCache) SetBytes(key string, data []byte, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	ttl = c.jitterTTL(ttl)
	c.cache.SetWithTTL(key, data, int64(len(data)), ttl)
	c.cache.Wait()
}

// Delete 删除缓存键
func (c *RistrettoCache) Delete(_ context.Context, keys ...string) error {
	for _, key := range keys {
		c.cache.Del(key)
	}
	return nil
}

// Exists 检查键是否存在
func (c *RistrettoCache) Exists(_ context.Context, key string) (bool, error) {
	_, ok := c.cache.Get(key)
	return ok, nil
}

// GetMulti 批量获取
func (c *RistrettoCache) GetMulti(_ context.Context, keys []string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, key := range keys {
		if data, ok := c.cache.Get(key); ok {
			var val interface{}
			if err := json.Unmarshal(data, &val); err != nil {
				result[key] = string(data)
			} else {
				result[key] = val
			}
			c.hitCount.Add(1)
		} else {
			c.missCount.Add(1)
		}
	}
	return result, nil
}

// SetMulti 批量设置
func (c *RistrettoCache) SetMulti(_ context.Context, items map[string]interface{}, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	ttl = c.jitterTTL(ttl)

	for key, value := range items {
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("marshal key %s: %w", key, err)
		}
		c.cache.SetWithTTL(key, data, int64(len(data)), ttl)
	}
	// 等待写入落地，保证调用方紧接着的 Get 能读到（与 Set 行为一致）
	c.cache.Wait()
	return nil
}

// Size 当前缓存大小（字节）
func (c *RistrettoCache) Size() int64 {
	return int64(c.cache.Metrics.CostAdded() - c.cache.Metrics.CostEvicted())
}

// HitRatio 命中率
func (c *RistrettoCache) HitRatio() float64 {
	hits := c.hitCount.Load()
	misses := c.missCount.Load()

	total := hits + misses
	if total == 0 {
		return 0
	}
	ratio := float64(hits) / float64(total)
	return math.Round(ratio*10000) / 10000
}

// Evictions 驱逐数量
func (c *RistrettoCache) Evictions() uint64 {
	return c.cache.Metrics.KeysEvicted()
}

// Hits 命中次数
func (c *RistrettoCache) Hits() uint64 {
	return c.hitCount.Load()
}

// Misses 未命中次数
func (c *RistrettoCache) Misses() uint64 {
	return c.missCount.Load()
}

// Close 关闭缓存
func (c *RistrettoCache) Close() {
	c.cache.Close()
}
