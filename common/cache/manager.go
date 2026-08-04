// Package cache 两级缓存（L1 本地 + L2 分布式）+ 布隆过滤器。
// 对标 gin 版 common/cache 模块，当前 L1 用 Ristretto，L2 用 Redis。
//
// 使用方式:
//
//	svcCtx.Cache.Set(ctx, key, value, ttl)
//	svcCtx.Cache.Get(ctx, key)
//	svcCtx.Cache.Delete(ctx, key)
//	svcCtx.Cache.Exists(ctx, key)       // 布隆 + 缓存穿透保护
package cache

import (
	"context"
	"time"
)

// Item 缓存值
type Item struct {
	Key   string
	Value []byte
	TTL   time.Duration
}

// Manager 两级缓存管理器
type Manager struct {
	l1     CacheStore // 本地缓存（Ristretto / memory）
	l2     CacheStore // 分布式缓存（Redis / Valkey），nil 时仅 L1
	bloom  BloomFilter
	config Config
}

// Config 缓存配置
type Config struct {
	Enabled bool
	L1      L1Config
	L2      L2Config
}

// L1Config 本地缓存配置
type L1Config struct {
	Enabled     bool
	MaxMemoryMB int
	DefaultTTL  time.Duration
	NumCounters int64
	MaxCost     int64
	BufferItems int64
}

// L2Config 分布式缓存配置
type L2Config struct {
	Enabled   bool
	Type      string // redis / none
	Addresses []string
	Password  string
	DB        int
	PoolSize  int
}

// CacheStore 缓存存储接口
type CacheStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Close() error
}

// BloomFilter 布隆过滤器接口
type BloomFilter interface {
	Add(key string)
	MightContain(key string) bool
}

// New 创建两级缓存管理器。
// 若 config.Enabled=false 返回 nilManager（所有操作均为 no-op）。
// 若 L2 不可达则自动降级为仅 L1 模式（不报错，仅记日志）。
func New(config Config) *Manager {
	if !config.Enabled {
		return &Manager{l1: &nilStore{}, bloom: &nilBloom{}}
	}

	m := &Manager{config: config}

	// L1: Ristretto 本地缓存
	if config.L1.Enabled {
		m.l1 = newRistrettoStore(config.L1)
	} else {
		m.l1 = &nilStore{}
	}

	// L2: Redis 分布式缓存
	if config.L2.Enabled && config.L2.Type == "redis" && len(config.L2.Addresses) > 0 {
		m.l2 = newRedisStore(config.L2)
	} else {
		m.l2 = nil
	}

	// 布隆过滤器：保护缓存穿透
	m.bloom = newRistrettoBloom(config.L1)

	return m
}

// Get 从缓存获取值（先查 L1，未命中则查 L2 并回填 L1）。
func (m *Manager) Get(ctx context.Context, key string) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	// L1
	if val, err := m.l1.Get(ctx, key); err == nil && val != nil {
		return val, nil
	}
	// L2
	if m.l2 != nil {
		if val, err := m.l2.Get(ctx, key); err == nil && val != nil {
			// 回填 L1（失败不影响返回值）
			_ = m.l1.Set(ctx, key, val, m.config.L1.DefaultTTL)
			return val, nil
		}
	}
	return nil, nil
}

// Set 同时写入 L1 和 L2。L2 失败仅记日志不报错。
func (m *Manager) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if m == nil {
		return nil
	}
	m.bloom.Add(key)
	if err := m.l1.Set(ctx, key, value, ttl); err != nil {
		return err
	}
	if m.l2 != nil {
		_ = m.l2.Set(ctx, key, value, ttl)
	}
	return nil
}

// Delete 同时删除 L1 和 L2
func (m *Manager) Delete(ctx context.Context, key string) error {
	if m == nil {
		return nil
	}
	_ = m.l1.Delete(ctx, key)
	if m.l2 != nil {
		_ = m.l2.Delete(ctx, key)
	}
	return nil
}

// Exists 检查 key 是否存在（布隆 + 缓存穿透保护）。
// 布隆说"可能存在"则查缓存；布隆说"一定不存在"则直接返回 false，避免穿透。
func (m *Manager) Exists(ctx context.Context, key string) bool {
	if m == nil {
		return false
	}
	if !m.bloom.MightContain(key) {
		return false
	}
	_, err := m.Get(ctx, key)
	return err == nil
}

// Close 关闭 L2 连接（L1 是本地缓存，无需关闭）。
func (m *Manager) Close() error {
	if m == nil || m.l2 == nil {
		return nil
	}
	return m.l2.Close()
}
