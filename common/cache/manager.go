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

	"github.com/cdcdx/hc-framework-go/common/metrics"
	"github.com/zeromicro/go-zero/core/logx"
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

// defaultBloomExpectedKeys 布隆过滤器默认预期容量。
// 约占用 m = -n*ln(0.01)/(ln2)^2 ≈ 9.6 bit/key → 100 万 key 约 1.2 MB。
const defaultBloomExpectedKeys = 1_000_000

// Config 缓存配置
type Config struct {
	Enabled bool
	L1      L1Config
	L2      L2Config
	Bloom   BloomConfig
}

// BloomConfig 布隆过滤器配置。
// 布隆用于缓存穿透保护，只增不删，因此容量需按业务 key 总量预估：
// 实际插入量远超 ExpectedKeys 时假阳性率上升（穿透保护变弱），但**不会**
// 产生假阴性，故不影响数据正确性。
type BloomConfig struct {
	// ExpectedKeys 预期 key 总量，0 表示使用默认值 100 万。
	ExpectedKeys uint64
	// FalsePositiveRate 目标假阳性率，0 或越界时取 0.01。
	FalsePositiveRate float64
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

// L2Config 分布式缓存配置。
//
// Type 取值: "redis" | "valkey" | "none"。Valkey 是 Redis 协议兼容的
// drop-in 替代品，二者共用同一套 go-redis 客户端实现，仅在类型标识上区分。
type L2Config struct {
	Enabled   bool
	Type      string // redis / valkey / none
	Addresses []string
	Username  string
	Password  string
	DB        int
	PoolSize  int
	// DialTimeout / ReadTimeout / WriteTimeout 调优 redis/valkey 客户端。
	DialTimeout  time.Duration `json:",optional"`
	ReadTimeout  time.Duration `json:",optional"`
	WriteTimeout time.Duration `json:",optional"`
	// TLS 启用到 redis/valkey 服务器的 TLS 连接（如云托管实例）。
	TLS bool `json:",optional"`
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
		l1, err := newRistrettoStore(config.L1)
		if err != nil {
			// L1 初始化失败不致命：降级为 nilStore（仅 L1 不可用），
			// 避免进程因配置/资源问题直接崩溃。
			logx.Errorf("cache: L1 init failed, fallback to no-op L1: %v", err)
			m.l1 = &nilStore{}
		} else {
			m.l1 = l1
		}
	} else {
		m.l1 = &nilStore{}
	}

	// L2: Redis / Valkey 分布式缓存（二者协议兼容，共用实现）。
	if config.L2.Enabled && (config.L2.Type == "redis" || config.L2.Type == "valkey") && len(config.L2.Addresses) > 0 {
		m.l2 = newRedisStore(config.L2)
	} else {
		m.l2 = nil
	}

	// 布隆过滤器：保护缓存穿透。
	// 必须使用不淘汰的位数组实现，保证零假阴性（见 bloom.go 注释）。
	expected := config.Bloom.ExpectedKeys
	if expected == 0 {
		expected = defaultBloomExpectedKeys
	}
	m.bloom = newBitsetBloom(expected, config.Bloom.FalsePositiveRate)

	return m
}

// Get 从缓存获取值（先查 L1，未命中则查 L2 并回填 L1）。
func (m *Manager) Get(ctx context.Context, key string) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	// L1
	if val, err := m.l1.Get(ctx, key); err == nil && val != nil {
		metrics.CacheHitTotal.WithLabelValues("l1").Inc()
		return val, nil
	}
	metrics.CacheMissTotal.WithLabelValues("l1").Inc()

	// L2
	if m.l2 != nil {
		start := time.Now()
		val, err := m.l2.Get(ctx, key)
		metrics.CacheOperationDurationSeconds.WithLabelValues("get").Observe(time.Since(start).Seconds())
		if err == nil && val != nil {
			metrics.CacheHitTotal.WithLabelValues("l2").Inc()
			// 回填 L1（失败不影响返回值）
			_ = m.l1.Set(ctx, key, val, m.config.L1.DefaultTTL)
			return val, nil
		}
		metrics.CacheMissTotal.WithLabelValues("l2").Inc()
	}
	return nil, nil
}

// Set 同时写入 L1 和 L2。L2 失败仅记日志不报错（L1 已有数据，且 L1 有 TTL 最终一致）。
func (m *Manager) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if m == nil {
		return nil
	}
	m.bloom.Add(key)
	if err := m.l1.Set(ctx, key, value, ttl); err != nil {
		return err
	}
	if m.l2 != nil {
		start := time.Now()
		if err := m.l2.Set(ctx, key, value, ttl); err != nil {
			logx.Errorf("cache: L2 set failed for %s: %v", key, err)
		}
		metrics.CacheOperationDurationSeconds.WithLabelValues("set").Observe(time.Since(start).Seconds())
	}
	return nil
}

// Delete 同时删除 L1 和 L2。L2 失败记日志（L1 已删除，L2 脏数据由 TTL 兜底）。
func (m *Manager) Delete(ctx context.Context, key string) error {
	if m == nil {
		return nil
	}
	_ = m.l1.Delete(ctx, key)
	if m.l2 != nil {
		start := time.Now()
		if err := m.l2.Delete(ctx, key); err != nil {
			logx.Errorf("cache: L2 delete failed for %s: %v", key, err)
		}
		metrics.CacheOperationDurationSeconds.WithLabelValues("del").Observe(time.Since(start).Seconds())
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
		metrics.CacheBloomRejectTotal.Inc()
		return false
	}
	val, err := m.Get(ctx, key)
	return err == nil && val != nil
}

// Close 关闭 L2 连接（L1 是本地缓存，无需关闭）。
func (m *Manager) Close() error {
	if m == nil || m.l2 == nil {
		return nil
	}
	return m.l2.Close()
}
