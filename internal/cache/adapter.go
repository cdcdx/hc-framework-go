// Package cache 提供多级缓存（L1 内存 + L2 Redis）及防穿透/击穿/雪崩策略。
package cache

import (
	"context"
	"time"
)

// Cache 通用缓存接口
type Cache interface {
	// Get 获取缓存值
	Get(ctx context.Context, key string) (interface{}, bool, error)
	// Set 设置缓存值
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	// Delete 删除缓存键
	Delete(ctx context.Context, keys ...string) error
	// Exists 检查键是否存在
	Exists(ctx context.Context, key string) (bool, error)
	// GetMulti 批量获取
	GetMulti(ctx context.Context, keys []string) (map[string]interface{}, error)
	// SetMulti 批量设置
	SetMulti(ctx context.Context, items map[string]interface{}, ttl time.Duration) error
}

// L1Cache 本地缓存接口 (Ristretto)
type L1Cache interface {
	Cache
	// Size 当前缓存大小（字节）
	Size() int64
	// HitRatio 命中率
	HitRatio() float64
	// Evictions 驱逐数量
	Evictions() uint64
	// Close 释放底层资源（如 Ristretto 缓存）
	Close()
}

// L2Cache 分布式缓存接口 (Redis/Valkey)
type L2Cache interface {
	Cache
	// Pipeline 批量操作
	Pipeline(ctx context.Context) (Pipeline, error)
	// Lock 分布式锁
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// Unlock 释放锁
	Unlock(ctx context.Context, key string) error
	// RefreshLock 续期：仅当本进程仍持有锁时延长 TTL（选主租约用）
	RefreshLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// Publish 发布消息
	Publish(ctx context.Context, channel string, message interface{}) error
	// Subscribe 订阅消息
	Subscribe(ctx context.Context, channel string, handler func(msg string)) error
	// ConfigSet 运行时设置 Redis 配置项（best-effort，如开启 Keyspace Notification）
	ConfigSet(ctx context.Context, key, value string) error
	// SAdd 向集合添加成员
	SAdd(ctx context.Context, key string, member string) error
	// SRem 从集合移除成员
	SRem(ctx context.Context, key string, member string) error
	// SScan 扫描集合全部成员（SSCAN 自动翻页）
	SScan(ctx context.Context, key string) ([]string, error)
	// IncrBy 原子递增指定值（用于每日积分计数器等）。
	IncrBy(ctx context.Context, key string, value int64) (int64, error)
	// BatchExists 批量检查键是否存在（Pipeline 批量，单次 RTT 返回各键结果）。
	// 离线检测 scanner 用它替代“逐成员串行 Exists”，将百万级判活的 RTT 从 O(N) 降到 O(N/batch)。
	BatchExists(ctx context.Context, keys []string) ([]bool, error)
}

// Pipeline 批量操作接口
type Pipeline interface {
	Get(ctx context.Context, key string) error
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Exec(ctx context.Context) ([]interface{}, error)
}

// BloomFilter 布隆过滤器接口
type BloomFilter interface {
	// Add 添加元素
	Add(ctx context.Context, key string) error
	// AddMulti 批量添加
	AddMulti(ctx context.Context, keys []string) error
	// Contains 检查元素是否存在
	Contains(ctx context.Context, key string) (bool, error)
}

// Serializer 序列化接口
type Serializer interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}
