package cache

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/redis/go-redis/v9"
)

// RedisBloom 基于 Redis Bitmap 的布隆过滤器
type RedisBloom struct {
	client    redis.UniversalClient
	redisKey  string
	bitSize   int64  // 位数组总长度 m
	hashCount uint64 // 哈希函数数量 k
}

// NewRedisBloom 创建布隆过滤器
// capacity: 预期元素数量
// errorRate: 误判率（如 0.01）
// 自动计算最优位数组大小 m 和哈希函数数量 k
func NewRedisBloom(client redis.UniversalClient, cfg *config.BloomConfig) *RedisBloom {
	m, k := optimalBloomParams(cfg.Capacity, cfg.ErrorRate)
	return &RedisBloom{
		client:    client,
		redisKey:  cfg.RedisKey,
		bitSize:   m,
		hashCount: k,
	}
}

// optimalBloomParams 计算最优参数
// m = -n * ln(p) / (ln(2))^2  — 位数组大小
// k = (m/n) * ln(2)            — 哈希函数数量
func optimalBloomParams(n int64, p float64) (int64, uint64) {
	if n <= 0 {
		n = 1000000
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	m := int64(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
	k := uint64(math.Ceil(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	return m, k
}

// hashPositions 计算 k 个哈希位置（使用 double-hashing 技巧）
func (b *RedisBloom) hashPositions(data []byte) []int64 {
	h1, h2 := fnvHash(data)
	positions := make([]int64, b.hashCount)
	for i := uint64(0); i < b.hashCount; i++ {
		pos := int64((h1 + i*h2) % uint64(b.bitSize))
		positions[i] = pos
	}
	return positions
}

func fnvHash(data []byte) (uint64, uint64) {
	h := fnv.New64a()
	h.Write(data)
	h1 := h.Sum64()

	h.Reset()
	h.Write(append(data, 0x01))
	h2 := h.Sum64()
	return h1, h2
}

// Add 添加元素到布隆过滤器
func (b *RedisBloom) Add(ctx context.Context, key string) error {
	positions := b.hashPositions([]byte(key))
	pipe := b.client.Pipeline()
	for _, pos := range positions {
		pipe.SetBit(ctx, b.redisKey, pos, 1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// AddMulti 批量添加
func (b *RedisBloom) AddMulti(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	pipe := b.client.Pipeline()
	for _, key := range keys {
		for _, pos := range b.hashPositions([]byte(key)) {
			pipe.SetBit(ctx, b.redisKey, pos, 1)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Contains 检查元素是否可能存在（有误判率）
func (b *RedisBloom) Contains(ctx context.Context, key string) (bool, error) {
	positions := b.hashPositions([]byte(key))
	pipe := b.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(positions))
	for i, pos := range positions {
		cmds[i] = pipe.GetBit(ctx, b.redisKey, pos)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("bloom contains pipeline: %w", err)
	}

	for _, cmd := range cmds {
		bit, err := cmd.Result()
		if err != nil {
			return false, fmt.Errorf("bloom contains getbit: %w", err)
		}
		if bit == 0 {
			return false, nil // 确定不存在
		}
	}
	return true, nil // 可能存在
}

// Info 返回布隆过滤器参数信息
func (b *RedisBloom) Info() (bitSize int64, hashCount uint64) {
	return b.bitSize, b.hashCount
}
