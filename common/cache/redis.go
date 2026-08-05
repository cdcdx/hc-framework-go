package cache

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisStore L2 分布式缓存实现（Redis / Valkey 兼容）。
type redisStore struct {
	client redis.UniversalClient
	prefix string
}

// newRedisStore 创建 Redis/Valkey 缓存实例。
//
// Valkey 与 Redis 共用同一 wire protocol，因此同一 UniversalClient 实现
// 即可服务两者。此处透传 DialTimeout / ReadTimeout / WriteTimeout / TLS，
// 以便对接云托管实例或高延迟网络。
func newRedisStore(cfg L2Config) *redisStore {
	opts := &redis.UniversalOptions{
		Addrs:        cfg.Addresses,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	if cfg.TLS {
		// 默认使用安全最小配置，生产环境如需自定义 CA 可在此扩展。
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	rdb := redis.NewUniversalClient(opts)
	return &redisStore{client: rdb, prefix: "hc:"}
}

func (s *redisStore) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := s.client.Get(ctx, s.prefix+key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

func (s *redisStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.client.Set(ctx, s.prefix+key, value, ttl).Err()
}

func (s *redisStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.prefix+key).Err()
}

func (s *redisStore) Exists(ctx context.Context, key string) (bool, error) {
	n, err := s.client.Exists(ctx, s.prefix+key).Result()
	return n > 0, err
}

func (s *redisStore) Close() error {
	return s.client.Close()
}
