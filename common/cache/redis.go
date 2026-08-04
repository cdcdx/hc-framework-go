package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisStore L2 分布式缓存实现（Redis / Valkey 兼容）。
type redisStore struct {
	client redis.UniversalClient
	prefix string
}

// newRedisStore 创建 Redis 缓存实例。
func newRedisStore(cfg L2Config) *redisStore {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    cfg.Addresses,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
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
