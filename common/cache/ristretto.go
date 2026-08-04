package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// ristrettoStore L1 本地缓存实现。
type ristrettoStore struct {
	cache *ristretto.Cache[string, []byte]
}

// newRistrettoStore 创建 Ristretto 本地缓存实例。
// 参数来自 L1Config，NumCounters 建议为 MaxCost 的 10 倍。
func newRistrettoStore(cfg L1Config) *ristrettoStore {
	if cfg.NumCounters <= 0 {
		cfg.NumCounters = 10_000_000
	}
	if cfg.MaxCost <= 0 {
		cfg.MaxCost = 1 << 28 // 256 MB
	}
	if cfg.BufferItems <= 0 {
		cfg.BufferItems = 64
	}

	c, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: cfg.NumCounters,
		MaxCost:     cfg.MaxCost,
		BufferItems: cfg.BufferItems,
	})
	if err != nil {
		panic(fmt.Sprintf("ristretto: %v", err))
	}
	return &ristrettoStore{cache: c}
}

func (s *ristrettoStore) Get(ctx context.Context, key string) ([]byte, error) {
	val, ok := s.cache.Get(key)
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (s *ristrettoStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	ok := s.cache.SetWithTTL(key, value, int64(len(value)), ttl)
	if !ok {
		return fmt.Errorf("ristretto: set rejected (cost too high or race)")
	}
	// 确保写入完成（Ristretto 异步写入）
	s.cache.Wait()
	return nil
}

func (s *ristrettoStore) Delete(ctx context.Context, key string) error {
	s.cache.Del(key)
	return nil
}

func (s *ristrettoStore) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := s.cache.Get(key)
	return ok, nil
}

func (s *ristrettoStore) Close() error {
	s.cache.Close()
	return nil
}

// ---- nilStore: 缓存禁用时的 no-op 实现 ----
type nilStore struct{}

func (n *nilStore) Get(ctx context.Context, key string) ([]byte, error)    { return nil, nil }
func (n *nilStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return nil
}
func (n *nilStore) Delete(ctx context.Context, key string) error { return nil }
func (n *nilStore) Exists(ctx context.Context, key string) (bool, error) { return false, nil }
func (n *nilStore) Close() error                                  { return nil }
