package cache

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/redis/go-redis/v9"
)

// RedisCache L2 分布式缓存实现（Redis / Valkey 统一，协议兼容）
type RedisCache struct {
	client     redis.UniversalClient
	cfg        *config.L2CacheConfig
	serializer Serializer
	keyPrefix  string
}

// NewRedisCache 创建 Redis/Valkey 缓存实例
// 返回 nil 表示 L2 不可用（优雅降级）
func NewRedisCache(cfg *config.L2CacheConfig, keyPrefix string) (*RedisCache, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("l2 cache disabled")
	}

	// TCP 快速预检，避免 go-redis 连接池重试日志刷屏
	if len(cfg.Addresses) > 0 {
		addr := cfg.Addresses[0]
		dialer := net.Dialer{Timeout: 3 * time.Second}
		conn, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("redis dial %s: %w", addr, err)
		}
		conn.Close()
	}

	client := createRedisClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisCache{
		client:     client,
		cfg:        cfg,
		serializer: &JSONSerializer{},
		keyPrefix:  keyPrefix,
	}, nil
}

// createRedisClient 按配置创建 Redis 客户端（单机/哨兵/集群）
func createRedisClient(cfg *config.L2CacheConfig) redis.UniversalClient {
	maxRetries := 0 // 连接阶段不重试，运行期由 go-redis 内部管理

	if cfg.Cluster.Enabled {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Addresses,
			Password:     cfg.Password,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			MaxRedirects: 8,
			MaxRetries:   maxRetries,
		})
	}

	if cfg.Sentinel.Enabled {
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.Sentinel.MasterName,
			SentinelAddrs: cfg.Sentinel.Addresses,
			Password:      cfg.Password,
			DB:            cfg.DB,
			PoolSize:      cfg.PoolSize,
			MinIdleConns:  cfg.MinIdleConns,
			DialTimeout:   cfg.DialTimeout,
			ReadTimeout:   cfg.ReadTimeout,
			WriteTimeout:  cfg.WriteTimeout,
			MaxRetries:    maxRetries,
		})
	}

	return redis.NewClient(&redis.Options{
		Addr:         cfg.Addresses[0],
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		MaxRetries:   maxRetries,
	})
}

// prefixKey 拼接完整 Key（委托 cacheKey，与 Manager.l2Key 规则统一）。
func (c *RedisCache) prefixKey(key string) string {
	return cacheKey(c.keyPrefix, key)
}

// Get 获取缓存值
func (c *RedisCache) Get(ctx context.Context, key string) (interface{}, bool, error) {
	data, err := c.client.Get(ctx, c.prefixKey(key)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis get: %w", err)
	}

	// 空值标记检测（bytes.Equal 零分配，避免整段 value 转 string 的开销）
	if bytes.Equal(data, nullMarkerBytes) {
		return nil, true, nil // 命中空值缓存
	}

	var result interface{}
	if err := c.serializer.Unmarshal(data, &result); err != nil {
		return string(data), true, nil
	}
	return result, true, nil
}

// Set 设置缓存值
func (c *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := c.serializer.Marshal(value)
	if err != nil {
		return fmt.Errorf("redis set marshal: %w", err)
	}
	return c.client.Set(ctx, c.prefixKey(key), data, ttl).Err()
}

// SetNull 设置空值标记（防穿透）
func (c *RedisCache) SetNull(ctx context.Context, key string, ttl time.Duration) error {
	return c.client.Set(ctx, c.prefixKey(key), NullMarker, ttl).Err()
}

// Delete 删除缓存键
func (c *RedisCache) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	prefixed := make([]string, len(keys))
	for i, k := range keys {
		prefixed[i] = c.prefixKey(k)
	}
	// 集群模式下多 key 若不在同一 slot，直接 Del 会返回 CROSSSLOT 错误。
	// 改用 Pipeline：go-redis 集群客户端会按 key 的 slot 自动分组、分别向对应节点提交，
	// 因此本实现同时兼容单机与集群模式。
	pipe := c.client.Pipeline()
	for _, k := range prefixed {
		pipe.Del(ctx, k)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Exists 检查键是否存在
func (c *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.client.Exists(ctx, c.prefixKey(key)).Result()
	return n > 0, err
}

// GetMulti 批量获取（使用 Pipeline）
func (c *RedisCache) GetMulti(ctx context.Context, keys []string) (map[string]interface{}, error) {
	if len(keys) == 0 {
		return map[string]interface{}{}, nil
	}

	pipe := c.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Get(ctx, c.prefixKey(k))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis pipeline getmulti: %w", err)
	}

	result := make(map[string]interface{}, len(keys))
	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			continue
		}
		if bytes.Equal(data, nullMarkerBytes) {
			result[keys[i]] = nil
			continue
		}
		var val interface{}
		if err := c.serializer.Unmarshal(data, &val); err != nil {
			result[keys[i]] = string(data)
		} else {
			result[keys[i]] = val
		}
	}
	return result, nil
}

// SetMulti 批量设置（使用 Pipeline）
func (c *RedisCache) SetMulti(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	pipe := c.client.Pipeline()
	for key, value := range items {
		data, err := c.serializer.Marshal(value)
		if err != nil {
			return fmt.Errorf("redis setmulti marshal key %s: %w", key, err)
		}
		pipe.Set(ctx, c.prefixKey(key), data, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipeline setmulti: %w", err)
	}
	return nil
}

// TTL 获取键的剩余 TTL
func (c *RedisCache) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.client.TTL(ctx, c.prefixKey(key)).Result()
}

// Incr 原子递增
func (c *RedisCache) Incr(ctx context.Context, key string) (int64, error) {
	return c.client.Incr(ctx, c.prefixKey(key)).Result()
}

// IncrBy 原子递增指定值
func (c *RedisCache) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.client.IncrBy(ctx, c.prefixKey(key), value).Result()
}

// ────────── 分布式锁 ──────────

const unlockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

// randomToken 生成随机锁 token（防误解锁）
func randomToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Lock 分布式锁（SET NX EX），返回是否获取成功
// 返回的 token 用于后续解锁校验，防止误解锁
func (c *RedisCache) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	token := randomToken()
	ok, err := c.client.SetNX(ctx, c.prefixKey(key), token, ttl).Result()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	// 存储 token 到 map 以便后续解锁（用 key→token 映射）
	c.storeLockToken(key, token)
	return ok, nil
}

// lockTokens 存储锁 token 映射（内存级别，进程内使用）
var lockTokens sync.Map

func (c *RedisCache) storeLockToken(key, token string) {
	lockTokens.Store(c.prefixKey(key), token)
}

func (c *RedisCache) popLockToken(key string) string {
	v, ok := lockTokens.LoadAndDelete(c.prefixKey(key))
	if !ok {
		return ""
	}
	// 全局 lockTokens 只由本包 storeLockToken 写入 string，但仍加类型守卫避免脏 entry
	// 触发裸断言 panic（与 middleware/ratelimit.go cleanup 同源防御，13 §3.44）。
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// Unlock 释放锁（Lua 脚本保证原子性，校验 token 防止误解锁）
func (c *RedisCache) Unlock(ctx context.Context, key string) error {
	token := c.popLockToken(key)
	if token == "" {
		return nil // 未持有锁，无需释放
	}
	return c.client.Eval(ctx, unlockScript, []string{c.prefixKey(key)}, token).Err()
}

// refreshLockScript 仅当锁值仍为持有者 token 时延长 TTL（防止已过期被他人持有后误续）。
const refreshLockScript = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  redis.call('expire', KEYS[1], ARGV[2])
  return 1
else
  return 0
end
`

// peekLockToken 读取本地存储的锁 token（不删除），供 RefreshLock 校验“是否仍由本进程持有”。
func (c *RedisCache) peekLockToken(key string) string {
	v, ok := lockTokens.Load(c.prefixKey(key))
	if !ok {
		return ""
	}
	// 全局 lockTokens 只由本包 storeLockToken 写入 string，但仍加类型守卫避免脏 entry
	// 触发裸断言 panic（与 middleware/ratelimit.go cleanup 同源防御，13 §3.44）。
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// RefreshLock 续期：仅当本进程仍持有该锁（Redis 中值等于本地 token）时延长 TTL。
// 用于选主租约续期，避免“自我冲突”——直接 re-Lock(SetNX) 会因 key 已存在而失败，
// 误判为失去领导权。返回 true 表示续期成功（仍持有锁）。
func (c *RedisCache) RefreshLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	token := c.peekLockToken(key)
	if token == "" {
		return false, nil // 未持有锁
	}
	ok, err := c.client.Eval(ctx, refreshLockScript, []string{c.prefixKey(key)}, token, int(ttl.Seconds())).Bool()
	if err != nil {
		return false, err
	}
	if !ok {
		// 锁已易主或过期：清理本地 token，避免误续/误解锁
		c.popLockToken(key)
	}
	return ok, nil
}

// ────────── Pub/Sub 广播 ──────────

// Publish 发布消息到频道（带重试）
func (c *RedisCache) Publish(ctx context.Context, channel string, message interface{}) error {
	return c.client.Publish(ctx, channel, message).Err()
}

// Subscribe 订阅频道消息（自动重连）
func (c *RedisCache) Subscribe(ctx context.Context, channel string, handler func(msg string)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := c.subscribeLoop(ctx, channel, handler); err != nil {
				// 上下文取消则退出，否则等待后重连
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// 重连间隔：用 select 使等待可被 ctx 取消中断（Redis 长期不可达时，
				// 进程关闭不应再等满 2s 才退出，降低退出延迟）。
				timer := time.NewTimer(2 * time.Second)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
			}
		}
	}
}

// subscribeLoop 单次订阅循环（连接断开时返回 nil 以触发重连）
func (c *RedisCache) subscribeLoop(ctx context.Context, channel string, handler func(msg string)) error {
	pubsub := c.client.Subscribe(ctx, channel)
	defer func() {
		_ = pubsub.Close()
	}()

	// 等待订阅确认
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return fmt.Errorf("subscribe confirm: %w", err)
	}

	ch := pubsub.Channel()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return nil // 通道关闭，触发外层重连
			}
			handler(msg.Payload)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ────────── Pipeline 封装 ──────────

// redisPipeline 实现 Pipeline 接口
type redisPipeline struct {
	pipe redis.Pipeliner
}

// Pipeline 创建批量操作管道
func (c *RedisCache) Pipeline(ctx context.Context) (Pipeline, error) {
	return &redisPipeline{pipe: c.client.Pipeline()}, nil
}

func (p *redisPipeline) Get(ctx context.Context, key string) error {
	return p.pipe.Get(ctx, key).Err()
}

func (p *redisPipeline) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return p.pipe.Set(ctx, key, value, ttl).Err()
}

func (p *redisPipeline) Delete(ctx context.Context, keys ...string) error {
	return p.pipe.Del(ctx, keys...).Err()
}

func (p *redisPipeline) Exec(ctx context.Context) ([]interface{}, error) {
	cmders, err := p.pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]interface{}, len(cmders))
	for i, c := range cmders {
		results[i] = c
	}
	return results, nil
}

// Close 关闭连接池
func (c *RedisCache) Close() error {
	return c.client.Close()
}

// ────────── 集合（离线检测活跃会话登记） ──────────

// SAdd 向集合添加成员
func (c *RedisCache) SAdd(ctx context.Context, key string, member string) error {
	return c.client.SAdd(ctx, c.prefixKey(key), member).Err()
}

// SRem 从集合移除成员
func (c *RedisCache) SRem(ctx context.Context, key string, member string) error {
	return c.client.SRem(ctx, c.prefixKey(key), member).Err()
}

// SScan 扫描集合全部成员（SSCAN 自动翻页，避免阻塞 Redis）
func (c *RedisCache) SScan(ctx context.Context, key string) ([]string, error) {
	var out []string
	iter := c.client.SScan(ctx, c.prefixKey(key), 0, "", 0).Iterator()
	for iter.Next(ctx) {
		out = append(out, iter.Val())
	}
	return out, iter.Err()
}

// ConfigSet 运行时设置 Redis 配置项（如开启 Keyspace Notification: notify-keyspace-events Ex）
// 属 best-effort：云平台 Redis 可能禁止 CONFIG 命令，调用方需容忍失败。
func (c *RedisCache) ConfigSet(ctx context.Context, key, value string) error {
	return c.client.ConfigSet(ctx, key, value).Err()
}

// BatchExists 批量检查键是否存在（Pipeline 批量，单次 RTT 返回各键结果）。
// 离线检测 scanner 用它一次性判定一批成员的心跳 Key 是否存活，将“逐成员串行 Exists”
// 的 O(N) 次 RTT 降到 O(N/batch)（单实例百万活跃会话时从百万次 RTT 降至数百次）。
// 集群模式下 go-redis 按 key 的 slot 自动分组提交，单 key 命令不受 CROSSSLOT 限制。
func (c *RedisCache) BatchExists(ctx context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := c.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Exists(ctx, c.prefixKey(k))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis pipeline batchexists: %w", err)
	}
	out := make([]bool, len(keys))
	for i, cmd := range cmds {
		n, _ := cmd.Result()
		out[i] = n > 0
	}
	return out, nil
}
