package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Manager 全局缓存管理器
type Manager struct {
	L1          L1Cache
	L2          L2Cache
	Bloom       BloomFilter
	HotKey      *HotKeyDetector
	Strategy    *CacheStrategy
	cm          *config.Manager
	log         *zap.Logger
	l2Client    redis.UniversalClient // 保留引用给 health check
	invalCancel context.CancelFunc    // 用于优雅关闭广播失效监听协程
}

// NewManager 创建缓存管理器
// L2 不可用时优雅降级，不影响服务启动。cm 为配置管理器（连接级配置取快照，
// 读写降级开关由 Strategy 实时读取以支持热更新）。
func NewManager(cm *config.Manager, log *zap.Logger) (*Manager, error) {
	cfg := cm.Get().Cache
	m := &Manager{
		cm:  cm,
		log: log,
	}

	// 初始化 L1
	l1, err := NewRistrettoCache(&cfg.L1, cfg.L1.DefaultTTL, cfg.TTLJitter)
	if err != nil {
		log.Warn("L1 cache init failed, running without L1", zap.Error(err))
	} else {
		m.L1 = l1
		log.Info("L1 cache initialized",
			zap.Int("max_memory_mb", cfg.L1.MaxMemoryMB),
			zap.Duration("default_ttl", cfg.L1.DefaultTTL),
		)
	}

	// 初始化 L2
	l2, err := NewRedisCache(&cfg.L2, cfg.KeyPrefix)
	if err != nil {
		log.Warn("L2 cache unavailable, running without L2",
			zap.String("type", cfg.L2.Type),
			zap.Error(err),
		)
	} else {
		m.L2 = l2
		m.l2Client = l2.client
		log.Info("L2 cache initialized",
			zap.String("type", cfg.L2.Type),
			zap.Int("pool_size", cfg.L2.PoolSize),
		)
	}

	// 初始化布隆过滤器（依赖 L2 Redis 连接）
	if cfg.Bloom.Enabled && m.l2Client != nil {
		m.Bloom = NewRedisBloom(m.l2Client, &cfg.Bloom)
		log.Info("Bloom filter initialized",
			zap.Int64("capacity", cfg.Bloom.Capacity),
			zap.Float64("error_rate", cfg.Bloom.ErrorRate),
		)
	}

	// 初始化热点 Key 检测器
	if cfg.HotKey.Enabled {
		m.HotKey = NewHotKeyDetector(cfg.HotKey.WindowSeconds, cfg.HotKey.TopN, cfg.HotKey.TTLMultiplier)
		log.Info("HotKey detector initialized",
			zap.Int("window_seconds", cfg.HotKey.WindowSeconds),
			zap.Int("top_n", cfg.HotKey.TopN),
		)
	}

	// 初始化策略引擎
	m.Strategy = NewCacheStrategy(m.L1, m.L2, m.Bloom, m.HotKey, cm, log)

	// 启动广播失效监听（使用可取消的 context，便于优雅关闭）
	if m.L2 != nil {
		invalCtx, cancel := context.WithCancel(context.Background())
		m.invalCancel = cancel
		m.Strategy.StartInvalidationListener(invalCtx)
	}

	return m, nil
}

// Close 关闭缓存管理器
// 依次：取消广播失效监听 → 关闭 L1 本地缓存 → 关闭 L2 连接池
func (m *Manager) Close() {
	m.log.Info("Shutting down cache manager...")

	// 1. 取消广播失效监听协程（否则 Subscribe 会随 context.Background 永久泄漏）
	if m.invalCancel != nil {
		m.invalCancel()
	}

	// 2. 关闭 L1 本地缓存，释放 Ristretto 资源
	if m.L1 != nil {
		m.L1.Close()
	}

	// 3. 关闭 L2 连接池
	if m.L2 != nil {
		if rc, ok := m.L2.(*RedisCache); ok {
			rc.Close()
		}
	}
	m.log.Info("Cache manager closed")
}

// SetDegradeSender 注入写降级事件发送器（MQ 生产者就绪后由 main.go 调用）。
// 未注入时，写降级退化为本地异步写 L2。
func (m *Manager) SetDegradeSender(sender DegradeEventSender) {
	if m.Strategy != nil {
		m.Strategy.SetDegradeSender(sender)
	}
}

// ────────── TokenBlacklistChecker 接口实现 ──────────

const blacklistPrefix = "jwt:blacklist"

// IsBlacklisted 检查 Token JTI 是否在黑名单
func (m *Manager) IsBlacklisted(ctx context.Context, jti string) (bool, error) {
	if m.L2 == nil {
		return false, nil
	}
	return m.L2.Exists(ctx, blacklistPrefix+":"+jti)
}

// IsBlacklistedMulti 批量检查多个 JTI/键是否在黑名单，单次 RTT（Pipeline）返回各键结果。
// 鉴权中间件用它一次性判定 jti 与 pwd_change 两个键，避免两次独立 Redis 往返。
// L2 不可用时返回全 false（同 IsBlacklisted 的降级语义）。
func (m *Manager) IsBlacklistedMulti(ctx context.Context, jtis ...string) ([]bool, error) {
	out := make([]bool, len(jtis))
	if m.L2 == nil {
		return out, nil
	}
	keys := make([]string, len(jtis))
	for i, j := range jtis {
		keys[i] = blacklistPrefix + ":" + j
	}
	existed, err := m.L2.BatchExists(ctx, keys)
	if err != nil {
		return out, err
	}
	copy(out, existed)
	return out, nil
}

// AddBlacklist 添加 Token JTI 到黑名单
func (m *Manager) AddBlacklist(ctx context.Context, jti string, ttl time.Duration) error {
	if m.L2 == nil {
		return fmt.Errorf("L2 cache not available")
	}
	return m.L2.Set(ctx, blacklistPrefix+":"+jti, time.Now().Unix(), ttl)
}

// ────────── AccountLockStore 接口实现（跨 Pod 共享的账号锁定） ──────────
//
// 复用现有 L2 Redis 连接，单 key Hash 存储：
//   {login:fail:<sha256(email)>}  ->  field "count": 连续失败次数（HINCRBY 原子递增，TTL 自动清理）
//                                ->  field "lock" : 锁定截止时间（Unix 纳秒，HSET，TTL = 锁定时长）
// 花括号为 Redis Cluster hash tag，保证计数/锁定字段同 slot，使 Lua 脚本可原子操作。
// Redis 不可用时，调用方会回退到进程内实现（auth.NewMemoryAccountLockStore），
// 因此本实现仅在 L2Enabled() 时由 main 注入 AuthService。

const (
	// accountLockKeyPrefix 账号锁定单 key 的逻辑前缀。
	accountLockKeyPrefix = "login:fail"
)

// accountLockKey 返回单 key 完整逻辑名（含 hash tag，便于集群单 slot）。
func (m *Manager) accountLockKey(email string) string {
	return "{" + accountLockKeyPrefix + ":" + lockKeyHash(email) + "}"
}

// GetFailureCount 返回连续失败次数（跨 Pod 共享，诊断用）。
func (m *Manager) GetFailureCount(ctx context.Context, email string) (int, error) {
	if m.L2 == nil {
		return 0, fmt.Errorf("L2 cache not available")
	}
	n, err := m.l2Client.HGet(ctx, m.l2Key(m.accountLockKey(email)), "count").Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// recordFailureScript 原子记录一次登录失败：
//   - HINCRBY 递增失败计数（单 key Hash，天然集群单 slot）；
//   - 首次写入设置 countTTL，避免无限增长；
//   - 达到 maxFailures 时 HSET 锁定截止时间，并将整 key TTL 收紧为 lockTTL（锁定期间不被清理）；
//   - 返回 [新计数, 是否本次触发锁定]。
//
// 单次往返、Redis 端原子执行，消除“计数达阈值却未写入锁定”的竞态。
var recordFailureScript = redis.NewScript(`
local key = KEYS[1]
local c = redis.call('HINCRBY', key, 'count', 1)
if c == 1 then
  redis.call('EXPIRE', key, ARGV[1])
end
local locked = 0
if tonumber(ARGV[2]) > 0 and c >= tonumber(ARGV[2]) then
  redis.call('HSET', key, 'lock', ARGV[3])
  redis.call('EXPIRE', key, ARGV[4])
  locked = 1
end
return {c, locked}
`)

// RecordFailure 原子记录一次登录失败（跨 Pod 共享）。
func (m *Manager) RecordFailure(ctx context.Context, email string, maxFailures int, countTTL, lockTTL time.Duration) (int, bool, error) {
	if m.L2 == nil {
		return 0, false, fmt.Errorf("L2 cache not available")
	}
	res, err := recordFailureScript.Run(ctx, m.l2Client,
		[]string{m.l2Key(m.accountLockKey(email))},
		int(countTTL.Seconds()),
		maxFailures,
		strconv.FormatInt(time.Now().Add(lockTTL).UnixNano(), 10),
		int(lockTTL.Seconds()),
	).Result()
	if err != nil {
		return 0, false, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 2 {
		return 0, false, fmt.Errorf("unexpected record-failure result: %v", res)
	}
	c, _ := arr[0].(int64)
	l, _ := arr[1].(int64)
	return int(c), l == 1, nil
}

// GetLockUntil 返回锁定截止时间（零值表示未锁定或已过期）。
func (m *Manager) GetLockUntil(ctx context.Context, email string) (time.Time, error) {
	if m.L2 == nil {
		return time.Time{}, fmt.Errorf("L2 cache not available")
	}
	s, err := m.l2Client.HGet(ctx, m.l2Key(m.accountLockKey(email)), "lock").Result()
	if err == redis.Nil {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	until := time.Unix(0, n)
	if until.Before(time.Now()) {
		return time.Time{}, nil
	}
	return until, nil
}

// Clear 清除失败计数与锁定（登录成功时，单 key DEL）。
func (m *Manager) Clear(ctx context.Context, email string) error {
	if m.L2 == nil {
		return fmt.Errorf("L2 cache not available")
	}
	return m.l2Client.Del(ctx, m.l2Key(m.accountLockKey(email))).Err()
}

// cacheKey 拼接完整 Redis Key（prefix 非空时加前缀）。
// 与 RedisCache.prefixKey 共用同一规则，避免两处 Key 拼接逻辑漂移导致锁定/缓存 key 不一致。
func cacheKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + ":" + key
}

// l2Key 拼接带前缀的完整 Redis Key（委托 cacheKey，规则与 RedisCache.prefixKey 一致）。
func (m *Manager) l2Key(logical string) string {
	return cacheKey(m.cm.Get().Cache.KeyPrefix, logical)
}

// lockKeyHash 对邮箱做 SHA-256，避免特殊字符直接进入 Redis Key。
func lockKeyHash(email string) string {
	sum := sha256.Sum256([]byte(email))
	return hex.EncodeToString(sum[:])
}

// ────────── 便捷方法 ──────────

// Get 三级缓存读取（委托给 Strategy）
func (m *Manager) Get(ctx context.Context, key string, ttl time.Duration, loader LoadFn) (interface{}, error) {
	return m.Strategy.Get(ctx, key, ttl, loader)
}

// Set 写入缓存（延迟双删）
func (m *Manager) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return m.Strategy.Set(ctx, key, value, ttl)
}

// Delete 删除缓存
func (m *Manager) Delete(ctx context.Context, keys ...string) error {
	return m.Strategy.Delete(ctx, keys...)
}

// ────────── 每日挂机积分：原子封顶预占（修复结算 TOCTOU） ──────────
//
// 原实现先在事务外读 GetDailyPoints 计算剩余额度（封顶），事务提交后才 IncrDailyPoints 累加 Redis 计数。
// 两个并发结算会读到相同的 alreadyEarned，各自认为有剩余额度并分别写 points，导致 Redis 计数与 DB
// 聚合双双超过 DailyPointsLimit。改为「预占-确认/回退」：用 Redis Lua 脚本在单线程内原子地计算
// 本次可授予积分（封顶到 limit），授予即累加计数；事务提交成功则保留（确认），事务失败或被乐观锁
// 跳过则 DECRBY 回退（释放额度）。详见 idle_service.settleSession / repository.IdleRepository。

// IdleDailyPointsKey 当日已得挂机积分计数器（Redis）逻辑 Key。Key 含本地日期，自然按日分片、次日失效。
// 导出供 repository 复用，保证 Key 命名单一来源。
func IdleDailyPointsKey(userID string) string {
	return fmt.Sprintf("idle:dailypts:%s:%s", userID, time.Now().Format("2006-01-02"))
}

// tryAcquireDailyPointsScript 原子封顶预占 Lua 脚本。
//
//	KEYS[1]=计数器 Key；ARGV[1]=want, ARGV[2]=limit, ARGV[3]=ttl(秒)
//	返回实际授予积分（已累加进计数器；首次创建时设置 TTL 至当日结束+1h）。
var tryAcquireDailyPointsScript = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local want = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local granted
if cur + want <= limit then
  granted = want
else
  granted = limit - cur
end
if granted <= 0 then
  return 0
end
redis.call('INCRBY', KEYS[1], granted)
if redis.call('TTL', KEYS[1]) < 0 then
  redis.call('EXPIRE', KEYS[1], ttl)
end
return granted
`)

// endOfLocalDay 返回距本地时区当日 24:00 的剩余时长（+1h 缓冲，用于每日计数器 TTL）。
func endOfLocalDay() time.Duration {
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Add(24 * time.Hour)
	return end.Sub(now) + time.Hour
}

// TryAcquireDailyPoints 原子预占当日挂机积分额度（封顶到 limit）。
// L2 不可用或 limit<=0 时直接返回 want（不限额、由乐观锁兜底）。
// granted 已计入 Redis 计数器；调用方事务成功则保留，失败/被乐观锁跳过则调 ReleaseDailyPoints 回退。
func (m *Manager) TryAcquireDailyPoints(ctx context.Context, userID string, want, limit int64) (int64, error) {
	if m.L2 == nil || m.l2Client == nil || limit <= 0 {
		return want, nil
	}
	key := m.l2Key(IdleDailyPointsKey(userID))
	granted, err := tryAcquireDailyPointsScript.Run(ctx, m.l2Client, []string{key}, want, limit, int(endOfLocalDay().Seconds())).Int64()
	if err != nil {
		return 0, err
	}
	return granted, nil
}

// ReleaseDailyPoints 回退预占的每日积分额度（DECRBY），事务失败或被乐观锁跳过时释放额度。
func (m *Manager) ReleaseDailyPoints(ctx context.Context, userID string, granted int64) error {
	if m.L2 == nil || m.l2Client == nil || granted <= 0 {
		return nil
	}
	return m.l2Client.DecrBy(ctx, m.l2Key(IdleDailyPointsKey(userID)), granted).Err()
}

// ────────── 定时抢购：Redis 原子预扣库存（削峰快速拦截层） ──────────
//
// 抢购最终以 DB 条件更新为权威防超卖，Redis 预扣是前置的削峰层：在到达 DB 前用单线程
// 原子脚本快速拦截「超出限量」与「超出每人限购」的请求，直接返回不入库，避免海量无效请求
// 打到 DB。L2 不可用时降级为纯 DB 兜底（由调用方处理），不阻断抢购。

// FlashSaleStockKey 抢购活动剩余库存计数器 Key。
func FlashSaleStockKey(activityID int64) string {
	return fmt.Sprintf("flashsale:stock:%d", activityID)
}

// FlashSaleUserKey 抢购活动每人已抢数量 Hash Key（field=userID）。含 hash tag 与 stock 同 slot。
func FlashSaleUserKey(activityID int64) string {
	return fmt.Sprintf("flashsale:users:%d", activityID)
}

// tryAcquireFlashSaleScript 原子预扣一个抢购名额。
//
//	KEYS[1]=stock 计数器；KEYS[2]=每人已抢 Hash
//	ARGV[1]=limitQty(首次初始化用), ARGV[2]=userID, ARGV[3]=perUserLimit, ARGV[4]=ttl(秒)
//	返回：>=0 成功（值=剩余库存）；-1 已抢光；-2 超出每人限购。
var tryAcquireFlashSaleScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[4])
end
local perLimit = tonumber(ARGV[3])
if perLimit > 0 then
  local ucnt = tonumber(redis.call('HGET', KEYS[2], ARGV[2]) or '0')
  if ucnt >= perLimit then
    return -2
  end
end
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
  return -1
end
redis.call('DECR', KEYS[1])
redis.call('HINCRBY', KEYS[2], ARGV[2], 1)
if redis.call('TTL', KEYS[2]) < 0 then
  redis.call('EXPIRE', KEYS[2], ARGV[4])
end
return stock - 1
`)

// FlashSaleAcquireResult Redis 预扣结果语义。
type FlashSaleAcquireResult int

const (
	FlashSaleOK        FlashSaleAcquireResult = iota // 预扣成功
	FlashSaleSoldOut                                 // 已抢光
	FlashSaleUserLimit                               // 超出每人限购
	FlashSaleSkipped                                 // L2 不可用，跳过（由 DB 兜底）
)

// TryAcquireFlashSale 原子预扣一个抢购名额。L2 不可用时返回 FlashSaleSkipped，交由 DB 兜底。
// remaining 为预扣成功后的剩余库存（仅 result==FlashSaleOK 时有意义）。
func (m *Manager) TryAcquireFlashSale(ctx context.Context, activityID int64, userID string, limitQty, perUserLimit int, ttl time.Duration) (result FlashSaleAcquireResult, remaining int64, err error) {
	if m.L2 == nil || m.l2Client == nil {
		return FlashSaleSkipped, 0, nil
	}
	if perUserLimit <= 0 {
		perUserLimit = 1
	}
	keys := []string{m.l2Key(FlashSaleStockKey(activityID)), m.l2Key(FlashSaleUserKey(activityID))}
	n, err := tryAcquireFlashSaleScript.Run(ctx, m.l2Client, keys, limitQty, userID, perUserLimit, int(ttl.Seconds())).Int64()
	if err != nil {
		return FlashSaleSkipped, 0, err
	}
	switch {
	case n == -1:
		return FlashSaleSoldOut, 0, nil
	case n == -2:
		return FlashSaleUserLimit, 0, nil
	default:
		return FlashSaleOK, n, nil
	}
}

// releaseFlashSaleScript 回滚一次预扣（DB 兜底判定实际售罄/事务失败时调用）。
//
//	KEYS[1]=stock；KEYS[2]=每人已抢 Hash；ARGV[1]=userID
var releaseFlashSaleScript = redis.NewScript(`
redis.call('INCR', KEYS[1])
local u = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '0')
if u > 0 then
  redis.call('HINCRBY', KEYS[2], ARGV[1], -1)
end
return 1
`)

// ReleaseFlashSale 回滚一次 Redis 预扣（当 DB 权威判定售罄或入库事务失败时释放名额）。
func (m *Manager) ReleaseFlashSale(ctx context.Context, activityID int64, userID string) error {
	if m.L2 == nil || m.l2Client == nil {
		return nil
	}
	keys := []string{m.l2Key(FlashSaleStockKey(activityID)), m.l2Key(FlashSaleUserKey(activityID))}
	return releaseFlashSaleScript.Run(ctx, m.l2Client, keys, userID).Err()
}

// WarmupFlashSale 活动开始前预热：将 Redis 剩余库存初始化为 limitQty（幂等，仅当不存在时写入）。
// 便于运营在开抢前把库存推入 Redis，避免首个请求承担初始化开销。
func (m *Manager) WarmupFlashSale(ctx context.Context, activityID int64, limitQty int, ttl time.Duration) error {
	if m.L2 == nil || m.l2Client == nil {
		return nil
	}
	return m.l2Client.SetNX(ctx, m.l2Key(FlashSaleStockKey(activityID)), limitQty, ttl).Err()
}

// FlashActivityCacheKey 抢购活动元信息缓存 Key（三级缓存，配合 cacheMgr.Get/Set 读写）。
func FlashActivityCacheKey(activityID int64) string {
	return fmt.Sprintf("flashsale:activity:%d", activityID)
}

// FlashActivityListCacheKey 进行中抢购活动列表缓存 Key（三级缓存）。
func FlashActivityListCacheKey() string {
	return "flashsale:activities:active"
}

// FlashUserFlashCountCacheKey 用户在某抢购活动下的「已成功订单数」缓存 Key（短 TTL，每人限购兜底计数）。
func FlashUserFlashCountCacheKey(activityID int64, userID string) string {
	return fmt.Sprintf("flashsale:count:%d:%s", activityID, userID)
}

// FlashUserFlashCountTTL 每人限购兜底计数（CountUserFlashOrders）的缓存 TTL。取较短（3s）以在
// 「开抢瞬间 1000 VU 并发查计数」与「成功下单后计数即时刷新（见 ShopService.FlashRedeem 失效该 key）」
// 之间平衡：TTL 内重试命中缓存不再回源，切断「报错-重试」对 DB 连接池的放大；成功下单即失效，
// 保证兜底计数不会放行同人短时间内二次抢购。
const FlashUserFlashCountTTL = 3 * time.Second

// ReconcileFlashSale 将 Redis 库存计数器强制重置为权威剩余值 remaining（=limit_qty - sold_qty）。
// 用途：
//   - 开抢前预热：首跑即把库存推入 Redis（等价于 WarmupFlashSale 的强制版）；
//   - 运行期/事故后库存对账自愈：消除「进程崩溃导致 Redis 预扣未回滚」等造成的 Redis 库存漂移，
//     使 Redis 剩余值与 DB 权威值保持一致。
//
// 仅重置库存计数，不动每人已抢 Hash（每人限购以 DB 订单数权威校验兜底，Redis 计数仅作削峰）。
// L2 不可用时为 no-op（降级为纯 DB 兜底）。
func (m *Manager) ReconcileFlashSale(ctx context.Context, activityID int64, remaining int64, ttl time.Duration) error {
	if m.L2 == nil || m.l2Client == nil {
		return nil
	}
	if remaining < 0 {
		remaining = 0
	}
	return m.l2Client.Set(ctx, m.l2Key(FlashSaleStockKey(activityID)), remaining, ttl).Err()
}

// ────────── 普通商品兑换：Redis 原子预扣库存（削峰快速拦截层） ──────────
//
// 与普通抢购(FlashSale)同理：DB 条件更新（WHERE stock>=quantity）为权威防超卖，Redis 预扣是
// 前置削峰层，在到达 DB 前用单线程原子脚本快速拦截「已售罄」请求，直接返回不入库，避免海量无效
// 请求打到 DB 行锁/连接池。这正是 shop-flash 压测中 163714 次「库存不足」空刀风暴的根因——
// 库存售罄后这些请求仍走完整链路直到 DeductStock 行锁失败。L2 不可用时降级为纯 DB 兜底。

// RedeemStockKey 普通商品剩余库存计数器 Key。
func RedeemStockKey(itemID int64) string {
	return fmt.Sprintf("redeem:stock:%d", itemID)
}

// tryAcquireRedeemScript 原子预扣一个普通商品兑换名额。
//
//	KEYS[1]=stock 计数器；ARGV[1]=seedStock(首次初始化用), ARGV[2]=ttl(秒)
//	返回：>=0 成功（值=剩余库存）；-1 已售罄。
//
// 仅在 key 不存在时初始化为 seed（首次见到该商品）。运行期不再因「Redis 当前值 < seed」
// 而自动重置——否则运营把库存调大后，削峰门禁会被重新放开（旧值落后时误把上限抬回 seed），
// 失去削峰意义。库存变更（运营改库存、压测重置等）须显式调 ReconcileRedeem，以 DB 权威值
// 对齐 Redis 计数器，详见 ReconcileRedeem。
var tryAcquireRedeemScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
  return -1
end
redis.call('DECR', KEYS[1])
return stock - 1
`)

// RedeemAcquireResult Redis 预扣结果语义。
type RedeemAcquireResult int

const (
	RedeemOK      RedeemAcquireResult = iota // 预扣成功
	RedeemSoldOut                            // 已售罄
	RedeemSkipped                            // L2 不可用，跳过（由 DB 兜底）
)

// TryAcquireRedeem 原子预扣一个普通商品兑换名额。L2 不可用时返回 RedeemSkipped，交由 DB 兜底。
// remaining 为预扣成功后的剩余库存（仅 result==RedeemOK 时有意义）。
func (m *Manager) TryAcquireRedeem(ctx context.Context, itemID int64, seedStock int, ttl time.Duration) (result RedeemAcquireResult, remaining int64, err error) {
	if m.L2 == nil || m.l2Client == nil {
		return RedeemSkipped, 0, nil
	}
	n, err := tryAcquireRedeemScript.Run(ctx, m.l2Client, []string{m.l2Key(RedeemStockKey(itemID))}, seedStock, int(ttl.Seconds())).Int64()
	if err != nil {
		return RedeemSkipped, 0, err
	}
	switch {
	case n == -1:
		return RedeemSoldOut, 0, nil
	default:
		return RedeemOK, n, nil
	}
}

// releaseRedeemScript 回滚一次预扣（DB 兜底判定实际售罄/事务失败时调用）。
var releaseRedeemScript = redis.NewScript(`
redis.call('INCR', KEYS[1])
return 1
`)

// ReleaseRedeem 回滚一次 Redis 预扣（当 DB 权威判定售罄或入库事务失败时释放名额）。
func (m *Manager) ReleaseRedeem(ctx context.Context, itemID int64) error {
	if m.L2 == nil || m.l2Client == nil {
		return nil
	}
	return releaseRedeemScript.Run(ctx, m.l2Client, []string{m.l2Key(RedeemStockKey(itemID))}).Err()
}

// ReconcileRedeem 将 Redis 库存计数器强制重置为权威剩余值 remaining（=DB 当前 stock）。
// 运营手动改库存后调用，消除 Redis 预扣漂移，使 Redis 剩余值与 DB 权威值保持一致。
// L2 不可用时为 no-op（降级为纯 DB 兜底）。
func (m *Manager) ReconcileRedeem(ctx context.Context, itemID int64, remaining int64, ttl time.Duration) error {
	if m.L2 == nil || m.l2Client == nil {
		return nil
	}
	if remaining < 0 {
		remaining = 0
	}
	return m.l2Client.Set(ctx, m.l2Key(RedeemStockKey(itemID)), remaining, ttl).Err()
}

// IsRedeemSoldOut 判断普通商品兑换是否已在 Redis 削峰层被置为售罄：redeem:stock 键存在且值<=0。
// 仅当键已存在时返回 true；键不存在表示该商品尚未预扣/仍在售，交由后续链路（FindItemByID + 行锁）判定。
// 用途：对齐抢购(FlashSale)，在 FindItemByID/DB 行锁之前以一次只读 GET 拦截「已售罄」空刀，
// 避免海量无效请求打到 DB（shop k6 压测中 33 万次库存不足请求穿透到 DeductStock 行锁的根因）。
// 计数器为 0 仅出现在商品真售罄时（成功兑换递减至 0 / ReconcileRedeem 置 0 / DB 权威售罄不回滚硬置 0），
// 不会误拦在售商品。L2 不可用时返回 false（降级为纯 DB 兜底）。
func (m *Manager) IsRedeemSoldOut(ctx context.Context, itemID int64) bool {
	if m.L2 == nil || m.l2Client == nil {
		return false
	}
	key := m.l2Key(RedeemStockKey(itemID))
	n, err := m.l2Client.Get(ctx, key).Int()
	if err != nil {
		return false
	}
	return n <= 0
}

// Lock 获取分布式锁（委托 L2 Redis）。
// 缓存未启用（L2 为 nil）时直接返回成功，由业务层乐观锁兜底，避免阻断兑换流程。
func (m *Manager) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if m.L2 == nil {
		return true, nil
	}
	return m.L2.Lock(ctx, key, ttl)
}

// DecodeCached 将 cacheMgr.Get 返回的 interface{} 还原为具体类型 T。
//
// 背景：三级缓存的 L1(Ristretto) 与 L2(Redis) 均以 JSON 字节存储值（见 l1.go / l2.go），
// 命中时反序列化为通用中间类型：JSON 对象 → map[string]interface{}，JSON 数组 → []interface{}。
// 只有"缓存未命中、由回源函数直接返回"这一路径拿到的才是原始 Go 类型 T。
// 因此调用方若直接对命中结果做 val.(*T) / val.([]T) 断言，会在缓存命中时 panic
// （被熔断器 recover 后表现为 "internal server error"）。本 helper 统一兼容：
//   - val 为 nil（未命中 / 空值缓存）：返回类型零值；
//   - val 已是 T（回源直返）：直接返回，零拷贝；
//   - 其他（map / 切片等 JSON 反序列化中间类型）：经 JSON 中转还原为 T，
//     既支持单对象（*model.X）也支持切片（[]model.X）。
func DecodeCached[T any](val interface{}) (T, error) {
	var zero T
	if val == nil {
		return zero, nil
	}
	if v, ok := val.(T); ok {
		return v, nil
	}
	// 命中缓存时为 JSON 反序列化的中间类型（map[string]interface{} / []interface{} 等），
	// 经 JSON 中转还原为具体类型 T。
	b, err := json.Marshal(val)
	if err != nil {
		return zero, fmt.Errorf("cache: marshal cached value (%T): %w", val, err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		return zero, fmt.Errorf("cache: unmarshal cached value to %T: %w", zero, err)
	}
	return out, nil
}

// Unlock 释放分布式锁（委托 L2 Redis）。缓存未启用时为 no-op。
func (m *Manager) Unlock(ctx context.Context, key string) error {
	if m.L2 == nil {
		return nil
	}
	return m.L2.Unlock(ctx, key)
}

// RefreshLock 续期（委托 L2 Redis）。缓存未启用时直接返回成功（乐观锁兜底）。
func (m *Manager) RefreshLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if m.L2 == nil {
		return true, nil
	}
	return m.L2.RefreshLock(ctx, key, ttl)
}

// Stats 返回缓存统计信息
func (m *Manager) Stats() map[string]interface{} {
	stats := make(map[string]interface{})

	if m.L1 != nil {
		stats["l1_enabled"] = true
		stats["l1_size_bytes"] = m.L1.Size()
		stats["l1_hit_ratio"] = m.L1.HitRatio()
		stats["l1_evictions"] = m.L1.Evictions()
	} else {
		stats["l1_enabled"] = false
	}

	if m.L2 != nil {
		stats["l2_enabled"] = true
	} else {
		stats["l2_enabled"] = false
	}

	if m.Bloom != nil {
		stats["bloom_enabled"] = true
	} else {
		stats["bloom_enabled"] = false
	}

	if m.HotKey != nil {
		stats["hotkey_enabled"] = true
		stats["hotkey_count"] = len(m.HotKey.GetHotKeys())
	} else {
		stats["hotkey_enabled"] = false
	}

	return stats
}

// 以下访问方法供 metrics 采集器读取缓存运行时统计（nil 安全）。

// L1Enabled 返回 L1 是否已启用。
func (m *Manager) L1Enabled() bool { return m.L1 != nil }

// L2Enabled 返回 L2 是否已启用。
func (m *Manager) L2Enabled() bool { return m.L2 != nil }

// BloomEnabled 返回布隆过滤器是否已启用。
func (m *Manager) BloomEnabled() bool { return m.Bloom != nil }

// HotKeyEnabled 返回热点 Key 检测是否已启用。
func (m *Manager) HotKeyEnabled() bool { return m.HotKey != nil }

// L1Hits 返回 L1 累计命中次数（L1 未启用时返回 0）。
func (m *Manager) L1Hits() uint64 {
	if m.L1 == nil {
		return 0
	}
	if r, ok := m.L1.(*RistrettoCache); ok {
		return r.Hits()
	}
	return 0
}

// L1Misses 返回 L1 累计未命中次数（L1 未启用时返回 0）。
func (m *Manager) L1Misses() uint64 {
	if m.L1 == nil {
		return 0
	}
	if r, ok := m.L1.(*RistrettoCache); ok {
		return r.Misses()
	}
	return 0
}

// L1Size 返回 L1 当前近似大小（字节）。
func (m *Manager) L1Size() int64 {
	if m.L1 == nil {
		return 0
	}
	return m.L1.Size()
}

// L1Evictions 返回 L1 累计驱逐数量。
func (m *Manager) L1Evictions() uint64 {
	if m.L1 == nil {
		return 0
	}
	return m.L1.Evictions()
}

// L1HitRatio 返回 L1 命中率（L1 未启用时返回 0）。
func (m *Manager) L1HitRatio() float64 {
	if m.L1 == nil {
		return 0
	}
	return m.L1.HitRatio()
}

// HotKeyCount 返回当前检测到的热点 Key 数量。
func (m *Manager) HotKeyCount() int {
	if m.HotKey == nil {
		return 0
	}
	return len(m.HotKey.GetHotKeys())
}

// Health 健康检查
func (m *Manager) Health(ctx context.Context) map[string]interface{} {
	result := map[string]interface{}{
		"l1_ok": m.L1 != nil,
	}
	if m.L2 != nil {
		start := time.Now()
		ok, err := m.L2.Exists(ctx, "_health_check")
		result["l2_ok"] = err == nil
		result["l2_latency_ms"] = time.Since(start).Milliseconds()
		_ = ok
	} else {
		result["l2_ok"] = false
	}
	return result
}
