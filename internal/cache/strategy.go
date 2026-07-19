package cache

import (
	"context"
	"math/rand"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/degrade"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

const (
	// lockSpinInterval 击穿防护自旋等待间隔（每次未获取到回源锁后的轮询周期）
	lockSpinInterval = 50 * time.Millisecond
	// lockSpinTimes 击穿防护自旋最大次数（上限 500ms 后放弃自旋、转直接回源）
	lockSpinTimes = 10
	// asyncWriteTimeout 缓存「尽力而为」异步写（Bloom/L2 广播/降级回填/空值标记）的单次上限。
	// 这些 goroutine 原先用 context.Background()，Redis 慢/关闭时 goroutine 永久阻塞泄漏；
	// 加 3s 上限后即便下游卡住也会自动退出，避免优雅关闭期或 Redis 抖动时的资源泄漏。
	asyncWriteTimeout = 3 * time.Second
)

// bgAsyncCtx 返回一次「尽力而为」异步写使用的 context：带固定短超时（非 context.Background()），
// 保证下游卡住时 goroutine 必然退出（需求 §6 优雅关闭 / 资源泄漏收敛）。
func (s *CacheStrategy) bgAsyncCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), asyncWriteTimeout)
}

// CacheStrategy 三级缓存策略引擎
// 职责：L1→L2→DB 回源、延迟双删、空值缓存、广播失效、读写降级（需求 §11）
type CacheStrategy struct {
	L1            L1Cache
	L2            L2Cache
	Bloom         BloomFilter
	HotKey        *HotKeyDetector
	cm            *config.Manager    // 配置管理器（读路径实时读取降级开关，支持热更新）
	degradeSender DegradeEventSender // 写降级事件发送器（nil 时退化为本地异步写 L2）
	log           *zap.Logger
	sf            singleflight.Group // 本进程内回源合并（缓存击穿防护第一道防线）
	invalCh       string             // 广播失效频道名（初始化时取自配置，运行期不变）
}

// NewCacheStrategy 创建缓存策略引擎
func NewCacheStrategy(
	l1 L1Cache,
	l2 L2Cache,
	bloom BloomFilter,
	hotKey *HotKeyDetector,
	cm *config.Manager,
	log *zap.Logger,
) *CacheStrategy {
	cc := cm.Get().Cache
	ch := cc.InvalidateChannel
	if ch == "" {
		ch = "cache:invalidate"
	}
	return &CacheStrategy{
		L1:      l1,
		L2:      l2,
		Bloom:   bloom,
		HotKey:  hotKey,
		cm:      cm,
		log:     log,
		invalCh: ch,
	}
}

// DegradeEventSender 写降级时，将缓存失效/刷新事件异步投递到消息队列，
// 由消费者侧异步刷新缓存（同步转异步，需求 §11）。nil 时降级为本地异步 goroutine 写 L2。
type DegradeEventSender interface {
	PublishCacheInvalidate(ctx context.Context, key string) error
}

// SetDegradeSender 设置写降级事件发送器（在 MQ 生产者初始化后注入，main.go 调用）。
func (s *CacheStrategy) SetDegradeSender(sender DegradeEventSender) {
	s.degradeSender = sender
}

// cacheCfg 返回当前生效的缓存配置（实时读取，热更新可见）。
func (s *CacheStrategy) cacheCfg() config.CacheConfig {
	return s.cm.Get().Cache
}

// readDegraded 读是否需要降级：配置 read_skip_l2 开启，或熔断器触发全局降级。
func (s *CacheStrategy) readDegraded() bool {
	cc := s.cacheCfg()
	return (cc.Degrade.Enabled && cc.Degrade.ReadSkipL2) || degrade.IsActive()
}

// writeDegraded 写是否需要降级：配置 write_async 开启，或熔断器触发全局降级。
func (s *CacheStrategy) writeDegraded() bool {
	cc := s.cacheCfg()
	return (cc.Degrade.Enabled && cc.Degrade.WriteAsync) || degrade.IsActive()
}

// jitterTTL 计算带随机偏移的 TTL（防雪崩）
func (s *CacheStrategy) jitterTTL(baseTTL time.Duration) time.Duration {
	if s.cacheCfg().TTLJitter <= 0 {
		return baseTTL
	}
	jitter := time.Duration(float64(baseTTL) * s.cacheCfg().TTLJitter * (rand.Float64()*2 - 1))
	result := baseTTL + jitter
	if result <= 0 {
		return baseTTL
	}
	return result
}

// LoadFn 回源加载函数签名
type LoadFn func(ctx context.Context) (interface{}, error)

// Get 三级缓存读取：L1 → L2 → LoadFn (DB回源)
// 自动穿透/击穿/雪崩防护
func (s *CacheStrategy) Get(ctx context.Context, key string, ttl time.Duration, loader LoadFn) (interface{}, error) {
	// 1. L1 本地缓存（进程内，无外部依赖，降级时仍可用）
	if s.L1 != nil {
		val, ok, err := s.L1.Get(ctx, key)
		if err == nil && ok {
			// 热点检测记录
			if s.HotKey != nil {
				s.HotKey.Record(key)
			}
			if val == nil { // 空值缓存命中
				return nil, nil
			}
			return val, nil
		}
	}

	// 读降级：跳过 L2（Redis）与布隆过滤器，直接进入回源（直读 DB），缓解 Redis 压力（需求 §11）
	if s.readDegraded() {
		s.log.Debug("read degraded: skip L2, load from DB", zap.String("key", key))
		// 仍用 singleflight 在本进程合并并发回源，避免降级时大量同 key 请求全部穿透到 DB。
		// 回填仅写 L1，不再写 L2，避免继续打扰 Redis（与「跳过 L2」语义一致）。
		v, err, _ := s.sf.Do(key, func() (interface{}, error) {
			// leader 回源 ctx 与单个请求取消解耦：见下方正常路径说明。
			return s.loadAndFill(context.WithoutCancel(ctx), key, ttl, loader, false)
		})
		return v, err
	}

	// 2. 布隆过滤器（穿透防护，仅 L2 miss 时使用）
	// 注：L1 miss 后先查 L2，若 L2 也不存在再走布隆检查
	if s.L2 != nil {
		val, ok, err := s.L2.Get(ctx, key)
		if err == nil && ok {
			// L2 命中 → 回填 L1
			if s.L1 != nil {
				fillTTL := s.jitterTTL(ttl)
				if s.HotKey != nil && s.HotKey.IsHot(key) {
					fillTTL = s.HotKey.HotTTL(ttl)
				}
				go s.safeSetL1(key, val, fillTTL)
			}
			if val == nil {
				return nil, nil
			}
			return val, nil
		}

		// L2 miss → 布隆过滤器检查
		// 注意：本实现中布隆过滤器仅在“成功回源后”才 Add，并未对全量 DB 记录预热，
		// 因此“!exists”只能说明“该 key 尚未被加载进缓存”，绝不能等同于“DB 中不存在”。
		// 若据 !exists 直接返回空（并写空值缓存），会把 DB 中真实存在、只是尚未缓存的
		// key 误判为不存在，导致所有缓存单实体查询（如 FindItemByID）永远返回空。
		// 故这里仅把布隆当作“可能存在”的正向提示，绝不在 !exists 时短路，必须回源确认。
		if s.Bloom != nil {
			if exists, bloomErr := s.Bloom.Contains(ctx, key); bloomErr == nil && exists {
				s.log.Debug("bloom may contain key, proceed to load", zap.String("key", key))
			}
		}
	}

	// 3. 击穿防护：本进程 singleflight 合并 + 跨进程分布式锁
	//    singleflight 先把同一 key 的并发回源在本进程内合并为一次：既消除「自旋超时后
	//    重复回源」的放大效应、减轻分布式锁争用，也作为 L2 不可用（Redis 故障/仅 L1 部署）
	//    时唯一的本进程防护——原实现在此场景下完全无防护，并发缺失 key 会直接打到 DB，
	//    是真实的缓存击穿漏洞。跨进程防击穿仍由 sourceOnce 内的分布式锁负责。
	result, err, _ := s.sf.Do(key, func() (interface{}, error) {
		// leader 回源 ctx 与「单个请求」的取消解耦：singleflight 的 leader 使用首个到达
		// 请求的 ctx 执行回源，若该请求被取消（而其它同 key 等待者仍存活），回源会以
		// ctx.Err() 失败，导致所有等待者共享一次回源失败——这是 singleflight 的标准陷阱。
		// context.WithoutCancel 保留原 ctx 的 deadline 与 value，但忽略其取消信号：回源仍
		// 受 deadline 约束不会无限挂起，却不会因某一调用方提前退出而中途失败，等待者能拿到
		// 正常结果。进程级退出（背景 ctx 被 cancel）仍会按 deadline 兜底结束。
		return s.sourceOnce(context.WithoutCancel(ctx), key, ttl, loader)
	})
	return result, err
}

// sourceOnce 在 singleflight 内执行的回源逻辑（本进程并发已合并为一次）。
// 负责跨进程防击穿（分布式锁）+ 最终回源。L2 不可用时无分布式锁，仅靠 singleflight
// 合并本进程并发回源；L2 可用时争用分布式锁，拿不到锁则自旋等待其它进程回填。
func (s *CacheStrategy) sourceOnce(ctx context.Context, key string, ttl time.Duration, loader LoadFn) (interface{}, error) {
	// 进入回源前再查一次 L1/L2：可能已被本进程其它请求或回源完成回填。
	if s.L1 != nil {
		if val, ok, _ := s.L1.Get(ctx, key); ok {
			if val == nil {
				return nil, nil
			}
			return val, nil
		}
	}
	if s.L2 != nil {
		if val, ok, err := s.L2.Get(ctx, key); ok {
			if val == nil {
				return nil, nil
			}
			return val, nil
		} else if err != nil {
			// L2 后端错误（连接断开/超时等），记录告警后跳过 L2 继续回源 ——
			// 这种情况不应被当作「key 不存在」触发不必要的 DB 回源。
			s.log.Warn("L2 check before source failed, falling through to DB",
				zap.String("key", key), zap.Error(err))
		}
	}

	// L2 不可用：无分布式锁，仅靠 singleflight 合并本进程并发回源（填补原实现的击穿漏洞）。
	if s.L2 == nil {
		return s.loadAndFill(ctx, key, ttl, loader, false)
	}

	// 分布式锁（跨进程防击穿）：仅当 L2 可用时启用。
	lockKey := "lock:source:" + key
	lockTTL := 10 * time.Second
	locked, lockErr := s.L2.Lock(ctx, lockKey, lockTTL)
	if lockErr != nil {
		// 分布式锁后端不可用（Redis 连接断开等）：所有副本都无法获取锁，
		// 自旋只会白白等待后再各自回源。此处直接跳过锁机制，仅靠 singleflight
		// 合并本进程并发回源，避免不必要的延迟。
		s.log.Warn("Distributed lock backend unavailable, falling back to singleflight-only source",
			zap.String("key", key), zap.Error(lockErr))
		return s.loadAndFill(ctx, key, ttl, loader, false)
	}
	if !locked {
		// 未获取锁 → 其它进程正在回源，自旋等待其回填（尊重请求取消）。
		return s.spinWaitSource(ctx, key, loader, ttl)
	}
	defer s.L2.Unlock(ctx, lockKey)
	// L2 写必须同步：保证分布式锁释放前 L2 已写好，否则其它进程抢到锁后 L2 仍 miss
	// 会二次回源，破坏单飞语义。
	return s.loadAndFill(ctx, key, ttl, loader, true)
}

// spinWaitSource 抢锁失败后自旋等待其它进程回源结果；自旋耗尽则本进程兜底回源。
// 自旋复用单个 time.Timer 并 Reset，避免 time.After 在循环内反复创建 timer（高并发
// 抢锁失败场景下会堆积大量短命 timer，加重 timer 堆与 GC 负担）。
func (s *CacheStrategy) spinWaitSource(ctx context.Context, key string, loader LoadFn, ttl time.Duration) (interface{}, error) {
	timer := time.NewTimer(lockSpinInterval)
	defer timer.Stop()
	for i := 0; i < lockSpinTimes; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		// 等待期间重新查 L1/L2
		if s.L1 != nil {
			if val, ok, _ := s.L1.Get(ctx, key); ok {
				if val == nil {
					return nil, nil
				}
				return val, nil
			}
		}
		if s.L2 != nil {
			if val, ok, err := s.L2.Get(ctx, key); ok {
				if val == nil {
					return nil, nil
				}
				return val, nil
			} else if err != nil {
				// L2 不可用时继续自旋，不中断等待（仍有其它副本可能正在回填 L1）
				s.log.Warn("L2 check during spin-wait failed",
					zap.String("key", key), zap.Error(err))
			}
		}
		// 复位 timer 进入下一次自旋（drain 已触发的 channel，避免 Reset 竞态）
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(lockSpinInterval)
	}
	// 自旋超时：本进程兜底回源（与锁持有者并发，属降级兜底）
	s.log.Warn("cache stampede spin timeout, fallback to local source", zap.String("key", key))
	return s.loadAndFill(ctx, key, ttl, loader, true)
}

// loadAndFill 回源 DB 并写回缓存（抽取自 Get，供正常路径与读降级路径共用）。
// fillL2=false 时（读降级 / 全局降级）回填只写 L1，不碰 L2，真正减轻 Redis 压力。
func (s *CacheStrategy) loadAndFill(ctx context.Context, key string, ttl time.Duration, loader LoadFn, fillL2 bool) (interface{}, error) {
	// 回源 DB
	result, err := loader(ctx)
	if err != nil {
		return nil, err
	}

	// 写回缓存
	if result != nil {
		s.fillCache(ctx, key, result, ttl, fillL2)
		// 更新布隆过滤器
		if s.Bloom != nil {
			go func() {
				ctx, cancel := s.bgAsyncCtx()
				defer cancel()
				if err := s.Bloom.Add(ctx, key); err != nil {
					s.log.Warn("Bloom add failed", zap.String("key", key), zap.Error(err))
				}
			}()
		}
	} else {
		// 空值缓存
		s.safeSetNull(ctx, key, fillL2)
	}

	return result, nil
}

// Set 写入缓存（延迟双删 + 广播失效；写降级时转异步，需求 §11）
func (s *CacheStrategy) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// 1. 立即删除 L1（双删第一步，主路径与降级路径均执行）
	if s.L1 != nil {
		s.L1.Delete(ctx, key)
	}

	// 写降级：跳过同步 L2 写，转异步（投递 Kafka 事件由消费者刷新；无 Kafka 则本地异步写 L2）
	if s.writeDegraded() {
		s.log.Debug("write degraded: skip sync L2 write", zap.String("key", key))
		s.asyncWriteDegraded(ctx, key, value, ttl)
		return nil
	}

	// 2. 写入 L2
	if s.L2 != nil {
		jitteredTTL := s.jitterTTL(ttl)
		if s.HotKey != nil && s.HotKey.IsHot(key) {
			jitteredTTL = s.HotKey.HotTTL(ttl)
		}
		if err := s.L2.Set(ctx, key, value, jitteredTTL); err != nil {
			return err
		}
	}

	// 3. 广播失效通知
	if s.L2 != nil {
		go func() {
			ctx, cancel := s.bgAsyncCtx()
			defer cancel()
			if err := s.L2.Publish(ctx, s.invalCh, key); err != nil {
				s.log.Warn("Cache invalidation publish failed",
					zap.String("key", key),
					zap.Error(err),
				)
			}
		}()
	}

	// 4. 延迟二次删除 L1（双删第二步）
	// 延迟 = max(配置延迟双删值, 配置主从复制延迟 × 2)
	// 确保从库已经有最新数据后再删 L1，避免读到旧数据
	delay := s.cacheCfg().DoubleDeleteDelay
	if delay <= 0 {
		delay = 500 * time.Millisecond
	}
	// 动态叠加主从复制延迟（如果有配置）
	if s.cacheCfg().ReplicationLag > 0 {
		dynamicDelay := s.cacheCfg().ReplicationLag * 2
		if dynamicDelay > delay {
			delay = dynamicDelay
		}
	}
	go func() {
		time.Sleep(delay)
		if s.L1 != nil {
			if err := s.L1.Delete(context.Background(), key); err != nil {
				s.log.Warn("Double delete L1 failed",
					zap.String("key", key),
					zap.Error(err),
				)
			}
		}
	}()

	return nil
}

// asyncWriteDegraded 写降级路径：将同步 L2 写转异步。
// 优先投递 Kafka 失效/刷新事件（由消费者侧异步刷新缓存）；无发送器时退化为本地异步写 L2。
func (s *CacheStrategy) asyncWriteDegraded(ctx context.Context, key string, value interface{}, ttl time.Duration) {
	if s.degradeSender != nil {
		if err := s.degradeSender.PublishCacheInvalidate(ctx, key); err != nil {
			s.log.Warn("degrade publish cache invalidate failed",
				zap.String("key", key), zap.Error(err))
		}
		return
	}
	// 无 Kafka：本地异步写 L2（与请求路径解耦，仍保证最终一致）
	if s.L2 != nil {
		jitteredTTL := s.jitterTTL(ttl)
		if s.HotKey != nil && s.HotKey.IsHot(key) {
			jitteredTTL = s.HotKey.HotTTL(ttl)
		}
		go func() {
			ctx, cancel := s.bgAsyncCtx()
			defer cancel()
			if err := s.L2.Set(ctx, key, value, jitteredTTL); err != nil {
				s.log.Warn("degrade async L2 write failed",
					zap.String("key", key), zap.Error(err))
			}
		}()
	}
}

// Delete 删除缓存
func (s *CacheStrategy) Delete(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		if s.L1 != nil {
			s.L1.Delete(ctx, key)
		}
		if s.L2 != nil {
			s.L2.Delete(ctx, key)
		}
	}
	return nil
}

// Invalidate 接收广播失效通知，清理本地 L1
func (s *CacheStrategy) Invalidate(ctx context.Context, key string) {
	if s.L1 != nil {
		if err := s.L1.Delete(ctx, key); err != nil {
			s.log.Warn("Invalidate L1 failed",
				zap.String("key", key),
				zap.Error(err),
			)
		}
	}
}

// StartInvalidationListener 启动广播失效监听（goroutine）
func (s *CacheStrategy) StartInvalidationListener(ctx context.Context) {
	if s.L2 == nil {
		return
	}

	go func() {
		s.log.Info("Cache invalidation listener started",
			zap.String("channel", s.invalCh),
		)
		err := s.L2.Subscribe(ctx, s.invalCh, func(key string) {
			// 防御性 recover：单条消息 Invalidate 异常（L1 底层 panic 等）不应拖垮整个失效监听
			// goroutine，否则本实例 L1 缓存将永久无法被广播失效刷新、产生脏数据（与「消费 handler
			// panic 拖垮进程」同源风险）。捕获后仅告警，监听继续。
			defer func() {
				if r := recover(); r != nil {
					s.log.Error("Cache invalidation handler panicked",
						zap.Any("panic", r), zap.String("key", key))
				}
			}()
			// 复用外层订阅 ctx（由 cache manager 的可取消 invalCtx 派生）：优雅关闭时
			// 随订阅退出，避免 per-message 处理用 context.Background() 永不取消。
			s.Invalidate(ctx, key)
			s.log.Debug("Received invalidation", zap.String("key", key))
		})
		if err != nil {
			s.log.Warn("Invalidation listener stopped", zap.Error(err))
		}
	}()
}

// ────────── 内部辅助 ──────────

// fillCache 回源后写回缓存。
// 注意：本函数刻意同步执行 L2 写——回源路径持有分布式锁，必须在锁释放前把 L2 写好，
// 否则并发请求抢到锁后 L2 仍 miss 会二次回源，破坏单飞（击穿防护）语义。
// writeL2=false 时（读降级 / 全局降级）只写 L1，不写 L2，避免继续打扰 Redis。
func (s *CacheStrategy) fillCache(ctx context.Context, key string, value interface{}, ttl time.Duration, writeL2 bool) {
	jitteredTTL := s.jitterTTL(ttl)
	if s.HotKey != nil && s.HotKey.IsHot(key) {
		jitteredTTL = s.HotKey.HotTTL(ttl)
	}

	if writeL2 && s.L2 != nil {
		if err := s.L2.Set(ctx, key, value, jitteredTTL); err != nil {
			s.log.Warn("L2 fill failed", zap.String("key", key), zap.Error(err))
		}
	}

	if s.L1 != nil {
		s.safeSetL1(key, value, jitteredTTL)
	}
}

func (s *CacheStrategy) safeSetL1(key string, value interface{}, ttl time.Duration) {
	if err := s.L1.Set(context.Background(), key, value, ttl); err != nil {
		s.log.Warn("L1 fill failed", zap.String("key", key), zap.Error(err))
	}
}

// safeSetNull 设置空值缓存。
// writeL2=false 时（读降级 / 全局降级）只写 L1 空值，不写 L2。
func (s *CacheStrategy) safeSetNull(ctx context.Context, key string, writeL2 bool) {
	nullTTL := s.cacheCfg().NullCacheTTL
	if nullTTL <= 0 {
		nullTTL = 60 * time.Second
	}
	if s.L1 != nil {
		go s.safeSetL1(key, nil, nullTTL)
	}
	if writeL2 && s.L2 != nil {
		if rc, ok := s.L2.(*RedisCache); ok {
		go func() {
			ctx, cancel := s.bgAsyncCtx()
			defer cancel()
			if err := rc.SetNull(ctx, key, nullTTL); err != nil {
				s.log.Warn("Null cache L2 failed", zap.String("key", key), zap.Error(err))
			}
		}()
		}
	}
}
