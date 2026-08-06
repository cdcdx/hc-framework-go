package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/config"
	"github.com/cdcdx/hc-framework-go/common/cache"
	"github.com/cdcdx/hc-framework-go/common/dbclient"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/cdcdx/hc-framework-go/common/mq"
	"github.com/zeromicro/go-zero/core/logx"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

// ServiceContext 单一 rpc 服务上下文：
// 合并 4 个域的 DB 表与 JWT 管理器；域间协作（积分/任务进度）为进程内调用，无需 rpc client。
type ServiceContext struct {
	Config config.Config

	// Db 主业务库（向后兼容）。当配置了多库 Databases 时，Db = DBFactory.SQL("business")；
	// 否则由旧版单库字段 DB 创建。
	Db *gorm.DB

	// DBFactory 多库客户端工厂。通过 DBFactory.SQL(name) 获取 *gorm.DB；
	// 通过 DBFactory.MongoDB(name) / .Elasticsearch(name) 获取 NoSQL 客户端（待实现）。
	DBFactory *dbclient.Factory

	JwtMgr *jwt.Manager

	// Cache 两级缓存管理器。Enabled=false 时所有操作为 no-op。
	Cache *cache.Manager

	// MQProducer 消息生产者。
	MQProducer mq.Producer

	// MQConsumer 消息消费者。
	MQConsumer mq.Consumer

	// stop 用于优雅关闭后台 goroutine（如 idle scan）。
	stop chan struct{}
	// consumerCancel 取消 MQ 消费者上下文（task_progress 后台消费）。
	consumerCancel context.CancelFunc
	// sf 防止缓存击穿：同一 key 并发 miss 时只有一个请求回源 DB。
	sf singleflight.Group
	// progressAgg 任务进度事件聚合器；消费端投递到此处，后台批量 flush 到 DB。
	progressAgg *progressAggregator
	// wg 跟踪后台 goroutine，Close 时等待其退出。
	wg sync.WaitGroup
}

// NewServiceContext 装配 DB（含全部表结构与任务种子）、JWT、缓存、消息队列。
func NewServiceContext(c config.Config) *ServiceContext {
	// 日志
	if len(c.Log.ServiceName) > 0 {
		if err := logx.SetUp(c.Log); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] logx.SetUp failed, using default logger: %v\n", err)
		}
	}

	// ---- 数据库初始化 ----
	var db *gorm.DB
	var factory *dbclient.Factory

	if len(c.Databases) > 0 {
		// 多库模式：通过 Factory 统一管理所有 SQL/NoSQL 客户端
		// ToSpecs 完成 DSN 解析与连接池优先级适配，dbclient 不感知配置结构
		factory = dbclient.NewFactory(c.Databases.ToSpecs())

		// 主业务库（必配）
		bizDB, err := factory.SQL("business")
		if err != nil {
			logx.Must(fmt.Errorf("open business db: %w", err))
		}
		db = bizDB

		// user / monitor / log 按需延迟加载（factory.SQL() 自身支持线程安全的单次初始化）。
		// 各域 logic 首次访问时自动创建连接，无需在此预初始化。
	} else {
		// 单库模式（向后兼容）
		poolLabel := c.DB.PoolLabel
		if poolLabel == "" {
			poolLabel = "default"
		}
		var err error
		db, err = gormx.OpenWithPool(c.DB.Driver, c.DB.Dsn, gormx.PoolConfig{
			MaxOpenConns:    c.DB.MaxOpenConns,
			MaxIdleConns:    c.DB.MaxIdleConns,
			ConnMaxLifetime: c.DB.ConnMaxLifetime,
			Label:           poolLabel,
		})
		if err != nil {
			logx.Must(err)
		}
	}

	if err := db.AutoMigrate(
		&model.User{},
		&model.IdleRecord{}, &model.IdleDailyPoints{},
		&model.Task{}, &model.UserTaskProgress{},
		&model.ShopItem{}, &model.RedeemOrder{}, &model.ShopFlashActivity{},
	); err != nil {
		logx.Must(err)
	}
	seedTasks(db)

	// ---- JWT ----
	mgr, err := jwt.NewManager(
		c.Jwt.Algorithm, c.Jwt.SigningKey,
		c.Jwt.PrivateKeyPath, c.Jwt.PublicKeyPath,
		c.Jwt.Issuer, c.Jwt.AccessTTL, c.Jwt.RefreshTTL,
	)
	if err != nil {
		logx.Must(err)
	}

	// ---- 缓存 ----
	// MaxMemoryMB 到 MaxCost 的自动换算：仅当 MaxCost 未显式配置时使用。
	l1Cfg := c.Cache.L1.ToCacheL1()
	if l1Cfg.MaxCost == 0 && l1Cfg.MaxMemoryMB > 0 {
		l1Cfg.MaxCost = int64(l1Cfg.MaxMemoryMB) << 20
	}
	cacheMgr := cache.New(cache.Config{
		Enabled: c.Cache.Enabled,
		L1:      l1Cfg,
		L2:      c.Cache.L2.ToCacheL2(),
		Bloom:   c.Cache.Bloom.ToCacheBloom(),
	})

	// ---- 消息队列 ----
	mqProducer := mq.NewProducer(c.MQ.ToMQConfig())
	mqConsumer := mq.NewConsumer(c.MQ.ToMQConfig())

	svcCtx := &ServiceContext{
		Config:     c,
		Db:         db,
		DBFactory:  factory,
		JwtMgr:     mgr,
		Cache:      cacheMgr,
		MQProducer: mqProducer,
		MQConsumer: mqConsumer,
		stop:       make(chan struct{}),
	}

	svcCtx.wg.Add(1)
	go startIdleScan(svcCtx, svcCtx.stop, &svcCtx.wg)

	// MQ 消费端注册。任务进度上报异步化：兑换主链路只负责投递事件，
	// 由后台消费者（聚合器）批量处理「查 Task + 累加/判定进度」，将高频随机写
	// 合并为低频批量更新，显著降低 DB 压力。
	svcCtx.progressAgg = newProgressAggregator(svcCtx)
	svcCtx.progressAgg.start()
	if svcCtx.MQConsumer != nil {
		ctx, cancel := context.WithCancel(context.Background())
		svcCtx.consumerCancel = cancel
		// 注册死信处理器：重试耗尽的 task_progress 事件最终落到此处，
		// 由聚合器在 best-effort 模式下做最后一次同步落库，避免进度永久丢失。
		if dlq, ok := svcCtx.MQConsumer.(interface{ SetDLQ(mq.Handler) }); ok {
			dlq.SetDLQ(svcCtx.handleTaskProgressDLQ)
		}
		if err := svcCtx.MQConsumer.Subscribe(ctx, "task_progress", svcCtx.handleTaskProgress); err != nil {
			logx.Errorf("subscribe task_progress failed: %v", err)
		}
	}

	return svcCtx
}

// InvalidateFlashActivitiesCache 主动失效「进行中活动列表」缓存。
// 秒杀成功会改变活动 sold_qty / 状态，调用此函数保证列表尽快反映最新。
// 活动列表缓存 key 形如 flash:activities:{limit}（limit 范围 1..100）。
func (svc *ServiceContext) InvalidateFlashActivitiesCache(ctx context.Context) {
	if svc.Cache == nil {
		return
	}
	for limit := 1; limit <= 100; limit++ {
		_ = svc.Cache.Delete(ctx, fmt.Sprintf("flash:activities:%d", limit))
	}
}

// InvalidateShopItemsCache 主动失效「商品列表」缓存（默认分类首页热点路径）。
// 商品/活动发生写操作（创建/更新/上下架）时调用，保证列表尽快反映最新。
// key 形如 shop:items:default:{limit}（limit 范围 1..100）。
func (svc *ServiceContext) InvalidateShopItemsCache(ctx context.Context) {
	if svc.Cache == nil {
		return
	}
	for limit := 1; limit <= 100; limit++ {
		_ = svc.Cache.Delete(ctx, fmt.Sprintf("shop:items:default:%d", limit))
	}
}

// CachedGet 从两级缓存获取值；miss 时调用 loader 回源并自动回填缓存。
// 回填 TTL 使用 L1 的 DefaultTTL（传入 0 的语义）。
//
// 示例用法:
//
//	val, err := svcCtx.CachedGet(ctx, "user:profile:"+uid, func(ctx context.Context) ([]byte, error) {
//	    user, err := svcCtx.GetUserProfile(ctx, uid)
//	    return json.Marshal(user), err
//	})
// CachedGet 读穿缓存：先查缓存，未命中则用 loader 加载并回填。
// ttl 为回填 TTL；传 <=0 时回退到 L1.DefaultTTL，避免给 L2(Redis) 写入永久 key。
// Get 出错时按 miss 处理（不阻断主流程），并记录指标日志。
func (svc *ServiceContext) CachedGet(ctx context.Context, key string, loader func(context.Context) ([]byte, error), ttl time.Duration) ([]byte, error) {
	if svc.Cache == nil {
		return loader(ctx)
	}
	val, err := svc.Cache.Get(ctx, key)
	if err != nil {
		logx.WithContext(ctx).Infof("[cache] get %s error: %v (treat as miss)", key, err)
	}
	if val != nil {
		return val, nil
	}
	// 缓存击穿保护：同一 key 并发 miss 时，只有一个请求执行 loader 回源，
	// 其余请求共享结果，避免热点 key（商品列表/任务列表/余额）失效瞬间打爆 DB。
	v, err, _ := svc.sf.Do(key, func() (interface{}, error) {
		// 双重检查：singleflight 唤醒后可能已被其它协程回填，避免重复写。
		if val, err := svc.Cache.Get(ctx, key); err == nil && val != nil {
			return val, nil
		}
		v, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		ttl := ttl
		if ttl <= 0 {
			ttl = svc.Cache.DefaultTTL()
		}
		if err := svc.Cache.Set(ctx, key, v, ttl); err != nil {
			logx.WithContext(ctx).Infof("[cache] set %s error: %v (skip)", key, err)
		}
		return v, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// Publish 向指定 topic 发布消息。MQ 禁用时为 no-op。
func (svc *ServiceContext) Publish(ctx context.Context, topic, key string, value []byte) error {
	if svc.MQProducer == nil {
		return nil
	}
	return svc.MQProducer.Send(ctx, topic, key, value)
}

// ProgressAgg 返回任务进度聚合器（logic 层在 MQ 未启用时退化为同步入缓冲）。
func (svc *ServiceContext) ProgressAgg() *progressAggregator {
	return svc.progressAgg
}

// Close 优雅关闭所有外部资源：停止后台扫描 goroutine、关闭缓存(L2)、消息队列与数据库连接。
// 应在进程退出时调用（如 main 中 defer svcCtx.Close()）。重复调用安全。
func (svc *ServiceContext) Close() error {
	if svc.stop != nil {
		close(svc.stop)
	}
	if svc.consumerCancel != nil {
		svc.consumerCancel()
	}
	// 先停 MQ 消费者，再冲刷进度聚合器（确保剩余缓冲写入 DB 后才关 DB）。
	if svc.progressAgg != nil {
		svc.progressAgg.stopAndDrain()
	}
	svc.wg.Wait()

	if svc.Cache != nil {
		_ = svc.Cache.Close()
	}
	if svc.MQProducer != nil {
		_ = svc.MQProducer.Close()
	}
	if svc.MQConsumer != nil {
		_ = svc.MQConsumer.Close()
	}
	if svc.DBFactory != nil {
		_ = svc.DBFactory.Close()
	}
	return nil
}

// periodOfTask 计算任务当前周期（与 logic.periodOf 语义一致，内联避免 svc→logic 循环依赖）。
//   - daily → 当天 "2006-01-02"
//   - weekly → ISO 周 "2006-W01"
//   - 其它（achievement）→ "all"
func periodOfTask(t *model.Task, now time.Time) string {
	switch t.TaskType {
	case "daily":
		return now.Format("2006-01-02")
	case "weekly":
		y, w := now.ISOWeek()
		return fmt.Sprintf("%d-W%02d", y, w)
	default:
		return "all"
	}
}

// TaskProgressEvent 任务进度异步事件（通过 MQ 投递，由 handleTaskProgress 消费）。
// 导出以供 logic 包构造消息体。
type TaskProgressEvent struct {
	UserID  string `json:"user_id"`
	TaskKey string `json:"task_key"`
	Delta   int64  `json:"delta"`
}

// handleTaskProgress 消费 task_progress 事件。
// 不做直接 DB 写，仅将事件投递到 progressAgg 聚合器（内存累加 + 定时/定量批量 flush），
// 从而将高频随机写合并为低频批量更新。主链路只需 1 次异步发布，RT 与 DB 压力显著降低。
//
// 注：实现直接内联在 svc 包，避免 svc→logic 的反向依赖（logic 已 import svc）。
func (svc *ServiceContext) handleTaskProgress(ctx context.Context, msg *mq.Message) error {
	var ev TaskProgressEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		logx.WithContext(ctx).Errorf("[task_progress] bad message: %v", err)
		return err // 返回 error 仅记日志，不阻塞后续消息
	}
	if ev.UserID == "" || ev.TaskKey == "" || ev.Delta <= 0 {
		return nil
	}
	svc.progressAgg.add(ev.UserID, ev.TaskKey, ev.Delta)
	return nil
}

// handleTaskProgressDLQ 死信处理器：消费端重试耗尽后投递此处。
// 直接以 best-effort 方式把事件喂给聚合器做最后一次落库；
// 返回 nil 避免消息被再次投递形成死循环（聚合器本身具备内存缓冲，秒级后会被 flush）。
func (svc *ServiceContext) handleTaskProgressDLQ(ctx context.Context, msg *mq.Message) error {
	var ev TaskProgressEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		logx.WithContext(ctx).Errorf("[task_progress][DLQ] drop bad message: %v", err)
		return nil
	}
	if ev.UserID == "" || ev.TaskKey == "" || ev.Delta <= 0 {
		return nil
	}
	// 立即触发一次冲刷，尽量在本批次内落库。
	svc.progressAgg.add(ev.UserID, ev.TaskKey, ev.Delta)
	svc.progressAgg.flush()
	return nil
}
