package svc

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/cdcdx/hc-framework-go/app/rpc/config"
	"github.com/cdcdx/hc-framework-go/common/cache"
	"github.com/cdcdx/hc-framework-go/common/dbclient"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/cdcdx/hc-framework-go/common/mq"
	"github.com/zeromicro/go-zero/core/logx"
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

		// 主业务库
		bizDB, err := factory.SQL("business")
		if err != nil {
			logx.Must(fmt.Errorf("open business db: %w", err))
		}
		db = bizDB

		// 预初始化其他 SQL 库（可选，失败不阻塞启动）
		for _, name := range []string{"user", "monitor", "log"} {
			if _, ok := c.Databases[name]; ok {
				if cfg := c.Databases[name]; cfg.IsSQL() {
					if _, err := factory.SQL(name); err != nil {
						logx.Errorf("[db] init %s db failed (non-fatal): %v", name, err)
					}
				}
			}
		}
	} else {
		// 单库模式（向后兼容）
		var err error
		db, err = gormx.OpenWithPool(c.DB.Driver, c.DB.Dsn, gormx.PoolConfig{
			MaxOpenConns:    c.DB.MaxOpenConns,
			MaxIdleConns:    c.DB.MaxIdleConns,
			ConnMaxLifetime: c.DB.ConnMaxLifetime,
			Label:           c.DB.PoolLabel,
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
	cacheMgr := cache.New(cache.Config{
		Enabled: c.Cache.Enabled,
		L1: cache.L1Config{
			Enabled:     c.Cache.L1.Enabled,
			MaxMemoryMB: c.Cache.L1.MaxMemoryMB,
			DefaultTTL:  c.Cache.L1.DefaultTTL,
			NumCounters: c.Cache.L1.NumCounters,
			MaxCost:     c.Cache.L1.MaxCost,
		},
		L2: cache.L2Config{
			Enabled:   c.Cache.L2.Enabled,
			Type:      c.Cache.L2.Type,
			Addresses: c.Cache.L2.Addresses,
			Password:  c.Cache.L2.Password,
			DB:        c.Cache.L2.DB,
			PoolSize:  c.Cache.L2.PoolSize,
		},
		Bloom: cache.BloomConfig{
			ExpectedKeys:      c.Cache.Bloom.ExpectedKeys,
			FalsePositiveRate: c.Cache.Bloom.FalsePositiveRate,
		},
	})

	// ---- 消息队列 ----
	mqProducer := mq.NewProducer(mq.Config{
		Enabled:    c.MQ.Enabled,
		Type:       c.MQ.Type,
		BufferSize: 1024,
	})
	mqConsumer := mq.NewConsumer(mq.Config{
		Enabled:    c.MQ.Enabled,
		Type:       c.MQ.Type,
		BufferSize: 1024,
	})

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

	// 注册 MQ 默认消费端（业务域按需在各自 logic 中注册）
	if c.MQ.Enabled && c.MQ.Type != "none" {
		// 示例：订单事件消费（实际业务在对应 logic 中注册 handler）
		_ = svcCtx.MQConsumer.Subscribe(context.Background(), "hc.orders", func(ctx context.Context, msg *mq.Message) error {
			return nil // 待各域 logic 注册实际 handler
		})
	}

	return svcCtx
}

// CachedGet 从两级缓存获取值；miss 时调用 loader 回源并自动回填缓存。
// 示例用法:
//
//	val, err := svcCtx.CachedGet(ctx, "user:profile:"+uid, 5*time.Minute, func(ctx context.Context) ([]byte, error) {
//	    user, err := svcCtx.GetUserProfile(ctx, uid)
//	    return json.Marshal(user), err
//	})
func (svc *ServiceContext) CachedGet(ctx context.Context, key string, loader func(context.Context) ([]byte, error)) ([]byte, error) {
	if svc.Cache == nil {
		return loader(ctx)
	}
	val, _ := svc.Cache.Get(ctx, key)
	if val != nil {
		return val, nil
	}
	v, err := loader(ctx)
	if err != nil {
		return nil, err
	}
	_ = svc.Cache.Set(ctx, key, v, 0)
	return v, nil
}

// Publish 向指定 topic 发布消息。MQ 禁用时为 no-op。
func (svc *ServiceContext) Publish(ctx context.Context, topic, key string, value []byte) error {
	if svc.MQProducer == nil {
		return nil
	}
	return svc.MQProducer.Send(ctx, topic, key, value)
}

// Close 优雅关闭所有外部资源：停止后台扫描 goroutine、关闭缓存(L2)、消息队列与数据库连接。
// 应在进程退出时调用（如 main 中 defer svcCtx.Close()）。重复调用安全。
func (svc *ServiceContext) Close() error {
	if svc.stop != nil {
		close(svc.stop)
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
