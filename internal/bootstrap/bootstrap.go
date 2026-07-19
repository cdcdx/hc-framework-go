// Package bootstrap 组合根（composition root）：负责把所有内部组件（数据库 / 仓库 /
// 服务 / 中间件 / 路由 / 后台任务）按依赖顺序装配起来，并暴露进程生命周期
// 所需的辅助函数。把这部分从 cmd/server/main.go 抽出，既让 main 成为只做编排的
// 薄入口，也让数据库初始化等逻辑可独立测试。
package bootstrap

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/mq"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/internal/scheduler"
	"github.com/cdcdx/hc-framework-go/internal/service/shop"
	"github.com/cdcdx/hc-framework-go/internal/service/task"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
)

// Databases 聚合已初始化的数据库连接与仓库（组合根产物）。
type Databases struct {
	BusinessRW  *db.RWDB                  // 业务数据库（支持读写分离）
	UserRepo    repository.UserRepository // 用户仓库（GORM 或 MongoDB）
	MonitorRepo repository.MonitorRepo    // 监控仓库（GORM / ClickHouse）
	LogRepo     repository.LogRepo        // 日志仓库（GORM / Elasticsearch）
	LoginRepo   repository.LoginRepo      // 结构化登录记录仓库（跟随 log.driver：SQLite / Elasticsearch，ES 不可用时回退 SQLite）
	pingCancel  context.CancelFunc        // 停止从库探活后台协程（由 Close 调用，避免泄漏）
}

// Close 统一释放 Databases 聚合的全部数据库连接，确保优雅关闭时不再泄漏。
// 背景（13 §3.37）：此前 shutdown 仅关了 user/business 两类，monitor/log/login 三类的底层
// 连接（GORM sql.DB / ClickHouse conn / ES client transport）从未关闭，回退 SQLite 时每次退出/
// SIGHUP 都泄漏连接池。各 repo 的 Close 已补到接口（对齐 UserRepository）；本方法集中调用，
// 单个 repo 关闭失败不影响其余资源释放（错误被聚合返回）。
func (d *Databases) Close() error {
	var errs []error
	// 停止从库探活后台协程，避免 goroutine 泄漏与关闭后仍 ping 已关连接（13 §3.40）。
	if d.pingCancel != nil {
		d.pingCancel()
	}
	if d.BusinessRW != nil {
		if err := d.BusinessRW.Close(); err != nil {
			errs = append(errs, fmt.Errorf("business-db: %w", err))
		}
	}
	if d.UserRepo != nil {
		if err := d.UserRepo.Close(); err != nil {
			errs = append(errs, fmt.Errorf("user-db: %w", err))
		}
	}
	if d.MonitorRepo != nil {
		if err := d.MonitorRepo.Close(); err != nil {
			errs = append(errs, fmt.Errorf("monitor-db: %w", err))
		}
	}
	if d.LogRepo != nil {
		if err := d.LogRepo.Close(); err != nil {
			errs = append(errs, fmt.Errorf("log-db: %w", err))
		}
	}
	if d.LoginRepo != nil {
		if err := d.LoginRepo.Close(); err != nil {
			errs = append(errs, fmt.Errorf("login-db: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("databases close errors: %v", errs)
	}
	return nil
}

// validateInsecureMQ 生产环境误用 insecure MQ 的硬卡点（13 §3.40 方向2）。
// memory / none / noop 类型本就只用于本地 dev/test（at-most-once、无 DLQ/持久化）；
// 当部署呈「生产形态」（多副本 pod_total>1 或启用 Leader 选举）却仍未配置真实 broker 时，
// 直接拒绝启动，避免事件总线静默丢消息。单副本 dev 场景保持既有 WARN，不阻断。
// 紧急逃逸：环境变量 APP_MQ_ALLOW_INSECURE=true 可强制放行（仅限排障）。
func validateInsecureMQ(cfg *config.Config) error {
	t := strings.ToLower(strings.TrimSpace(cfg.MQ.Type))
	insecure := t == "" || t == "memory" || t == "none" || t == "noop"
	if !insecure {
		return nil
	}
	prodLike := cfg.Idle.ScanSharding.PodTotal > 1 || cfg.Idle.LeaderElection.Enabled
	if !prodLike {
		return nil // 单副本 dev：由 mq 包既有 WARN 提示，不阻断
	}
	if strings.EqualFold(os.Getenv("APP_MQ_ALLOW_INSECURE"), "true") {
		logger.L().Warn("insecure MQ type overridden by APP_MQ_ALLOW_INSECURE=true; events may be lost in production")
		return nil
	}
	return fmt.Errorf("refusing to start: MQ type %q is insecure (memory/none/noop) but deployment looks production-like (pod_total=%d, leader_election=%v); configure a real broker (kafka/rabbitmq/rocketmq) or set APP_MQ_ALLOW_INSECURE=true to override",
		cfg.MQ.Type, cfg.Idle.ScanSharding.PodTotal, cfg.Idle.LeaderElection.Enabled)
}

// ──────────────────────────────────────────────────────────────
// 数据库初始化
// ──────────────────────────────────────────────────────────────

// connectWithTimeout 带超时的 adapter 连接，避免数据库 / ES / CH 网络半开（能建连但握手阻塞）
// 时 InitDatabases 无限卡在启动阶段（13 §3.40）。默认 10s，可按需调整。
const defaultConnectTimeout = 10 * time.Second

func connectWithTimeout(name string, connect func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultConnectTimeout)
	defer cancel()
	if err := connect(ctx); err != nil {
		return fmt.Errorf("connect %s (timeout %s): %w", name, defaultConnectTimeout, err)
	}
	return nil
}

// gormAdapter 内部 GORM 适配器接口（SQLite / MySQL / PostgreSQL 适配器均实现
// Connect + GORM）。db 包另有更宽的 Adapter 接口（含 Name/Close/Ping 等），
// 此处只取打开 GORM 所需的最小契约。
type gormAdapter interface {
	Connect(ctx context.Context) error
	GORM() *gorm.DB
}

// rwOpenGORM 打开数据库并返回 *db.RWDB（单主库模式，无读写分离）。
// 用于不需要读写分离的数据库（User / Log / Monitor）。
// pool 为连接池配置（来自配置，值形式透传）；仅当显式配置了 max_open/max_idle 时才覆盖适配器默认值，
// 避免把「0 值」误当「无限制」。sqlite 路径忽略 pool。
func rwOpenGORM(name, driver, dsn string, models []interface{}, cfg *config.Config, pool db.PoolConfig) (*db.RWDB, error) {
	zlog := logger.L()
	var a gormAdapter
	switch driver {
	case "mysql":
		if pool.MaxOpenConns > 0 || pool.MaxIdleConns > 0 {
			a = db.NewMySQLAdapter(dsn, db.WithMySQLPool(pool.MaxIdleConns, pool.MaxOpenConns, pool.ConnMaxLifetime))
		} else {
			a = db.NewMySQLAdapter(dsn)
		}
	case "postgres":
		if pool.MaxOpenConns > 0 || pool.MaxIdleConns > 0 {
			a = db.NewPostgreSQLAdapter(dsn, db.WithPostgreSQLPool(pool.MaxIdleConns, pool.MaxOpenConns, pool.ConnMaxLifetime))
		} else {
			a = db.NewPostgreSQLAdapter(dsn)
		}
	default:
		a = db.NewSQLiteAdapter(dsn)
	}

	if err := a.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("connect %s: %w", name, err)
	}
	gormDB := a.GORM()
	if gormDB == nil {
		return nil, fmt.Errorf("failed to get GORM instance for %s", name)
	}
	zlog.Info("DB connected", safeConnFields(name, driver, dsn)...)

	if len(models) > 0 && cfg.Migration.AutoMigrate {
		if err := gormDB.AutoMigrate(models...); err != nil {
			return nil, fmt.Errorf("migrate %s: %w", name, err)
		}
	} else if len(models) > 0 && !cfg.Migration.AutoMigrate {
		zlog.Info("AutoMigrate skipped for DB (use 'make migrate' instead)",
			zap.String("name", name),
			zap.String("driver", driver),
		)
	}
	return db.CreateSingleRWDB(gormDB), nil
}

// openGORMOrFallback 打开 GORM 数据库；若指定驱动连接失败且非 sqlite，则回退到 SQLite。
// 统一 user/monitor/log/business 等「连接失败 → SQLite」逻辑，避免各分支重复 fallback。
func openGORMOrFallback(name, driver, dsn, fallbackDSN string, models []interface{}, cfg *config.Config, pool db.PoolConfig) (*gorm.DB, error) {
	rw, err := rwOpenGORM(name, driver, dsn, models, cfg, pool)
	if err == nil {
		return rw.Master(), nil
	}
	if driver == "sqlite" {
		return nil, err
	}
	logger.L().Warn(fmt.Sprintf("%s DB (%s) unavailable, falling back to SQLite", name, driver), zap.Error(err))
	rw, err = rwOpenGORM(name, "sqlite", fallbackDSN, models, cfg, db.PoolConfig{})
	if err != nil {
		return nil, err
	}
	return rw.Master(), nil
}

// sqliteFallback 在异构后端（mongodb / clickhouse / elasticsearch）不可用时，
// 统一以对应库的 SQLite DSN 打开 GORM，并交给 build 构造目标仓库。
// 把 user/monitor/log 三处「rwOpenGORM + 判错 + NewXRepository」近重复代码收敛为一次调用；
// 告警日志仍由各调用点按上下文（含后端专属字段）发出，本函数只负责「打开 + 构造」。
func sqliteFallback[T any](name, dsn string, models []interface{}, cfg *config.Config, build func(*gorm.DB) T) (T, error) {
	rw, err := rwOpenGORM(name, "sqlite", dsn, models, cfg, db.PoolConfig{})
	if err != nil {
		var zero T
		return zero, err
	}
	return build(rw.Master()), nil
}

// checkBusinessTables 在关闭启动自动迁移时，校验 businessModels 对应的核心表是否已存在。
// 若迁移被标记为已应用但表实际缺失（执行失败/连错库/中断），此处给出明确告警，
// 让问题在启动阶段暴露，而不是运行期随机报 42S02。不阻断启动，交由运维补齐迁移。
func checkBusinessTables(conn *gorm.DB, models []interface{}) {
	for _, m := range models {
		stmt := &gorm.Statement{DB: conn}
		stmt.Parse(m)
		name := ""
		if stmt.Schema != nil {
			name = stmt.Schema.Table
		}
		if !conn.Migrator().HasTable(m) {
			logger.L().Warn("business core table missing — run 'make migrate DB=business' to create it",
				zap.String("table", name))
		}
	}
}

// InitDatabases 按配置初始化四个数据库（business / user / monitor / log）以及
// 结构化登录记录库，各库在主驱动不可用时回退 SQLite。
func InitDatabases(cfg *config.Config) (*Databases, error) {
	zlog := logger.L()

	// Business DB: 业务数据（支持 mysql / postgres / sqlite，主驱动不可用时回退 sqlite）
	// 支持读写分离：当 read_write_split.enabled=true 且有从库时使用 RWDB
	var businessRW *db.RWDB
	var pingCancel context.CancelFunc // 从库探活协程取消函数，交给 Databases.Close 调用
	var businessOpts []db.MySQLOption
	businessDSN := cfg.Database.Business.DSN
	businessDriver := cfg.Database.Business.Driver
	businessModels := []interface{}{
		&model.EventDedup{},
		&model.IdleRecord{},
		&model.IdleDailyPoints{},
		&model.ShopItem{},
		&model.ShopFlashActivity{},
		&model.RedeemOrder{},
		&model.PointsTransaction{},
		&model.PointsOutbox{},
		&model.Task{},
		&model.UserTaskProgress{},
	}

	if businessDriver == "mysql" {
		mc := cfg.Database.Business.MySQL
		if mc.Master != "" {
			businessDSN = mc.Master
		}
		businessOpts = append(businessOpts,
			db.WithMySQLPool(mc.MaxIdleConns, mc.MaxOpenConns, mc.ConnMaxLifetime),
		)
		// 读写分离开启时创建 RWDB
		if cfg.Database.ReadWriteSplit.Enabled && len(mc.Slaves) > 0 {
			rw, err := db.CreateRWDBFromMySQLConfig(businessDSN, mc.Slaves, businessOpts...)
			if err != nil {
				zlog.Warn("Failed to create RW DB, falling back to single connection",
					zap.Error(err),
				)
			} else {
				zlog.Info("DB connected",
					zap.String("name", "business"),
					zap.String("driver", "mysql"),
					zap.String("mode", "rw"),
					zap.String("master", dsnAddr(businessDSN)),
					zap.Int("slaves", len(mc.Slaves)),
				)
				// 从库健康检测
				if mc.SlavePingInterval > 0 {
					pingCtx, pingCancelFn := context.WithCancel(context.Background())
					pingCancel = pingCancelFn
					go rw.SlavePingLoop(pingCtx, mc.SlavePingInterval)
				}
				businessRW = rw

				// AutoMigrate（仅主库）
				if cfg.Migration.AutoMigrate {
					if err := rw.Master().AutoMigrate(businessModels...); err != nil {
						zlog.Warn("AutoMigrate on RW master failed", zap.Error(err))
					}
				}
			}
		}
	} else if businessDriver == "postgres" {
		pc := cfg.Database.Business.Postgres
		if pc.Master != "" {
			businessDSN = pc.Master
		}
		// 读写分离开启时创建 RWDB（主从共用业务库连接池配置）
		var pcOpts []db.PostgreSQLOption
		if pc.MaxOpenConns > 0 || pc.MaxIdleConns > 0 {
			pcOpts = append(pcOpts, db.WithPostgreSQLPool(pc.MaxIdleConns, pc.MaxOpenConns, pc.ConnMaxLifetime))
		}
		if cfg.Database.ReadWriteSplit.Enabled && len(pc.Slaves) > 0 {
			rw, err := db.CreateRWDBFromPostgresConfig(businessDSN, pc.Slaves, pcOpts...)
			if err != nil {
				zlog.Warn("Failed to create RW DB, falling back to single connection",
					zap.Error(err),
				)
			} else {
				zlog.Info("DB connected",
					zap.String("name", "business"),
					zap.String("driver", "postgres"),
					zap.String("mode", "rw"),
					zap.String("master", dsnAddr(businessDSN)),
					zap.Int("slaves", len(pc.Slaves)),
				)
				businessRW = rw

				// AutoMigrate（仅主库）
				if cfg.Migration.AutoMigrate {
					if err := rw.Master().AutoMigrate(businessModels...); err != nil {
						zlog.Warn("AutoMigrate on RW master failed", zap.Error(err))
					}
				}
			}
		}
	}

	// 如果没有创建 RWDB（读写分离未启用或失败），使用单连接模式（连接失败回退 SQLite）
	if businessRW == nil {
		// 单连接路径同样按配置设置连接池（mysql/postgres 可配，sqlite 忽略）。
		var businessPool db.PoolConfig
		if businessDriver == "mysql" {
			mc := cfg.Database.Business.MySQL
			businessPool = db.PoolConfig{MaxIdleConns: mc.MaxIdleConns, MaxOpenConns: mc.MaxOpenConns, ConnMaxLifetime: mc.ConnMaxLifetime}
		} else if businessDriver == "postgres" {
			pc := cfg.Database.Business.Postgres
			businessPool = db.PoolConfig{MaxIdleConns: pc.MaxIdleConns, MaxOpenConns: pc.MaxOpenConns, ConnMaxLifetime: pc.ConnMaxLifetime}
		}
		businessDB, err := openGORMOrFallback("business", businessDriver, businessDSN, cfg.Database.Business.DSN, businessModels, cfg, businessPool)
		if err != nil {
			return nil, err
		}
		businessRW = db.CreateSingleRWDB(businessDB)
	}

	// 生产路径（auto_migrate=false）依赖 SQL 迁移建表。若迁移被标记为已应用但表实际缺失
	// （执行失败/连错库/中断），运行期才会报 42S02。此处启动即自检，缺失明确告警，避免静默。
	if !cfg.Migration.AutoMigrate {
		checkBusinessTables(businessRW.Master(), businessModels)
	}

	// 注册 business 库连接池指标（抢购 10309 容量根因：连接池/行锁饱和的直接观测入口）。
	if businessRW != nil {
		if s, e := businessRW.Master().DB(); e == nil {
			metrics.RegisterDBPool("business", s)
		}
	}

	// User DB: 用户身份数据（支持 mongodb / sqlite，mongodb 不可用时回退 sqlite）
	var userRepo repository.UserRepository
	if cfg.Database.User.Driver == "mongodb" {
		mc := cfg.Database.User.MongoDB
		if mc.DSN == "" {
			zlog.Warn("MongoDB DSN is empty, falling back to SQLite for user")
			repo, uErr := sqliteFallback("user", cfg.Database.User.DSN,
				[]interface{}{&model.User{}}, cfg, repository.NewUserRepository)
			if uErr != nil {
				return nil, uErr
			}
			userRepo = repo
		} else {
			mongoAdapter := db.NewMongoDBAdapter(db.MongoDBConfig{
				DSN:         mc.DSN,
				Database:    mc.Database,
				Username:    mc.Username,
				Password:    mc.Password,
				MinPoolSize: mc.MinPoolSize,
				MaxPoolSize: mc.MaxPoolSize,
			})
			if err := connectWithTimeout("mongodb", mongoAdapter.Connect); err != nil {
				_ = mongoAdapter.Close() // 半成功 Connect 可能已建连，显式 Close 避免泄漏（13 §3.42 ②）
				zlog.Warn("MongoDB unavailable, falling back to SQLite for user",
					zap.Error(err),
					zap.String("dsn", mc.DSN),
				)
				repo, gormErr := sqliteFallback("user", cfg.Database.User.DSN,
					[]interface{}{&model.User{}}, cfg, repository.NewUserRepository)
				if gormErr != nil {
					return nil, gormErr
				}
				userRepo = repo
			} else {
				zlog.Info("DB connected", safeConnFields("user", "mongodb", mc.DSN)...)
				userRepo = repository.NewMongoUserRepository(mongoAdapter)
				if err := userRepo.AutoMigrate(); err != nil {
					zlog.Warn("MongoDB migrate failed, falling back to SQLite for user", zap.Error(err))
					repo, gormErr := sqliteFallback("user", cfg.Database.User.DSN,
						[]interface{}{&model.User{}}, cfg, repository.NewUserRepository)
					if gormErr != nil {
						return nil, gormErr
					}
					userRepo = repo
				}
			}
		}
	} else {
		userPool := db.PoolConfig{
			MaxIdleConns:    cfg.Database.User.Pool.MaxIdleConns,
			MaxOpenConns:    cfg.Database.User.Pool.MaxOpenConns,
			ConnMaxLifetime: cfg.Database.User.Pool.ConnMaxLifetime,
		}
		userDB, err := openGORMOrFallback("user", cfg.Database.User.Driver, cfg.Database.User.DSN, cfg.Database.User.DSN,
			[]interface{}{&model.User{}}, cfg, userPool)
		if err != nil {
			return nil, err
		}
		userRepo = repository.NewUserRepository(userDB)
	}

	// 注册 user 库连接池指标（MongoDB 驱动无 *sql.DB，SQLDB() 返回 error 自动跳过）。
	if s, e := userRepo.SQLDB(); e == nil {
		metrics.RegisterDBPool("user", s)
	}

	// Monitor DB: 监控指标（支持 clickhouse / sqlite，clickhouse 不可用时回退 sqlite）
	var monitorRepo repository.MonitorRepo
	if cfg.Database.Monitor.Driver == "clickhouse" {
		cc := cfg.Database.Monitor.ClickHouse
		chAdapter := db.NewClickHouseAdapter(db.ClickHouseCfg{
			DSN:       cc.DSN,
			Addresses: cc.Addresses,
			Database:  cc.Database,
			Username:  cc.Username,
			Password:  cc.Password,
		})
		if err := connectWithTimeout("clickhouse", chAdapter.Connect); err != nil {
			// CH 不可用时回退到 SQLite；半成功 Connect 可能已建立连接，显式 Close 避免泄漏（13 §3.42 ②）
			_ = chAdapter.Close()
			zlog.Warn("ClickHouse unavailable, falling back to SQLite for monitor",
				zap.Error(err),
				zap.Strings("addresses", cc.Addresses),
			)
			repo, gormErr := sqliteFallback("monitor", cfg.Database.Monitor.DSN,
				[]interface{}{&model.MonitorMetric{}}, cfg, repository.NewMonitorRepository)
			if gormErr != nil {
				return nil, gormErr
			}
			monitorRepo = repo
		} else {
			zlog.Info("DB connected", safeConnFields("monitor", "clickhouse", cc.DSN)...)
			chRepo := repository.NewCHMonitorRepository(chAdapter)
			if err := chRepo.EnsureTable(context.Background()); err != nil {
				zlog.Warn("ClickHouse ensure table failed, falling back to SQLite", zap.Error(err))
				_ = chAdapter.Close() // 已连上但建表失败，回退前释放 CH 连接（13 §3.42 ②）
				repo, gormErr := sqliteFallback("monitor", cfg.Database.Monitor.DSN,
					[]interface{}{&model.MonitorMetric{}}, cfg, repository.NewMonitorRepository)
				if gormErr != nil {
					return nil, gormErr
				}
				monitorRepo = repo
			} else {
				monitorRepo = chRepo
			}
		}
	} else {
		monitorPool := db.PoolConfig{
			MaxIdleConns:    cfg.Database.Monitor.Pool.MaxIdleConns,
			MaxOpenConns:    cfg.Database.Monitor.Pool.MaxOpenConns,
			ConnMaxLifetime: cfg.Database.Monitor.Pool.ConnMaxLifetime,
		}
		monitorDB, err := openGORMOrFallback("monitor", cfg.Database.Monitor.Driver, cfg.Database.Monitor.DSN, cfg.Database.Monitor.DSN,
			[]interface{}{&model.MonitorMetric{}}, cfg, monitorPool)
		if err != nil {
			return nil, err
		}
		monitorRepo = repository.NewMonitorRepository(monitorDB)
	}

	// 注册 monitor 库连接池指标（ClickHouse 驱动无 *sql.DB，SQLDB() 返回 error 自动跳过）。
	if s, e := monitorRepo.SQLDB(); e == nil {
		metrics.RegisterDBPool("monitor", s)
	}

	// Log DB + 结构化登录记录仓库（需求 §6.9）：均隶属于「审计日志」域，统一读 log.driver 配置。
	// 支持 elasticsearch / sqlite：
	//   - log.driver=elasticsearch：审计日志与登录记录共享同一 ES 适配器写入 ES（避免重复建连）；
	//   - log.driver 为其它（默认 sqlite）：审计日志与登录记录均落 SQLite（与 log 库同源）；
	//   - elasticsearch 不可用时：二者统一回退 SQLite，保证登录审计结构化字段（失败原因等）可靠落库。
	// loginRepo 为 nil 时 LogService.RecordLogin 自动 no-op，不影响主流程。
	var logRepo repository.LogRepo
	var loginRepo repository.LoginRepo

	if cfg.Database.Log.Driver == "elasticsearch" {
		ec := cfg.Database.Log.Elasticsearch
		esAdapter := db.NewElasticsearchAdapter(db.ElasticsearchCfg{
			Addresses:   ec.Addresses,
			Username:    ec.Username,
			Password:    ec.Password,
			IndexPrefix: ec.IndexPrefix,
		})
		if err := connectWithTimeout("elasticsearch", esAdapter.Connect); err != nil {
			// ES 不可用时回退到 SQLite；半成功 Connect 可能已建连，显式 Close 释放底层 transport 连接池避免泄漏
			// （13 §3.42 ②）。登录记录与审计日志共用同一适配器，一并回退。
			_ = esAdapter.Close()
			zlog.Warn("Elasticsearch unavailable, falling back to SQLite for log and login_record",
				zap.Error(err),
				zap.Strings("addresses", ec.Addresses),
			)
			repo, gormErr := sqliteFallback("log", cfg.Database.Log.DSN,
				[]interface{}{&model.AuditLog{}}, cfg, repository.NewLogRepository)
			if gormErr != nil {
				return nil, gormErr
			}
			logRepo = repo
			loginFb, loginErr := sqliteFallback("login_record", cfg.Database.Log.DSN,
				[]interface{}{&model.LoginRecord{}}, cfg, repository.NewLoginRepository)
			if loginErr != nil {
				return nil, loginErr
			}
			loginRepo = loginFb
		} else {
			zlog.Info("DB connected",
				zap.String("driver", "elasticsearch"),
				zap.String("name", "log"),
				zap.Strings("addresses", ec.Addresses),
				zap.String("index_prefix", ec.IndexPrefix),
			)
			logRepo = repository.NewESLogRepository(esAdapter)
			loginRepo = repository.NewESLoginRepository(esAdapter)
		}
	} else {
		logPool := db.PoolConfig{
			MaxIdleConns:    cfg.Database.Log.Pool.MaxIdleConns,
			MaxOpenConns:    cfg.Database.Log.Pool.MaxOpenConns,
			ConnMaxLifetime: cfg.Database.Log.Pool.ConnMaxLifetime,
		}
		logDB, err := openGORMOrFallback("log", cfg.Database.Log.Driver, cfg.Database.Log.DSN, cfg.Database.Log.DSN,
			[]interface{}{&model.AuditLog{}}, cfg, logPool)
		if err != nil {
			return nil, err
		}
		logRepo = repository.NewLogRepository(logDB)

		// 登录记录与审计日志同源（默认 sqlite），复用 log.driver 配置下的 DSN / 驱动，
		// 主驱动不可用时同样回退 sqlite（保证登录审计结构化字段可靠落库）。
		loginDB, lerr := openGORMOrFallback("login_record", cfg.Database.Log.Driver, cfg.Database.Log.DSN, cfg.Database.Log.DSN,
			[]interface{}{&model.LoginRecord{}}, cfg, logPool)
		if lerr != nil {
			zlog.Warn("login_record init failed, login records disabled", zap.Error(lerr))
		} else {
			loginRepo = repository.NewLoginRepository(loginDB)
		}
	}

	// 注册 log / login_record 库连接池指标（ES 驱动无 *sql.DB，SQLDB() 返回 error 自动跳过）。
	if logRepo != nil {
		if s, e := logRepo.SQLDB(); e == nil {
			metrics.RegisterDBPool("log", s)
		}
	}
	if loginRepo != nil {
		if s, e := loginRepo.SQLDB(); e == nil {
			metrics.RegisterDBPool("login_record", s)
		}
	}

	logger.L().Info("All databases initialized and migrated")
	return &Databases{
		BusinessRW:  businessRW,
		UserRepo:    userRepo,
		MonitorRepo: monitorRepo,
		LogRepo:     logRepo,
		LoginRepo:   loginRepo,
		pingCancel:  pingCancel,
	}, nil
}

// ──────────────────────────────────────────────────────────────
// DSN 解析辅助（脱敏输出）
// ──────────────────────────────────────────────────────────────

// safeConnFields 从 DSN 构造数据库连接日志字段，自动脱敏密码
// 输出示例: [name=business, driver=mysql, addr=127.0.0.1:3306, db=hc_business]
func safeConnFields(name, driver, dsn string) []zap.Field {
	addr, db := extractAddrDB(dsn)
	fields := []zap.Field{
		zap.String("driver", driver),
		zap.String("name", name),
	}
	if addr != "" {
		fields = append(fields, zap.String("addr", addr))
	}
	if db != "" {
		fields = append(fields, zap.String("db", db))
	}
	return fields
}

// dsnAddr 从 DSN URL 中仅提取地址部分（host:port），不泄露密码
func dsnAddr(dsn string) string {
	addr, _ := extractAddrDB(dsn)
	return addr
}

// extractAddrDB 从 DSN 中提取地址和数据库名
// 层级 URL:   mysql://user:pass@host:port/db → (host:port, db)
// 非层级 DSN: file:./data/foo.db?params        → (./data/foo.db, "")
func extractAddrDB(dsn string) (addr, db string) {
	if dsn == "" {
		return "", ""
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn, ""
	}
	// 非层级 URL（如 SQLite 的 file:path?params），Opaque 包含路径+查询
	if u.Opaque != "" {
		path := u.Opaque
		if idx := strings.Index(path, "?"); idx != -1 {
			path = path[:idx]
		}
		return path, ""
	}
	if u.Host != "" {
		addr = u.Host
		db = strings.TrimPrefix(u.Path, "/")
	} else if u.Path != "" {
		// 无主机（sqlite 等文件型）：整个路径作为连接位置，无独立「库名」
		addr = u.Path
	}
	return
}

// ──────────────────────────────────────────────────────────────
// 业务辅助（种子数据 / 扫描间隔 / 降级事件桥接）
// ──────────────────────────────────────────────────────────────

// SeedData 首次运行时自动创建种子数据（任务与商城商品）。
func SeedData(taskSvc *task.TaskService, shopSvc *shop.ShopService) error {
	zlog := logger.L()
	if err := taskSvc.SeedTasks(); err != nil {
		return fmt.Errorf("seed tasks: %w", err)
	}
	zlog.Info("Tasks seeded")
	if err := shopSvc.SeedItems(); err != nil {
		return fmt.Errorf("seed items: %w", err)
	}
	zlog.Info("Shop items seeded")
	return nil
}

// IdleScanInterval 解析 idle.offline_check_interval（cron 分钟字段）为扫描间隔，
// 并钳制到 [timeout/3, 2*timeout] 之间：既保证及时检测超时，又避免扫描过于频繁。
func IdleScanInterval(cfg *config.Config) time.Duration {
	d, ok := scheduler.ParseCronMinute(cfg.Idle.OfflineCheckInterval)
	if !ok || d <= 0 {
		d = cfg.Idle.TimeoutThreshold
	}
	minI := cfg.Idle.TimeoutThreshold / 3
	if minI < 15*time.Second {
		minI = 15 * time.Second
	}
	if d < minI {
		d = minI
	}
	maxI := cfg.Idle.TimeoutThreshold * 2
	if d > maxI {
		d = maxI
	}
	return d
}

// degradeSender 实现 cache.DegradeEventSender：
// 写降级（cache.degrade.write_async 开启或熔断触发）时，将缓存失效事件投递到事件总线
// （kafka/rabbitmq/rocketmq），由消费者异步刷新缓存（同步转异步，需求 §11）。主题由生产者按
// 当前 MQ 类型经 topicMapOf 解析，故此处无需持有 topic 字段。
type degradeSender struct {
	producer mq.Producer
}

// NewDegradeSender 构造缓存写降级事件桥接器（实现 cache.DegradeEventSender）。
// 由 main 在事件生产者就绪后注入 cache.Manager；无事件总线时不应注入（退化为本地异步写）。
func NewDegradeSender(producer mq.Producer) cache.DegradeEventSender {
	return &degradeSender{producer: producer}
}

func (k *degradeSender) PublishCacheInvalidate(ctx context.Context, key string) error {
	return k.producer.Send(ctx, &event.Message{
		Timestamp: time.Now(),
		EventType: event.EventCacheInvalidate,
		Key:       key,
		Payload:   map[string]string{"key": key},
	})
}
