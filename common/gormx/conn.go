// Package gormx GORM 连接工厂（多方言）。
package gormx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/common/metrics"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// IsRecordNotFound 鲁棒判定"查无记录"错误。
// gorm 在部分驱动/链路下会用 fmt.Errorf 包装 ErrRecordNotFound，
// 上层直接 `== gorm.ErrRecordNotFound` 会漏判（误报 CodeDBError 而非 CodeNotFound）。
// 此处同时兼容直接相等与 errors.Is 包装两种情形。
func IsRecordNotFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, gorm.ErrRecordNotFound)
}

// PoolConfig 连接池配置（0 表示使用驱动默认值）。
type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	// Label 用于 db_pool_* 指标的 db 标签，便于多库区分；默认 "business"。
	Label string
}

// Open 按 driver 打开数据库连接（使用驱动默认连接池）：
//   - mysql / postgres / postgresql / sqlite
func Open(driver, dsn string) (*gorm.DB, error) {
	return OpenWithPool(driver, dsn, PoolConfig{})
}

// defaultMaxOpenConns / defaultMaxIdleConns 是连接池的兜底上限。
// 当上层未显式配置 MaxOpenConns（即 <=0）时采用，避免驱动默认值（mysql 为 0 = 无限制）
// 在高并发下无限创建连接，打爆数据库 max_connections（Error 1040 Too many connections）。
// 取值保守：单进程常配置 business/user/monitor/log 等多个库，各库连接数相加须低于
// MySQL 默认 max_connections(151)，否则多库总和直接触发 Error 1040。
const (
	defaultMaxOpenConns = 20
	defaultMaxIdleConns = 10
	// hardMaxOpenConns 是单库连接池的绝对上限，防止配置值过大（如 200）单实例就打爆 DB。
	// 最终生效值 = min(配置值, hardMaxOpenConns)。
	hardMaxOpenConns = 100
)

// OpenWithPool 在 Open 基础上应用连接池限制，并后台采集 db_pool_utilization /
// db_pool_wait_count_total 指标。
// 连接池兜底：MaxOpenConns <= 0 时强制设为 defaultMaxOpenConns（mysql 驱动默认 0=无限制，
// 是 Error 1040 的根因）；MaxIdleConns <= 0 时设为 defaultMaxIdleConns。
// driver == "sqlite" 时不兜底（本地单文件库，无服务端连接数约束）。
func OpenWithPool(driver, dsn string, pool PoolConfig) (*gorm.DB, error) {
	var dialector gorm.Dialector
	switch driver {
	case "mysql":
		dialector = mysql.Open(dsn)
	case "postgres", "postgresql":
		dialector = postgres.Open(dsn)
	case "sqlite":
		dialector = sqlite.Open(dsn)
	default:
		return nil, fmt.Errorf("unsupported db driver: %q (supported: sqlite, mysql, postgres)", driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		// 忽略 ErrRecordNotFound 的日志（幂等查询的正常路径，不是错误）。
		// 慢查询 (>200ms) 和普通 SQL 仍正常打印。
		// Logger: &ignoreNotFoundLogger{delegate: logger.Default.LogMode(logger.Warn)},
	})
	if err != nil {
		return nil, fmt.Errorf("open %s db: %w", driver, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB: %w", err)
	}

	// 连接池兜底上限：
	// - mysql/postgres 等客户端-服务端库，MaxOpenConns<=0 时强制兜底（驱动默认 0=无限制，
	//   是 Error 1040 Too many connections 的根因）。
	// - sqlite 单文件库无服务端连接数约束，仅当显式配置 >0 时应用，否则保留默认（不限）。
	maxOpen, maxIdle := resolvePoolDefaults(driver, pool)
	// 单库连接池绝对上限保护：防止配置值过大单实例直接打爆数据库 max_connections。
	if maxOpen > hardMaxOpenConns {
		maxOpen = hardMaxOpenConns
	}
	if maxOpen > 0 {
		sqlDB.SetMaxOpenConns(maxOpen)
	}
	if maxIdle > 0 {
		sqlDB.SetMaxIdleConns(maxIdle)
	}
	if pool.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(pool.ConnMaxLifetime)
	}

	label := pool.Label
	if label == "" {
		label = "business"
	}
	startPoolMetrics(sqlDB, label)
	enableTableMetrics(db, label)

	return db, nil
}

// resolvePoolDefaults 计算最终生效的连接池参数。
// 规则：
//   - MaxOpenConns/MaxIdleConns > 0：尊重显式配置。
//   - 否则，mysql/postgres 等客户端-服务端库兜底为 defaultMaxOpenConns/defaultMaxIdleConns
//     （驱动默认 0=无限制，是高并发下 Error 1040 Too many connections 的根因）。
//   - sqlite 单文件库无服务端连接数约束，未显式配置时返回 0（不限制）。
func resolvePoolDefaults(driver string, pool PoolConfig) (maxOpen, maxIdle int) {
	if pool.MaxOpenConns > 0 {
		maxOpen = pool.MaxOpenConns
	} else if driver != "sqlite" {
		maxOpen = defaultMaxOpenConns
	}
	if pool.MaxIdleConns > 0 {
		maxIdle = pool.MaxIdleConns
	} else if driver != "sqlite" {
		maxIdle = defaultMaxIdleConns
	}
	return maxOpen, maxIdle
}

// startPoolMetrics 周期性把连接池统计写入 Prometheus 指标。
// WaitCount 为累计值，这里记录差值作为 Counter 增量；Utilization 直接写 Gauge。
func startPoolMetrics(sqlDB *sql.DB, label string) {
	lastWait := int64(0)
	if st := sqlDB.Stats(); st.MaxOpenConnections > 0 {
		lastWait = st.WaitCount
	}
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			st := sqlDB.Stats()
			if st.MaxOpenConnections > 0 {
				metrics.DBPoolUtilization.WithLabelValues(label).Set(
					float64(st.InUse) / float64(st.MaxOpenConnections))
				if delta := st.WaitCount - lastWait; delta > 0 {
					metrics.DBPoolWaitCount.WithLabelValues(label).Add(float64(delta))
					lastWait = st.WaitCount
				}
			}
		}
	}()
}

// ignoreNotFoundLogger 包装 gorm Logger，在 Trace 层过滤 ErrRecordNotFound 的日志输出。
// 幂等查询（如 Start 中先查是否存在活跃记录）遇到 record not found 是正常路径，
// 不应以 WARNING 级别打印 SQL，避免日志噪音。
type ignoreNotFoundLogger struct {
	delegate logger.Interface
}

func (l *ignoreNotFoundLogger) LogMode(level logger.LogLevel) logger.Interface {
	return &ignoreNotFoundLogger{delegate: l.delegate.LogMode(level)}
}

func (l *ignoreNotFoundLogger) Info(ctx context.Context, msg string, data ...any) {
	l.delegate.Info(ctx, msg, data...)
}

func (l *ignoreNotFoundLogger) Warn(ctx context.Context, msg string, data ...any) {
	l.delegate.Warn(ctx, msg, data...)
}

func (l *ignoreNotFoundLogger) Error(ctx context.Context, msg string, data ...any) {
	l.delegate.Error(ctx, msg, data...)
}

func (l *ignoreNotFoundLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if err != nil && err.Error() == "record not found" {
		// 静默忽略：这是正常的幂等查询路径，不是错误。
		return
	}
	l.delegate.Trace(ctx, begin, fc, err)
}

// enableTableMetrics 为 gorm.DB 注册 Before/After 回调对，按 db+table+operation 维度
// 计数每次表级操作、记录耗时和错误，写入 Prometheus Counter/Histogram。
// 与连接池指标一样在 OpenWithPool 内自动启用，调用方无需额外操作。
func enableTableMetrics(db *gorm.DB, label string) {
	ops := []struct {
		name string
		op   string
	}{
		{"query", "select"},
		{"create", "insert"},
		{"update", "update"},
		{"delete", "delete"},
		{"raw", "raw"},
	}

	for _, o := range ops {
		name, op := o.name, o.op

		// 按操作类型注册 Before/After 回调对。
		// Before：记录开始时间戳到 db 实例（线程安全，gorm 每次操作创建新 session）。
		// After：计数 + 延迟 + 错误。
		switch name {
		case "query":
			db.Callback().Query().Before("gorm:before_query").Register("metrics:before_query", func(d *gorm.DB) {
				d.InstanceSet("metrics_start", time.Now())
			})
			db.Callback().Query().After("gorm:after_query").Register("metrics:after_query", func(d *gorm.DB) {
				recordTableMetrics(d, label, op)
			})
		case "create":
			db.Callback().Create().Before("gorm:before_create").Register("metrics:before_create", func(d *gorm.DB) {
				d.InstanceSet("metrics_start", time.Now())
			})
			db.Callback().Create().After("gorm:after_create").Register("metrics:after_create", func(d *gorm.DB) {
				recordTableMetrics(d, label, op)
			})
		case "update":
			db.Callback().Update().Before("gorm:before_update").Register("metrics:before_update", func(d *gorm.DB) {
				d.InstanceSet("metrics_start", time.Now())
			})
			db.Callback().Update().After("gorm:after_update").Register("metrics:after_update", func(d *gorm.DB) {
				recordTableMetrics(d, label, op)
			})
		case "delete":
			db.Callback().Delete().Before("gorm:before_delete").Register("metrics:before_delete", func(d *gorm.DB) {
				d.InstanceSet("metrics_start", time.Now())
			})
			db.Callback().Delete().After("gorm:after_delete").Register("metrics:after_delete", func(d *gorm.DB) {
				recordTableMetrics(d, label, op)
			})
		case "raw":
			db.Callback().Raw().Before("gorm:before_raw").Register("metrics:before_raw", func(d *gorm.DB) {
				d.InstanceSet("metrics_start", time.Now())
			})
			db.Callback().Raw().After("gorm:after_raw").Register("metrics:after_raw", func(d *gorm.DB) {
				recordTableMetrics(d, label, op)
			})
		}
	}
}

// recordTableMetrics 记录单次表操作的三维指标：计数 + 延迟 + 错误。
func recordTableMetrics(d *gorm.DB, label, op string) {
	table := d.Statement.Table
	if table == "" {
		table = "_unknown"
	}
	metrics.DBOperationsTotal.WithLabelValues(label, table, op).Inc()

	if start, ok := d.InstanceGet("metrics_start"); ok {
		metrics.DBOperationDurationSeconds.WithLabelValues(label, table, op).
			Observe(time.Since(start.(time.Time)).Seconds())
	}

	if d.Error != nil {
		metrics.DBOperationErrorsTotal.WithLabelValues(label, table, op).Inc()
	}
}

// EnableTableMetrics 为已有的 *gorm.DB 手动启用表操作指标。
// 大多数情况下不需要手动调用——OpenWithPool 已内置启用。
// 仅当通过 gorm.Open 等非 OpenWithPool 路径获取 db 时才需调用。
func EnableTableMetrics(db *gorm.DB, label string) {
	enableTableMetrics(db, label)
}
