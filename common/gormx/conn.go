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

// OpenWithPool 在 Open 基础上应用连接池限制，并后台采集 db_pool_utilization /
// db_pool_wait_count_total 指标（仅当 MaxOpenConns > 0 时启用连接池限制）。
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
		Logger: &ignoreNotFoundLogger{delegate: logger.Default.LogMode(logger.Warn)},
	})
	if err != nil {
		return nil, fmt.Errorf("open %s db: %w", driver, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB: %w", err)
	}

	if pool.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(pool.MaxOpenConns)
	}
	if pool.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(pool.MaxIdleConns)
	}
	if pool.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(pool.ConnMaxLifetime)
	}

	label := pool.Label
	if label == "" {
		label = "business"
	}
	startPoolMetrics(sqlDB, label)

	return db, nil
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
