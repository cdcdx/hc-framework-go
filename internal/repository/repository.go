package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// LogRepo 日志仓库接口（支持 GORM / Elasticsearch）
type LogRepo interface {
	Create(ctx context.Context, log *model.AuditLog) error
	FindByUser(ctx context.Context, userID string, cursor int64, limit int) ([]model.AuditLog, error)
	FindByType(ctx context.Context, eventType string, start, end time.Time, limit int) ([]model.AuditLog, error)
	CountByType(ctx context.Context, eventType string, start, end time.Time) (int64, error)
	// SQLDB 返回底层 *sql.DB（仅 GORM/SQLite 实现可用；ES 实现返回 error）。
	// 用于监控采集连接池统计（metrics.RegisterDBPool）。
	SQLDB() (*sql.DB, error)
	// Close 释放底层连接（GORM 关闭 sql.DB；ES 关闭底层 transport 释放连接池）。优雅关闭时调用。
	Close() error
}

// MonitorRepo 监控仓库接口（支持 GORM / ClickHouse）
type MonitorRepo interface {
	Record(ctx context.Context, metric *model.MonitorMetric) error
	CountByType(ctx context.Context, metricType string, start, end time.Time) (int64, error)
	SumByType(ctx context.Context, metricType string, start, end time.Time) (float64, error)
	FindByTimeRange(ctx context.Context, start, end time.Time, limit int) ([]model.MonitorMetric, error)
	// SQLDB 返回底层 *sql.DB（仅 GORM/SQLite 实现可用；ClickHouse 实现返回 error）。
	// 用于监控采集连接池统计（metrics.RegisterDBPool）。
	SQLDB() (*sql.DB, error)
	// Close 释放底层连接（GORM 关闭 sql.DB；ClickHouse 关闭原生 conn）。优雅关闭时调用。
	Close() error
}

// BatchLogRepo 可选批量写接口。日志写入器优先使用批量写以降低后端压力
// （ClickHouse/ES 逐行写是致命反模式，批量可数量级降低 part 数与网络往返）。
// 未实现该接口的仓库由 LogService 自动回退为逐条写。
type BatchLogRepo interface {
	CreateBatch(ctx context.Context, logs []*model.AuditLog) error
}

// BatchMonitorRepo 可选批量写接口，语义同 BatchLogRepo。
type BatchMonitorRepo interface {
	RecordBatch(ctx context.Context, metrics []*model.MonitorMetric) error
}
