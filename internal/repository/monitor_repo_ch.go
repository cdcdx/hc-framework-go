package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// CHMonitorRepository ClickHouse 监控仓库
type CHMonitorRepository struct {
	adapter *db.ClickHouseAdapter
	dbName  string
}

// NewCHMonitorRepository 创建 CH 监控仓库
func NewCHMonitorRepository(adapter *db.ClickHouseAdapter) *CHMonitorRepository {
	return &CHMonitorRepository{
		adapter: adapter,
		dbName:  adapter.Database(),
	}
}

// Close 关闭 ClickHouse 连接（转发到 adapter）。
func (r *CHMonitorRepository) Close() error {
	return r.adapter.Close()
}

// SQLDB ClickHouse 实现无 *sql.DB（使用 clickhouse-go 原生 conn），返回错误（监控跳过）。
func (r *CHMonitorRepository) SQLDB() (*sql.DB, error) {
	return nil, errors.New("clickhouse repository has no *sql.DB")
}

// connOrErr 返回已连接的 ClickHouse conn；adapter 未成功 Connect 时 Conn() 为 nil，
// 直接调用会 panic，此处显式防御并返回明确错误（13 §3.40）。
func (r *CHMonitorRepository) connOrErr() (clickhouse.Conn, error) {
	if r.adapter == nil {
		return nil, fmt.Errorf("clickhouse: adapter nil")
	}
	c := r.adapter.Conn()
	if c == nil {
		return nil, fmt.Errorf("clickhouse: not connected")
	}
	return c, nil
}

// EnsureTable 确保监控指标表存在
func (r *CHMonitorRepository) EnsureTable(ctx context.Context) error {
	conn, err := r.connOrErr()
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.monitor_metrics (
			id          Int64,
			user_id     String,
			metric_type String,
			metric_name String,
			value       Float64,
			tags        String,
			created_at  DateTime
		) ENGINE = MergeTree()
		ORDER BY (metric_type, created_at)
		PARTITION BY toYYYYMMDD(created_at)
	`, r.dbName)

	return conn.Exec(ctx, sql)
}

// Record 写入一条监控指标到 ClickHouse
func (r *CHMonitorRepository) Record(ctx context.Context, metric *model.MonitorMetric) error {
	conn, err := r.connOrErr()
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`
		INSERT INTO %s.monitor_metrics (id, user_id, metric_type, metric_name, value, tags, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, r.dbName)

	return conn.Exec(ctx, sql,
		metric.ID,
		metric.UserID,
		metric.MetricType,
		metric.MetricName,
		metric.Value,
		metric.Tags,
		metric.CreatedAt,
	)
}

// RecordBatch 批量写入监控指标到 ClickHouse。
// 使用 ClickHouse 原生 PrepareBatch（列式批量写入），相比逐行 INSERT 可数量级降低
// part 数量与网络往返，是 ClickHouse 官方推荐的写入方式（逐行 INSERT 会导致过载 i/o timeout）。
func (r *CHMonitorRepository) RecordBatch(ctx context.Context, metrics []*model.MonitorMetric) error {
	if len(metrics) == 0 {
		return nil
	}
	conn, err := r.connOrErr()
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`INSERT INTO %s.monitor_metrics (id, user_id, metric_type, metric_name, value, tags, created_at)`, r.dbName)
	batch, err := conn.PrepareBatch(ctx, sql)
	if err != nil {
		return fmt.Errorf("clickhouse prepare batch: %w", err)
	}
	// 无论 Send 成功与否都释放 batch（未 Send 时 Close 等价于 Abort），避免资源泄漏（13 §3.40）。
	defer batch.Close()
	for _, m := range metrics {
		if err := batch.Append(m.ID, m.UserID, m.MetricType, m.MetricName, m.Value, m.Tags, m.CreatedAt); err != nil {
			return fmt.Errorf("clickhouse batch append: %w", err)
		}
	}
	return batch.Send()
}

// CountByType 按指标类型统计
func (r *CHMonitorRepository) CountByType(ctx context.Context, metricType string, start, end time.Time) (int64, error) {
	conn, err := r.connOrErr()
	if err != nil {
		return 0, err
	}
	sql := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s.monitor_metrics
		WHERE metric_type = ? AND created_at >= ? AND created_at < ?
	`, r.dbName)

	var count uint64
	err = conn.QueryRow(ctx, sql, metricType, start, end).Scan(&count)
	return int64(count), err
}

// SumByType 按指标类型求和
func (r *CHMonitorRepository) SumByType(ctx context.Context, metricType string, start, end time.Time) (float64, error) {
	sql := fmt.Sprintf(`
		SELECT COALESCE(SUM(value), 0) FROM %s.monitor_metrics
		WHERE metric_type = ? AND created_at >= ? AND created_at < ?
	`, r.dbName)

	var sum float64
	conn, err := r.connOrErr()
	if err != nil {
		return 0, err
	}
	err = conn.QueryRow(ctx, sql, metricType, start, end).Scan(&sum)
	return sum, err
}

// FindByTimeRange 按时间范围查询指标
func (r *CHMonitorRepository) FindByTimeRange(ctx context.Context, start, end time.Time, limit int) ([]model.MonitorMetric, error) {
	sql := fmt.Sprintf(`
		SELECT id, user_id, metric_type, metric_name, value, tags, created_at
		FROM %s.monitor_metrics
		WHERE created_at >= ? AND created_at < ?
		ORDER BY created_at DESC
		LIMIT ?
	`, r.dbName)

	conn, err := r.connOrErr()
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, sql, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []model.MonitorMetric
	for rows.Next() {
		var m model.MonitorMetric
		if err := rows.Scan(&m.ID, &m.UserID, &m.MetricType, &m.MetricName, &m.Value, &m.Tags, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("clickhouse scan: %w", err)
		}
		metrics = append(metrics, m)
	}

	// 兼容 clickhouse-go v2 的 nil 检查
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return metrics, nil
}
