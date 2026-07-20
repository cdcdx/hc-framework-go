package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// MonitorRepository 监控仓库
type MonitorRepository struct {
	db *gorm.DB
}

// NewMonitorRepository 创建监控仓库
func NewMonitorRepository(db *gorm.DB) *MonitorRepository {
	return &MonitorRepository{db: db}
}

// Record 写入一条监控指标
func (r *MonitorRepository) Record(ctx context.Context, metric *model.MonitorMetric) error {
	if r.db == nil {
		return fmt.Errorf("monitor repo: db not initialized")
	}
	if metric == nil {
		return fmt.Errorf("monitor repo: nil MonitorMetric passed to Record")
	}
	// 省略 ID 列：ID 由 LogService 赋进程内序列值（供 ClickHouse 等无默认列后端使用），
	// 但关系库（MySQL/SQLite）的 id 是 autoIncrement 主键，显式写入会与自增计数器冲突
	// 导致 Error 1062 Duplicate entry。交给 DB 自增生成主键（见 docs/603 §2.9）。
	return r.db.WithContext(ctx).Omit("ID").Create(metric).Error
}

// RecordBatch 批量写入监控指标（GORM CreateInBatches，单条 SQL 多值插入）。
func (r *MonitorRepository) RecordBatch(ctx context.Context, metrics []*model.MonitorMetric) error {
	if r.db == nil {
		return fmt.Errorf("monitor repo: db not initialized")
	}
	if len(metrics) == 0 {
		return nil
	}
	// 过滤 nil 元素，避免 GORM CreateInBatches 对 nil 元素 panic（13 §3.40 ①）
	clean := make([]*model.MonitorMetric, 0, len(metrics))
	for _, metric := range metrics {
		if metric != nil {
			clean = append(clean, metric)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	// 省略 ID 列，理由同 Record（见 docs/603 §2.9）。
	return r.db.WithContext(ctx).Omit("ID").CreateInBatches(clean, 200).Error
}

// CountByType 按指标类型统计（用于监控面板）
func (r *MonitorRepository) CountByType(ctx context.Context, metricType string, start, end time.Time) (int64, error) {
	if r.db == nil {
		return 0, fmt.Errorf("monitor repo: db not initialized")
	}
	var count int64
	err := r.db.WithContext(ctx).Model(&model.MonitorMetric{}).
		Where("metric_type = ? AND created_at >= ? AND created_at < ?", metricType, start, end).
		Count(&count).Error
	return count, err
}

// SumByType 按指标类型求和
func (r *MonitorRepository) SumByType(ctx context.Context, metricType string, start, end time.Time) (float64, error) {
	if r.db == nil {
		return 0, fmt.Errorf("monitor repo: db not initialized")
	}
	var sum float64
	err := r.db.WithContext(ctx).Model(&model.MonitorMetric{}).
		Select("COALESCE(SUM(value), 0)").
		Where("metric_type = ? AND created_at >= ? AND created_at < ?", metricType, start, end).
		Scan(&sum).Error
	return sum, err
}

// FindByTimeRange 按时间范围查询指标
func (r *MonitorRepository) FindByTimeRange(ctx context.Context, start, end time.Time, limit int) ([]model.MonitorMetric, error) {
	if r.db == nil {
		return nil, fmt.Errorf("monitor repo: db not initialized")
	}
	var metrics []model.MonitorMetric
	err := r.db.WithContext(ctx).
		Where("created_at >= ? AND created_at < ?", start, end).
		Order("created_at DESC").
		Limit(limit).
		Find(&metrics).Error
	return metrics, err
}

// Close 关闭底层 GORM/sql 连接池，释放监控库（回退 SQLite 时即 SQLite 连接）连接。
func (r *MonitorRepository) Close() error {
	if r.db == nil {
		return nil // 无连接可释放（与读/写路径 nil-db 守卫一致，避免 shutdown 时 panic）
	}
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// SQLDB 返回底层 *sql.DB，供监控采集连接池统计。
func (r *MonitorRepository) SQLDB() (*sql.DB, error) {
	if r.db == nil {
		return nil, fmt.Errorf("monitor repo: db not initialized")
	}
	return r.db.DB()
}
