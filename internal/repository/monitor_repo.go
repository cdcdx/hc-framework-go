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
	return r.db.WithContext(ctx).Create(metric).Error
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
	return r.db.WithContext(ctx).CreateInBatches(clean, 200).Error
}

// CountByType 按指标类型统计（用于监控面板）
func (r *MonitorRepository) CountByType(ctx context.Context, metricType string, start, end time.Time) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&model.MonitorMetric{}).
		Where("metric_type = ? AND created_at BETWEEN ? AND ?", metricType, start, end).
		Count(&count).Error
	return count, err
}

// SumByType 按指标类型求和
func (r *MonitorRepository) SumByType(ctx context.Context, metricType string, start, end time.Time) (float64, error) {
	var sum float64
	err := r.db.WithContext(ctx).Model(&model.MonitorMetric{}).
		Select("COALESCE(SUM(value), 0)").
		Where("metric_type = ? AND created_at BETWEEN ? AND ?", metricType, start, end).
		Scan(&sum).Error
	return sum, err
}

// FindByTimeRange 按时间范围查询指标
func (r *MonitorRepository) FindByTimeRange(ctx context.Context, start, end time.Time, limit int) ([]model.MonitorMetric, error) {
	var metrics []model.MonitorMetric
	err := r.db.WithContext(ctx).
		Where("created_at BETWEEN ? AND ?", start, end).
		Order("created_at DESC").
		Limit(limit).
		Find(&metrics).Error
	return metrics, err
}

// Close 关闭底层 GORM/sql 连接池，释放监控库（回退 SQLite 时即 SQLite 连接）连接。
func (r *MonitorRepository) Close() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// SQLDB 返回底层 *sql.DB，供监控采集连接池统计。
func (r *MonitorRepository) SQLDB() (*sql.DB, error) {
	return r.db.DB()
}
