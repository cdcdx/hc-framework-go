package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// LogRepository 日志仓库
type LogRepository struct {
	db *gorm.DB
}

// NewLogRepository 创建日志仓库
func NewLogRepository(db *gorm.DB) *LogRepository {
	return &LogRepository{db: db}
}

// Create 写入一条审计日志
func (r *LogRepository) Create(ctx context.Context, log *model.AuditLog) error {
	if r.db == nil {
		return fmt.Errorf("log repo: db not initialized")
	}
	if log == nil {
		return fmt.Errorf("log repo: nil AuditLog passed to Create")
	}
	return r.db.WithContext(ctx).Create(log).Error
}

// CreateBatch 批量写入审计日志（GORM CreateInBatches，单条 SQL 多值插入）。
func (r *LogRepository) CreateBatch(ctx context.Context, logs []*model.AuditLog) error {
	if r.db == nil {
		return fmt.Errorf("log repo: db not initialized")
	}
	if len(logs) == 0 {
		return nil
	}
	// 过滤 nil 元素，避免 GORM CreateInBatches 对 nil 元素 panic（13 §3.40 ①）
	clean := make([]*model.AuditLog, 0, len(logs))
	for _, log := range logs {
		if log != nil {
			clean = append(clean, log)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).CreateInBatches(clean, 200).Error
}

// FindByUser 按用户查询日志（游标分页）
func (r *LogRepository) FindByUser(ctx context.Context, userID string, cursor int64, limit int) ([]model.AuditLog, error) {
	var logs []model.AuditLog
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND id > ?", userID, cursor).
		Order("id DESC").
		Limit(limit + 1).
		Find(&logs).Error
	return logs, err
}

// FindByType 按事件类型查询日志（用于运营后台）
func (r *LogRepository) FindByType(ctx context.Context, eventType string, start, end time.Time, limit int) ([]model.AuditLog, error) {
	var logs []model.AuditLog
	err := r.db.WithContext(ctx).
		Where("event_type = ? AND created_at BETWEEN ? AND ?", eventType, start, end).
		Order("created_at DESC").
		Limit(limit).
		Find(&logs).Error
	return logs, err
}

// CountByType 按事件类型统计（用于监控面板）
func (r *LogRepository) CountByType(ctx context.Context, eventType string, start, end time.Time) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&model.AuditLog{}).
		Where("event_type = ? AND created_at BETWEEN ? AND ?", eventType, start, end).
		Count(&count).Error
	return count, err
}

// Close 关闭底层 GORM/sql 连接池，释放日志库（回退 SQLite 时即 SQLite 连接）连接。
func (r *LogRepository) Close() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// SQLDB 返回底层 *sql.DB，供监控采集连接池统计。
func (r *LogRepository) SQLDB() (*sql.DB, error) {
	return r.db.DB()
}
