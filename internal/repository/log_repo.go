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
	// 省略 ID 列：ID 由 LogService 赋进程内序列值（供 ClickHouse 等无默认列后端使用），
	// 但关系库（MySQL/SQLite）的 id 是 autoIncrement 主键，显式写入会与自增计数器冲突
	// 导致 Error 1062 Duplicate entry（且进程重启后序列归零必然撞历史主键）。
	// 交给 DB 自增生成主键，避免主键冲突（见 docs/603 §2.9）。
	return r.db.WithContext(ctx).Omit("ID").Create(log).Error
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
	// 省略 ID 列，理由同 Create（见 docs/603 §2.9）。
	return r.db.WithContext(ctx).Omit("ID").CreateInBatches(clean, 200).Error
}

// FindByUser 按用户查询日志（游标分页）
func (r *LogRepository) FindByUser(ctx context.Context, userID string, cursor int64, limit int) ([]model.AuditLog, error) {
	if r.db == nil {
		return nil, fmt.Errorf("log repo: db not initialized")
	}
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
	if r.db == nil {
		return nil, fmt.Errorf("log repo: db not initialized")
	}
	var logs []model.AuditLog
	err := r.db.WithContext(ctx).
		Where("event_type = ? AND created_at >= ? AND created_at < ?", eventType, start, end).
		Order("created_at DESC").
		Limit(limit).
		Find(&logs).Error
	return logs, err
}

// CountByType 按事件类型统计（用于监控面板）
func (r *LogRepository) CountByType(ctx context.Context, eventType string, start, end time.Time) (int64, error) {
	if r.db == nil {
		return 0, fmt.Errorf("log repo: db not initialized")
	}
	var count int64
	err := r.db.WithContext(ctx).Model(&model.AuditLog{}).
		Where("event_type = ? AND created_at >= ? AND created_at < ?", eventType, start, end).
		Count(&count).Error
	return count, err
}

// CountByTypeAndResult 统计某事件类型在 [start,end] 内、且 login_result=result 的行数。
// 合并 login_records 后登录是 audit_logs 的一类事件，需用 login_result 过滤以区分成功/失败登录。
func (r *LogRepository) CountByTypeAndResult(ctx context.Context, eventType, result string, start, end time.Time) (int64, error) {
	if r.db == nil {
		return 0, fmt.Errorf("log repo: db not initialized")
	}
	var count int64
	q := r.db.WithContext(ctx).Model(&model.AuditLog{}).
		Where("event_type = ? AND created_at >= ? AND created_at < ?", eventType, start, end)
	if result != "" {
		q = q.Where("login_result = ?", result)
	}
	err := q.Count(&count).Error
	return count, err
}

// Close 关闭底层 GORM/sql 连接池，释放日志库（回退 SQLite 时即 SQLite 连接）连接。
func (r *LogRepository) Close() error {
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
func (r *LogRepository) SQLDB() (*sql.DB, error) {
	if r.db == nil {
		return nil, fmt.Errorf("log repo: db not initialized")
	}
	return r.db.DB()
}
