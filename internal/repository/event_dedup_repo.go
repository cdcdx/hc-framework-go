// Package repository 数据访问层（DAO），封装数据库读写。
package repository

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// EventDedupRepository 事件去重仓库（幂等消费）。
type EventDedupRepository struct {
	rw *db.RWDB // 用于清理旧记录；Mark 走调用方传入的事务
}

// NewEventDedupRepository 创建事件去重仓库。
// rw 可为 nil（仅在事务内调用 Mark 时不需要）。
func NewEventDedupRepository(rw *db.RWDB) *EventDedupRepository {
	return &EventDedupRepository{rw: rw}
}

// AutoMigrate 自动迁移（DDL 操作主库），与 Idle/Shop/Task 仓储保持一致，
// 使本仓储自描述其表结构；启动时亦由 businessModels 统一 AutoMigrate 覆盖。
// rw 为 nil 时（仅用于事务内 Mark）跳过，避免空指针。
func (r *EventDedupRepository) AutoMigrate() error {
	if r.rw == nil {
		return nil
	}
	return r.rw.Master().AutoMigrate(&model.EventDedup{})
}

// Mark 标记事件已处理，保证幂等（应在业务事务 tx 内调用）。
//   - 首次处理：插入成功 → 返回 (true, nil)，调用方继续累加进度。
//   - 已处理（event_id 唯一冲突）：ON CONFLICT DO NOTHING → 返回 (false, nil)，调用方应跳过。
//
// 采用 upsert 的 DoNothing 语义而非依赖 gorm.ErrDuplicatedKey：GORM 对
// SQLite/PostgreSQL 生成 `ON CONFLICT DO NOTHING`、对 MySQL 生成 `INSERT IGNORE`，
// 冲突时 RowsAffected=0。这样无需开启全局 TranslateError，三种数据库行为一致。
func (r *EventDedupRepository) Mark(tx *gorm.DB, eventID, source string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	rec := &model.EventDedup{EventID: eventID, Source: source}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(rec)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// DeleteBefore 删除 before 之前创建的去重记录（定时清理，防止表无限增长，写主库）。
// 幂等事件的 event_id 派生自业务主键，相同事件在 retention 窗口后已不可能重放，
// 因此按 created_at 过期清理是安全的。返回被删除的行数。
func (r *EventDedupRepository) DeleteBefore(ctx context.Context, before time.Time) (int64, error) {
	if r.rw == nil {
		return 0, nil // rw 可 nil（仅事务内 Mark）：无连接则无记录可删，视为已清理完成
	}
	result := r.rw.Write(ctx).
		Where("created_at < ?", before).
		Delete(&model.EventDedup{})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}
