package repository

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// PointsOutboxRepository 积分 outbox 表的读写。
// 通过行级状态条件更新（UPDATE ... WHERE status='pending'）实现多竞争者下的恰好一次认领。
type PointsOutboxRepository struct {
	db *gorm.DB
}

func NewPointsOutboxRepository(db *gorm.DB) *PointsOutboxRepository {
	return &PointsOutboxRepository{db: db}
}

// AutoMigrate 自动迁移（DDL 操作主库），与 Idle/Shop/Task 仓储保持一致，
// 使本仓储自描述其表结构；启动时亦由 businessModels 统一 AutoMigrate 覆盖。
func (r *PointsOutboxRepository) AutoMigrate() error {
	return r.db.AutoMigrate(&model.PointsOutbox{})
}

// AppendInTx 在业务事务 tx 内追加一条 outbox 记录，与库存/订单/积分流水同提交。
// tx 已携带调用方 ctx，直接使用以保留链路追踪与超时信息。
func (r *PointsOutboxRepository) AppendInTx(tx *gorm.DB, rec *model.PointsOutbox) error {
	return tx.Create(rec).Error
}

// Claim 将 pending 记录原子置为 processing，仅一个竞争者成功（行级状态条件更新）。
// 返回 claimed=true 表示本次负责应用该条积分调整。
func (r *PointsOutboxRepository) Claim(ctx context.Context, eventID string) (bool, error) {
	res := r.db.WithContext(ctx).Model(&model.PointsOutbox{}).
		Where("event_id = ? AND status = ?", eventID, model.OutboxStatusPending).
		Updates(map[string]interface{}{
			"status":     model.OutboxStatusProcessing,
			"updated_at": time.Now(),
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, nil
}

// MarkDone 标记已成功应用到 userDB，认领者才能置为 done。
func (r *PointsOutboxRepository) MarkDone(ctx context.Context, eventID string) error {
	return r.db.WithContext(ctx).Model(&model.PointsOutbox{}).
		Where("event_id = ? AND status = ?", eventID, model.OutboxStatusProcessing).
		Updates(map[string]interface{}{
			"status":     model.OutboxStatusDone,
			"updated_at": time.Now(),
		}).Error
}

// Requeue 应用失败后释放回 pending，交由 relay / 消费者重试（仍限制在 processing 态才能回退，
// 避免覆盖已被其他竞争者认领的记录）。
func (r *PointsOutboxRepository) Requeue(ctx context.Context, eventID string) error {
	return r.db.WithContext(ctx).Model(&model.PointsOutbox{}).
		Where("event_id = ? AND status = ?", eventID, model.OutboxStatusProcessing).
		Updates(map[string]interface{}{
			"status":     model.OutboxStatusPending,
			"updated_at": time.Now(),
		}).Error
}

// PendingOlderThan 返回 pending 且创建时间早于 before 的记录（供 relay 重试，grace 避免与
// 正在进行的同步快路径争用刚写入的记录）。
func (r *PointsOutboxRepository) PendingOlderThan(ctx context.Context, before time.Time, limit int) ([]model.PointsOutbox, error) {
	var rows []model.PointsOutbox
	if limit <= 0 {
		limit = 200
	}
	err := r.db.WithContext(ctx).
		Where("status = ? AND created_at < ?", model.OutboxStatusPending, before).
		Order("id ASC").Limit(limit).Find(&rows).Error
	return rows, err
}
