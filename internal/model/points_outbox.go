package model

import "time"

// PointsOutbox 积分调整的可靠投递表（Transactional Outbox）。
//
// 业务事务（businessDB）提交时一并写入 pending 记录，与库存/订单/积分流水原子提交；
// 随后由「同步快路径」「Kafka 消费者」「后台 relay」三者竞争 Claim（UPDATE ... WHERE
// status='pending'）后原子更新 userDB 余额，保证 businessDB 与 userDB 最终一致。
//
// 仅当 businessDB 事务成功提交才会产生 outbox 记录；若 userDB（MongoDB/SQLite）更新失败，
// 记录保持 pending，由 relay / 消费者重试，从而避免「库存已扣、积分未加」的不一致。
//
// 索引：idx_status_created (status, created_at) 覆盖 relay 的 PendingOlderThan 扫描
// （WHERE status='pending' AND created_at < ?），避免全表扫描。
type PointsOutbox struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID    string    `json:"user_id" gorm:"column:user_id;index;not null;type:varchar(64)"`
	Delta     int64     `json:"delta" gorm:"column:delta;not null;default:0"`                         // 正数=收入，负数=支出
	RefType   string    `json:"ref_type" gorm:"column:ref_type;not null;default:'';type:varchar(32)"` // redeem / idle / task
	RefID     string    `json:"ref_id" gorm:"column:ref_id;not null;default:'';type:varchar(128)"`
	EventID   string    `json:"event_id" gorm:"column:event_id;uniqueIndex;not null;type:varchar(128)"` // 幂等键
	Status    string    `json:"status" gorm:"column:status;index:idx_status_created,priority:1;not null;default:'pending';type:varchar(16)"`
	CreatedAt time.Time `json:"created_at" gorm:"column:created_at;index:idx_status_created,priority:2;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"column:updated_at;autoUpdateTime"`
}

func (PointsOutbox) TableName() string { return "points_outbox" }

// 积分 outbox 状态机：pending → processing(被某一竞争者认领) → done；
// 处理失败回退 processing → pending 等待重试。
const (
	OutboxStatusPending    = "pending"
	OutboxStatusProcessing = "processing"
	OutboxStatusDone       = "done"
)
