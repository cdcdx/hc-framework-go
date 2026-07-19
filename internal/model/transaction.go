package model

import "time"

// PointsTransaction 积分流水表
// idx_pts_user_created (user_id, created_at) 覆盖 getDailyPointsDB 的
// WHERE user_id = ? AND created_at >= ? 查询。
type PointsTransaction struct {
	ID           int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID       string    `json:"user_id" gorm:"index:idx_pts_user_created,priority:1;not null;type:varchar(191)"`
	ChangeAmount int64     `json:"change_amount" gorm:"not null;default:0"` // 正数=收入，负数=支出
	BalanceAfter int64     `json:"balance_after" gorm:"not null;default:0"`
	ChangeType   string    `json:"change_type" gorm:"index;not null;type:varchar(50)"` // idle_reward / task_reward / redeem_spend / admin_adjust
	ReferenceID  string    `json:"reference_id" gorm:"not null;default:'';type:varchar(191)"`
	CreatedAt    time.Time `json:"created_at" gorm:"index:idx_pts_user_created,priority:2;autoCreateTime"`
}

func (PointsTransaction) TableName() string {
	return "points_transactions"
}

// 积分变动类型常量
const (
	ChangeTypeIdleReward  = "idle_reward"
	ChangeTypeTaskReward  = "task_reward"
	ChangeTypeRedeemSpend = "redeem_spend"
	ChangeTypeAdminAdjust = "admin_adjust"
)
