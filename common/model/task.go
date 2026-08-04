package model

import "time"

// Task 任务表
type Task struct {
	ID           int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	TaskType     string    `json:"task_type" gorm:"not null;type:varchar(50)"` // daily / weekly / achievement
	TaskKey      string    `json:"task_key" gorm:"uniqueIndex;not null;type:varchar(191)"`
	TaskName     string    `json:"task_name" gorm:"not null;default:'';type:varchar(191)"`
	TargetValue  int       `json:"target_value" gorm:"default:1"`
	RewardPoints int64     `json:"reward_points" gorm:"default:0"`
	IsActive     bool      `json:"is_active" gorm:"default:true"`
	CreatedAt    time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt    time.Time `json:"updated_at" gorm:"autoUpdateTime"`
}

func (Task) TableName() string {
	return "tasks"
}

// 任务 Key 常量：SeedTasks 与事件驱动进度（ReportProgress）共用同一来源，
// 避免两处各自硬编码导致不一致。
const (
	TaskKeyDailyLogin       = "daily_login"
	TaskKeyDailyIdle30      = "daily_idle_30"
	TaskKeyDailyRedeem1     = "daily_redeem_1"
	TaskKeyWeeklyIdle300    = "weekly_idle_300"
	TaskKeyWeeklyRedeem3    = "weekly_redeem_3"
	TaskKeyAchievePoints10k = "achieve_points_10000"
	TaskKeyAchieveRedeem100 = "achieve_redeem_100"
)

// UserTaskProgress 用户任务进度表
type UserTaskProgress struct {
	ID              int64      `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID          string     `json:"user_id" gorm:"index:idx_user_task_period;not null;type:varchar(191)"`
	TaskID          int64      `json:"task_id" gorm:"index:idx_user_task_period;not null"`
	Period          string     `json:"period" gorm:"index:idx_user_task_period;default:'';type:varchar(50)"`
	CurrentProgress int        `json:"current_progress" gorm:"default:0"`
	IsCompleted     bool       `json:"is_completed" gorm:"default:false"`
	IsClaimed       bool       `json:"is_claimed" gorm:"default:false"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt       time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
}

func (UserTaskProgress) TableName() string {
	return "user_task_progress"
}
