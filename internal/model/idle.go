package model

import "time"

// IdleRecord 挂机记录表
type IdleRecord struct {
	ID              int64      `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID          string     `json:"user_id" gorm:"index:idx_user_status;not null;type:varchar(191)"`
	DeviceID        string     `json:"device_id" gorm:"index:idx_user_device_status,priority:2;not null;default:'';type:varchar(191)"`
	StartTime       time.Time  `json:"start_time" gorm:"not null"`
	EndTime         *time.Time `json:"end_time,omitempty"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty" gorm:"index:idx_status_heartbeat,priority:2"`
	DurationSeconds int        `json:"duration_seconds" gorm:"default:0"`
	PointsEarned    int64      `json:"points_earned" gorm:"default:0"`
	Status          string     `json:"status" gorm:"index:idx_user_status,priority:2;index:idx_user_device_status,priority:3;index:idx_status_heartbeat,priority:1;default:active;type:varchar(20)"` // active / completed / timeout
	CreatedAt       time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt       time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
}

// TableName 表名
func (IdleRecord) TableName() string {
	return "idle_records"
}

// IdleDailyPoints 每日挂机积分汇总表（根治 getDailyPointsDB 的全表 SUM 慢查询）。
// 每次结算在业务事务内增量累加 (user_id, day) 的 total；GetDailyPoints 的 DB 回源改为读单行 O(1)，
// 不再对 idle_records 做 SUM(points_earned) WHERE created_at >= today 的范围扫描。
// day 为本地日期串 "2006-01-02"，与 Redis 每日计数器边界（endOfLocalDay）一致。
type IdleDailyPoints struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID    string    `json:"user_id" gorm:"uniqueIndex:idx_idle_daily_user_day;not null;type:varchar(191)"`
	Day       string    `json:"day" gorm:"uniqueIndex:idx_idle_daily_user_day;not null;type:varchar(20)"`
	Total     int64     `json:"total" gorm:"not null;default:0"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime"`
}

// TableName 表名
func (IdleDailyPoints) TableName() string {
	return "idle_daily_points"
}
