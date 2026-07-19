package model

import "time"

// LoginRecord 结构化登录记录（需求 §6.9 / §10.8）。
// 与通用审计日志 AuditLog 区分：本表保存需求要求的 login_type / login_result / fail_reason
// 等结构化字段，专用于登录审计、风控与合规查询。默认存储于 SQLite（与 log 库同源）。
type LoginRecord struct {
	ID          int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID      string    `json:"user_id" gorm:"index;not null;type:varchar(191)"`
	LoginType   string    `json:"login_type" gorm:"index;not null;type:varchar(20)"` // password / google
	IPAddress   string    `json:"ip_address" gorm:"size:45"`
	DeviceInfo  string    `json:"device_info" gorm:"size:512"`
	LoginResult string    `json:"login_result" gorm:"index;not null;type:varchar(10)"` // success / fail
	FailReason  string    `json:"fail_reason" gorm:"size:255"`
	CreatedAt   time.Time `json:"created_at" gorm:"autoCreateTime;index"`
}

// LoginRecord 常量（与 02 §6.9 枚举一致）
const (
	LoginTypePassword = "password"
	LoginTypeGoogle   = "google"

	LoginResultSuccess = "success"
	LoginResultFail    = "fail"
)

func (LoginRecord) TableName() string {
	return "login_records"
}
