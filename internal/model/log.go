package model

import "time"

// AuditLog 审计日志（log.db）
type AuditLog struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID    string    `json:"user_id" gorm:"index;not null;type:varchar(191)"`
	EventType string    `json:"event_type" gorm:"index;not null;type:varchar(50)"` // register / login / device_online / task_complete / shop_redeem
	Detail    string    `json:"detail" gorm:"type:text"`                           // JSON 格式的事件详情
	IPAddress string    `json:"ip_address" gorm:"size:45"`
	UserAgent string    `json:"user_agent" gorm:"size:512"`
	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime;index"`
}

func (AuditLog) TableName() string {
	return "audit_logs"
}
