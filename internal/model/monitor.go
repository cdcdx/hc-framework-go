package model

import "time"

// MonitorMetric 监控指标（monitor.db）
type MonitorMetric struct {
	ID         int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID     string    `json:"user_id" gorm:"index;type:varchar(191)"`
	MetricType string    `json:"metric_type" gorm:"index;not null;type:varchar(50)"` // register / login / device_online / task_complete / shop_redeem
	MetricName string    `json:"metric_name" gorm:"not null;type:varchar(100)"`      // register_count / login_count 等
	Value      float64   `json:"value" gorm:"not null"`
	Tags       string    `json:"tags" gorm:"type:text"` // JSON 格式的附加标签
	CreatedAt  time.Time `json:"created_at" gorm:"autoCreateTime;index"`
}

func (MonitorMetric) TableName() string {
	return "monitor_metrics"
}
