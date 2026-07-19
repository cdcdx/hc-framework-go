package model

import "time"

// EventDedup 事件去重表（幂等消费）。
// 消费端处理事件前先 INSERT event_id（唯一索引），唯一冲突即代表已处理过，
// 从而在 Kafka at-least-once 语义下实现 effectively-once（同事件不重复累加进度）。
type EventDedup struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	EventID   string    `json:"event_id" gorm:"uniqueIndex;not null;type:varchar(128)"`
	Source    string    `json:"source" gorm:"not null;type:varchar(64)"` // 事件类型，便于排查
	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime"`
}

// TableName 指定表名
func (EventDedup) TableName() string {
	return "event_dedup"
}
