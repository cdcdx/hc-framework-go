// Package event 统一事件模型与事件类型定义。
package event

import (
	"time"
)

// 事件类型常量
const (
	EventUserRegistered   = "user.registered"
	EventUserLoggedIn     = "user.logged_in"
	EventIdleSettled      = "idle.settled"
	EventTaskCompleted    = "task.completed"
	EventShopRedeemed     = "shop.redeemed"
	EventUserPointsAdjust = "user.points.adjust" // 积分可靠投递（Outbox）事件
	EventCacheInvalidate  = "cache.invalidate"   // 写降级缓存失效/刷新事件（由消费者异步刷新 L2）
)

// Topic 常量
const (
	TopicUserEvents = "user-events"
	TopicIdleEvents = "idle-events"
	TopicTaskEvents = "task-events"
	TopicShopEvents = "shop-events"
)

// Message 统一消息结构
type Message struct {
	TraceID   string            `json:"trace_id"`
	Timestamp time.Time         `json:"timestamp"`
	EventType string            `json:"event_type"`
	Key       string            `json:"key"`
	EventID   string            `json:"event_id,omitempty"` // 幂等键：建议由生产者设为稳定唯一值（如业务主键派生），供消费端去重
	Payload   interface{}       `json:"payload"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// UserRegisteredPayload 用户注册事件
type UserRegisteredPayload struct {
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	LoginType string `json:"login_type"` // password / google
}

// UserLoggedInPayload 用户登录事件
type UserLoggedInPayload struct {
	UserID    string `json:"user_id"`
	LoginType string `json:"login_type"`
	IPAddress string `json:"ip_address"`
	Device    string `json:"device"`
}

// IdleSettledPayload 挂机结算事件
type IdleSettledPayload struct {
	UserID          string `json:"user_id"`
	IdleRecordID    int64  `json:"idle_record_id"`
	DurationSeconds int    `json:"duration_seconds"`
	PointsEarned    int64  `json:"points_earned"`
}

// TaskCompletedPayload 任务完成事件
type TaskCompletedPayload struct {
	UserID       string `json:"user_id"`
	TaskID       int64  `json:"task_id"`
	TaskType     string `json:"task_type"`
	TaskKey      string `json:"task_key"`
	RewardPoints int64  `json:"reward_points"`
}

// ShopRedeemedPayload 商品兑换事件
type ShopRedeemedPayload struct {
	UserID      string `json:"user_id"`
	OrderID     int64  `json:"order_id"`
	ItemID      int64  `json:"item_id"`
	ItemName    string `json:"item_name"`
	PointsSpent int64  `json:"points_spent"`
}

// UserPointsAdjustPayload 积分调整事件（Outbox 投递）
// EventID 与 points_outbox.event_id 一致，是消费端幂等去重的依据。
type UserPointsAdjustPayload struct {
	UserID  string `json:"user_id"`
	EventID string `json:"event_id"`
	Delta   int64  `json:"delta"`
	RefType string `json:"ref_type"` // redeem / idle / task
	RefID   string `json:"ref_id"`
}
