package model

import "time"

// AuditLog 审计日志（log.db）
// 登录审计（event_type=login）的登录专属字段（login_type / login_result / fail_reason /
// device_info）直接内嵌于本表，不再单独建 login_records 表，使登录写入从 2 次降到 1 次。
type AuditLog struct {
	ID          int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID      string    `json:"user_id" gorm:"index;not null;type:varchar(191)"`
	EventType   string    `json:"event_type" gorm:"index;not null;type:varchar(50)"` // register / login / device_online / task_complete / shop_redeem
	LoginType   string    `json:"login_type" gorm:"index;not null;default:'';type:varchar(20)"` // password / google，仅 login 事件有值
	LoginResult string    `json:"login_result" gorm:"index;not null;default:'';type:varchar(10)"` // success / fail，仅 login 事件有值
	FailReason  string    `json:"fail_reason" gorm:"type:varchar(255)"`                       // 失败原因（如 user_not_found / password_wrong），仅 fail 有值
	DeviceInfo  string    `json:"device_info" gorm:"type:varchar(512)"`                       // 设备信息
	Detail      string    `json:"detail" gorm:"type:text"`                                  // JSON 格式的事件详情
	IPAddress   string    `json:"ip_address" gorm:"size:45"`
	UserAgent   string    `json:"user_agent" gorm:"size:512"`
	CreatedAt   time.Time `json:"created_at" gorm:"autoCreateTime;index"`
}

func (AuditLog) TableName() string {
	return "audit_logs"
}

// 登录类型与结果常量（合并 login_records 后统一放此处，便于 auth / common 包引用）。
const (
	LoginTypePassword  = "password"
	LoginTypeGoogle    = "google"
	LoginResultSuccess = "success"
	LoginResultFail    = "fail"
)

// 登录失败原因常量（仅 login_result=fail 时有值，统一枚举便于 auth / common 包引用与看板过滤）。
const (
	FailReasonUserNotFound   = "user_not_found"   // 邮箱对应的用户不存在
	FailReasonAccountDisabled = "account_disabled" // 账号被管理员禁用（status!=active）
	FailReasonAccountLocked   = "account_locked"   // 连续失败被锁定（security.account_lock）
	FailReasonPasswordWrong   = "password_wrong"   // 密码错误
	FailReasonOAuthFailed     = "oauth_failed"     // Google OAuth 换取用户信息失败（Sub 为空）
)
