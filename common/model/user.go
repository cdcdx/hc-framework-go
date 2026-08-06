// Package model 领域模型与数据库表结构定义（go-zero 版，与 gin 版共用同一表结构）。
package model

import "time"

// User 用户表（MySQL / PostgreSQL / SQLite）
type User struct {
	UserID            string     `json:"user_id" gorm:"primaryKey;uniqueIndex;type:varchar(191)"`
	Username          string     `json:"username" gorm:"not null;type:varchar(100)"`
	Email             string     `json:"email" gorm:"uniqueIndex;not null;type:varchar(200)"`
	PasswordHash      string     `json:"-" gorm:"column:password_hash;type:varchar(255)"`
	GoogleID          *string    `json:"google_id,omitempty" gorm:"uniqueIndex;type:varchar(191)"`
	AvatarURL         string     `json:"avatar_url" gorm:"type:text"`
	PointsBalance     int64      `json:"points_balance" gorm:"default:1000;check:points_balance >= 0"`
	Status            string     `json:"status" gorm:"default:active;type:varchar(20)"` // active / banned / deleted
	PasswordChangedAt *time.Time `json:"password_changed_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt         time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
}

// TableName GORM 表名
func (User) TableName() string {
	return "users"
}
