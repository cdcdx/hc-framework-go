package model

import "time"

// User 用户表（MongoDB / SQLite）
type User struct {
	UserID            string     `json:"user_id" bson:"user_id" gorm:"primaryKey;uniqueIndex;type:varchar(191)"`
	Username          string     `json:"username" bson:"username" gorm:"not null;type:varchar(100)"`
	Email             string     `json:"email" bson:"email" gorm:"uniqueIndex;not null;type:varchar(200)"`
	PasswordHash      string     `json:"-" bson:"password_hash" gorm:"column:password_hash;type:varchar(255)"`
	GoogleID          *string    `json:"google_id,omitempty" bson:"google_id,omitempty" gorm:"uniqueIndex;type:varchar(191)"`
	AvatarURL         string     `json:"avatar_url" bson:"avatar_url;type:text"`
	PointsBalance     int64      `json:"points_balance" bson:"points_balance" gorm:"default:0;check:points_balance >= 0"`
	Status            string     `json:"status" bson:"status" gorm:"default:active;type:varchar(20)"` // active / banned / deleted
	PasswordChangedAt *time.Time `json:"password_changed_at,omitempty" bson:"password_changed_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at" bson:"created_at" gorm:"autoCreateTime"`
	UpdatedAt         time.Time  `json:"updated_at" bson:"updated_at" gorm:"autoUpdateTime"`
}

// TableName GORM 表名
func (User) TableName() string {
	return "users"
}
