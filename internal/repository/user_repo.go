package repository

import (
	"context"
	"database/sql"
	"fmt"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// UserRepository 用户数据仓库接口（GORM / MongoDB 共用）
type UserRepository interface {
	Create(ctx context.Context, user *model.User) error
	FindByID(ctx context.Context, userID string) (*model.User, error)
	FindByEmail(ctx context.Context, email string) (*model.User, error)
	FindByGoogleID(ctx context.Context, googleID string) (*model.User, error)
	Update(ctx context.Context, user *model.User) error
	UpdatePoints(ctx context.Context, userID string, amount int64) error
	UpdatePassword(ctx context.Context, userID, passwordHash string, changedAt interface{}) error
	AutoMigrate() error
	// SQLDB 返回底层 *sql.DB（仅 GORM/SQLite 实现可用；MongoDB 实现返回 error）。
	// 用于监控采集连接池统计（metrics.RegisterDBPool）。
	SQLDB() (*sql.DB, error)
	// Close 释放底层连接（GORM 关闭 sql.DB；MongoDB 断开 client）。优雅关闭时调用。
	Close() error
}

// ============================================
// GORM 实现
// ============================================

// gormUserRepository GORM 用户数据仓库
type gormUserRepository struct {
	db *gorm.DB
}

// NewUserRepository 创建用户仓库（GORM/SQLite 实现）
func NewUserRepository(db *gorm.DB) UserRepository {
	return &gormUserRepository{db: db}
}

// Create 创建用户
func (r *gormUserRepository) Create(ctx context.Context, user *model.User) error {
	return r.db.WithContext(ctx).Create(user).Error
}

// FindByID 根据 UserID 查询
func (r *gormUserRepository) FindByID(ctx context.Context, userID string) (*model.User, error) {
	var user model.User
	err := r.db.WithContext(ctx).Where("user_id = ?", userID).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// FindByEmail 根据邮箱查询
func (r *gormUserRepository) FindByEmail(ctx context.Context, email string) (*model.User, error) {
	var user model.User
	err := r.db.WithContext(ctx).Where("email = ?", email).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// FindByGoogleID 根据 Google ID 查询
func (r *gormUserRepository) FindByGoogleID(ctx context.Context, googleID string) (*model.User, error) {
	var user model.User
	err := r.db.WithContext(ctx).Where("google_id = ?", googleID).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// Update 更新用户
func (r *gormUserRepository) Update(ctx context.Context, user *model.User) error {
	return r.db.WithContext(ctx).Save(user).Error
}

// UpdatePoints 原子更新积分余额
func (r *gormUserRepository) UpdatePoints(ctx context.Context, userID string, amount int64) error {
	result := r.db.WithContext(ctx).Model(&model.User{}).
		Where("user_id = ? AND points_balance >= ?", userID, -amount).
		Update("points_balance", gorm.Expr("points_balance + ?", amount))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("insufficient points or user not found")
	}
	return nil
}

// UpdatePassword 更新密码哈希和修改时间
func (r *gormUserRepository) UpdatePassword(ctx context.Context, userID, passwordHash string, changedAt interface{}) error {
	return r.db.WithContext(ctx).Model(&model.User{}).
		Where("user_id = ?", userID).
		Updates(map[string]interface{}{
			"password_hash":       passwordHash,
			"password_changed_at": changedAt,
		}).Error
}

// AutoMigrate 自动迁移
func (r *gormUserRepository) AutoMigrate() error {
	return r.db.AutoMigrate(&model.User{})
}

// Close 关闭底层 GORM/sql 连接池，释放 userDB 连接。
func (r *gormUserRepository) Close() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// SQLDB 返回底层 *sql.DB，供监控采集连接池统计。
func (r *gormUserRepository) SQLDB() (*sql.DB, error) {
	return r.db.DB()
}
