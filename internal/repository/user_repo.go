package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/cache"
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
	db       *gorm.DB
	cacheMgr *cache.Manager
}

// NewUserRepository 创建用户仓库（GORM/SQLite 实现，不带缓存）
func NewUserRepository(db *gorm.DB) UserRepository {
	return &gormUserRepository{db: db}
}

// NewUserRepositoryWithCache 创建带三级缓存的用户仓库：
// FindByID/FindByEmail 走缓存（singleflight 防击穿），写操作失效对应缓存键。
// 仅 prod/正常 GORM 路径使用；sqliteFallback 与单测使用无缓存的 NewUserRepository。
func NewUserRepositoryWithCache(db *gorm.DB, cacheMgr *cache.Manager) UserRepository {
	return &gormUserRepository{db: db, cacheMgr: cacheMgr}
}

// 用户缓存短 TTL 即可：登录/资料读取收益最大，points 变更后经失效保持最终一致。
const userCacheTTL = 10 * time.Second

func userCacheKeyByID(uid string) string      { return "user:uid:" + uid }
func userCacheKeyByEmail(email string) string { return "user:email:" + email }

// invalidateUser 失效 uid（及可选 email）缓存键；cacheMgr 为 nil 时安全跳过。
func (r *gormUserRepository) invalidateUser(ctx context.Context, uid, email string) {
	if r.cacheMgr == nil {
		return
	}
	keys := make([]string, 0, 2)
	if uid != "" {
		keys = append(keys, userCacheKeyByID(uid))
	}
	if email != "" {
		keys = append(keys, userCacheKeyByEmail(email))
	}
	if len(keys) > 0 {
		_ = r.cacheMgr.Delete(ctx, keys...)
	}
}

// Create 创建用户
func (r *gormUserRepository) Create(ctx context.Context, user *model.User) error {
	if r.db == nil {
		return fmt.Errorf("user repo: db not initialized")
	}
	return r.db.WithContext(ctx).Create(user).Error
}

// FindByID 根据 UserID 查询（带三级缓存）
func (r *gormUserRepository) FindByID(ctx context.Context, userID string) (*model.User, error) {
	if r.db == nil {
		return nil, fmt.Errorf("user repo: db not initialized")
	}
	if r.cacheMgr != nil {
		val, err := r.cacheMgr.Get(ctx, userCacheKeyByID(userID), userCacheTTL, func(ctx context.Context) (interface{}, error) {
			var user model.User
			if err := r.db.WithContext(ctx).Where("user_id = ?", userID).First(&user).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil, nil
				}
				return nil, err
			}
			return &user, nil
		})
		if err == nil {
			return cache.DecodeCached[*model.User](val)
		}
		// 缓存层异常：降级为直查 DB
	}
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

// FindByEmail 根据邮箱查询（带三级缓存）
func (r *gormUserRepository) FindByEmail(ctx context.Context, email string) (*model.User, error) {
	if r.db == nil {
		return nil, fmt.Errorf("user repo: db not initialized")
	}
	if r.cacheMgr != nil {
		val, err := r.cacheMgr.Get(ctx, userCacheKeyByEmail(email), userCacheTTL, func(ctx context.Context) (interface{}, error) {
			var user model.User
			if err := r.db.WithContext(ctx).Where("email = ?", email).First(&user).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil, nil
				}
				return nil, err
			}
			return &user, nil
		})
		if err == nil {
			return cache.DecodeCached[*model.User](val)
		}
		// 缓存层异常：降级为直查 DB
	}
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
	if r.db == nil {
		return nil, fmt.Errorf("user repo: db not initialized")
	}
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

// Update 更新用户（写后失效缓存）
func (r *gormUserRepository) Update(ctx context.Context, user *model.User) error {
	if r.db == nil {
		return fmt.Errorf("user repo: db not initialized")
	}
	if err := r.db.WithContext(ctx).Save(user).Error; err != nil {
		return err
	}
	r.invalidateUser(ctx, user.UserID, user.Email)
	return nil
}

// UpdatePoints 原子更新积分余额（写后失效 uid 缓存）
func (r *gormUserRepository) UpdatePoints(ctx context.Context, userID string, amount int64) error {
	if r.db == nil {
		return fmt.Errorf("user repo: db not initialized")
	}
	result := r.db.WithContext(ctx).Model(&model.User{}).
		Where("user_id = ? AND points_balance >= ?", userID, -amount).
		Update("points_balance", gorm.Expr("points_balance + ?", amount))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("insufficient points or user not found")
	}
	// 失效 uid 缓存（points 变更）；email 键不含 points 且登录不依赖余额，无需失效。
	r.invalidateUser(ctx, userID, "")
	return nil
}

// UpdatePassword 更新密码哈希和修改时间（写后失效 uid+email 缓存，避免旧哈希短期可用）
func (r *gormUserRepository) UpdatePassword(ctx context.Context, userID, passwordHash string, changedAt interface{}) error {
	if r.db == nil {
		return fmt.Errorf("user repo: db not initialized")
	}
	if err := r.db.WithContext(ctx).Model(&model.User{}).
		Where("user_id = ?", userID).
		Updates(map[string]interface{}{
			"password_hash":       passwordHash,
			"password_changed_at": changedAt,
		}).Error; err != nil {
		return err
	}
	// 失效 uid + email 缓存；email 键需查出邮箱一并失效，避免旧密码哈希在 TTL 内仍可用于登录。
	email := ""
	if r.cacheMgr != nil {
		var u model.User
		if err := r.db.WithContext(ctx).Select("email").Where("user_id = ?", userID).First(&u).Error; err == nil {
			email = u.Email
		}
	}
	r.invalidateUser(ctx, userID, email)
	return nil
}

// AutoMigrate 自动迁移
func (r *gormUserRepository) AutoMigrate() error {
	if r.db == nil {
		return fmt.Errorf("user repo: db not initialized")
	}
	return r.db.AutoMigrate(&model.User{})
}

// Close 关闭底层 GORM/sql 连接池，释放 userDB 连接。
func (r *gormUserRepository) Close() error {
	if r.db == nil {
		return nil // 无连接可释放（与读/写路径 nil-db 守卫一致，避免 shutdown 时 panic）
	}
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// SQLDB 返回底层 *sql.DB，供监控采集连接池统计。
func (r *gormUserRepository) SQLDB() (*sql.DB, error) {
	if r.db == nil {
		return nil, fmt.Errorf("user repo: db not initialized")
	}
	return r.db.DB()
}
