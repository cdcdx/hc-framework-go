package repository

import (
	"context"
	"database/sql"
	"fmt"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// LoginRepo 结构化登录记录仓库接口（GORM / SQLite）。
// 与 AuditLog 的 LogRepo 解耦：登录记录需可靠保存结构化字段（失败原因等），
// 始终落 SQLite，不依赖 ES/CH 等异构日志后端。
type LoginRepo interface {
	Create(ctx context.Context, rec *model.LoginRecord) error
	CreateBatch(ctx context.Context, recs []*model.LoginRecord) error
	// SQLDB 返回底层 *sql.DB，供监控采集连接池统计（metrics.RegisterDBPool）。
	SQLDB() (*sql.DB, error)
	// Close 释放底层 SQLite 连接。优雅关闭时调用。
	Close() error
}

// LoginRepository GORM 实现
type LoginRepository struct {
	db *gorm.DB
}

// NewLoginRepository 创建登录记录仓库
func NewLoginRepository(db *gorm.DB) *LoginRepository {
	return &LoginRepository{db: db}
}

// Create 写入一条登录记录
func (r *LoginRepository) Create(ctx context.Context, rec *model.LoginRecord) error {
	if r.db == nil {
		return fmt.Errorf("login repo: db not initialized")
	}
	if rec == nil {
		return fmt.Errorf("login repo: nil LoginRecord passed to Create")
	}
	return r.db.WithContext(ctx).Create(rec).Error
}

// CreateBatch 批量写入登录记录（单条 SQL 多值插入）
func (r *LoginRepository) CreateBatch(ctx context.Context, recs []*model.LoginRecord) error {
	if r.db == nil {
		return fmt.Errorf("login repo: db not initialized")
	}
	if len(recs) == 0 {
		return nil
	}
	// 过滤 nil 元素，避免 GORM CreateInBatches 对 nil 元素 panic（13 §3.40 ①）
	clean := make([]*model.LoginRecord, 0, len(recs))
	for _, rec := range recs {
		if rec != nil {
			clean = append(clean, rec)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).CreateInBatches(clean, 200).Error
}

// Close 关闭底层 SQLite 连接池，释放登录记录库连接。
func (r *LoginRepository) Close() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// SQLDB 返回底层 *sql.DB，供监控采集连接池统计。
func (r *LoginRepository) SQLDB() (*sql.DB, error) {
	return r.db.DB()
}
