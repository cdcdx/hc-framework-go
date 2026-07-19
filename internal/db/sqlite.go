package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// SQLiteAdapter SQLite 数据库适配器
type SQLiteAdapter struct {
	mu   sync.RWMutex
	db   *gorm.DB
	dsn  string
	name string
}

// NewSQLiteAdapter 创建 SQLite 适配器
func NewSQLiteAdapter(dsn string) *SQLiteAdapter {
	return &SQLiteAdapter{
		dsn:  dsn,
		name: "sqlite",
	}
}

// Name 返回适配器名称
func (a *SQLiteAdapter) Name() string { return a.name }

// Connect 建立连接
func (a *SQLiteAdapter) Connect(_ context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 确保数据目录存在
	dbPath := strings.TrimPrefix(a.dsn, "file:")
	if idx := strings.Index(dbPath, "?"); idx != -1 {
		dbPath = dbPath[:idx]
	}
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("sqlite create dir %s: %w", dir, err)
	}

	db, err := gorm.Open(sqlite.Open(a.dsn), &gorm.Config{
		Logger: newGormLogger(),
	})
	if err != nil {
		return fmt.Errorf("sqlite connect: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("sqlite get db: %w", err)
	}
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetMaxOpenConns(1)

	a.db = db
	return nil
}

// Close 关闭连接
func (a *SQLiteAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.db != nil {
		sqlDB, err := a.db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	}
	return nil
}

// Ping 健康检查
func (a *SQLiteAdapter) Ping(_ context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.db == nil {
		return fmt.Errorf("sqlite not connected")
	}
	sqlDB, err := a.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Ping()
}

// DB 返回 GORM DB 实例
func (a *SQLiteAdapter) DB() interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}

// DriverName 返回驱动名称
func (a *SQLiteAdapter) DriverName() string { return "sqlite3" }

// GORM 返回 *gorm.DB 实例（类型安全的访问方式）
func (a *SQLiteAdapter) GORM() *gorm.DB {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}
