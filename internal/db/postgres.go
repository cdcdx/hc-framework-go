package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// PostgreSQLAdapter PostgreSQL 数据库适配器
type PostgreSQLAdapter struct {
	mu   sync.RWMutex
	db   *gorm.DB
	dsn  string
	name string

	maxIdleConns     int
	maxOpenConns     int
	connMaxLifetime  time.Duration
	connMaxIdleTime  time.Duration
}

// PostgreSQLOption PostgreSQL 适配器选项
type PostgreSQLOption func(*PostgreSQLAdapter)

// WithPostgreSQLPool 设置连接池参数
func WithPostgreSQLPool(maxIdle, maxOpen int, lifetime time.Duration) PostgreSQLOption {
	return func(a *PostgreSQLAdapter) {
		a.maxIdleConns = maxIdle
		a.maxOpenConns = maxOpen
		a.connMaxLifetime = lifetime
		a.connMaxIdleTime = 5 * time.Minute // 默认：空闲 5 分钟后关闭
	}
}

// NewPostgreSQLAdapter 创建 PostgreSQL 适配器
func NewPostgreSQLAdapter(dsn string, opts ...PostgreSQLOption) *PostgreSQLAdapter {
	a := &PostgreSQLAdapter{
		dsn:             dsn,
		name:            "postgres",
		maxIdleConns:    10,
		maxOpenConns:    100,
		connMaxLifetime: time.Hour,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Name 返回适配器名称
func (a *PostgreSQLAdapter) Name() string { return a.name }

// Connect 建立连接
func (a *PostgreSQLAdapter) Connect(_ context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	db, err := gorm.Open(postgres.Open(a.dsn), &gorm.Config{
		Logger: newGormLogger(),
	})
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("postgres get db: %w", err)
	}
	sqlDB.SetMaxIdleConns(a.maxIdleConns)
	sqlDB.SetMaxOpenConns(a.maxOpenConns)
	sqlDB.SetConnMaxLifetime(a.connMaxLifetime)
	if a.connMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(a.connMaxIdleTime)
	}

	a.db = db
	return nil
}

// Close 关闭连接
func (a *PostgreSQLAdapter) Close() error {
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
func (a *PostgreSQLAdapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.db == nil {
		return fmt.Errorf("postgres not connected")
	}
	sqlDB, err := a.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// DB 返回 GORM DB 实例
func (a *PostgreSQLAdapter) DB() interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}

// DriverName 返回驱动名称
func (a *PostgreSQLAdapter) DriverName() string { return "postgres" }

// GORM 返回 *gorm.DB 实例（类型安全的访问方式）
func (a *PostgreSQLAdapter) GORM() *gorm.DB {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}
