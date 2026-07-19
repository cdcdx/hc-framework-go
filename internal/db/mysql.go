package db

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// MySQLAdapter MySQL 数据库适配器
type MySQLAdapter struct {
	mu   sync.RWMutex
	db   *gorm.DB
	dsn  string
	name string

	maxIdleConns     int
	maxOpenConns     int
	connMaxLifetime  time.Duration
	connMaxIdleTime  time.Duration
}

// MySQLOption MySQL 适配器选项
type MySQLOption func(*MySQLAdapter)

// PoolConfig 统一连接池参数（MySQL / PostgreSQL 适配器共用）。
// 用于把配置中的连接池设置以「值」形式透传给 rwOpenGORM，避免 mysql/postgres
// 两种不同选项类型在同一个变参签名里冲突。
type PoolConfig struct {
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
}

// WithMySQLPool 设置连接池参数
func WithMySQLPool(maxIdle, maxOpen int, lifetime time.Duration) MySQLOption {
	return func(a *MySQLAdapter) {
		a.maxIdleConns = maxIdle
		a.maxOpenConns = maxOpen
		a.connMaxLifetime = lifetime
		a.connMaxIdleTime = 5 * time.Minute // 默认：空闲 5 分钟后关闭，避免无限堆积
	}
}

// NewMySQLAdapter 创建 MySQL 适配器
// dsn 支持两种格式：
//   - 标准格式: "user:pass@tcp(host:port)/db?params"
//   - URL 格式:  "mysql://user:pass@host:port/db?params"（会自动转为标准格式）
func NewMySQLAdapter(dsn string, opts ...MySQLOption) *MySQLAdapter {
	a := &MySQLAdapter{
		dsn:             normalizeMySQLDSN(dsn),
		name:            "mysql",
		maxIdleConns:    10,
		maxOpenConns:    100,
		connMaxLifetime: time.Hour,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// normalizeMySQLDSN 将 mysql:// URL 格式 DSN 转换为 go-sql-driver/mysql 标准格式
//   - "mysql://user:pass@host:port/db?params" → "user:pass@tcp(host:port)/db?params"
//   - 非 mysql:// 前缀的 DSN 原样返回（兼容旧格式）
func normalizeMySQLDSN(dsn string) string {
	if !strings.HasPrefix(dsn, "mysql://") {
		return dsn
	}

	u, err := url.Parse(dsn)
	if err != nil {
		// 解析失败，回退到原始值
		return dsn
	}

	password, _ := u.User.Password()
	dbName := strings.TrimPrefix(u.Path, "/")

	standard := fmt.Sprintf("%s:%s@tcp(%s)/%s", u.User.Username(), password, u.Host, dbName)
	if u.RawQuery != "" {
		standard += "?" + u.RawQuery
	}

	return standard
}

// Name 返回适配器名称
func (a *MySQLAdapter) Name() string { return a.name }

// Connect 建立连接
func (a *MySQLAdapter) Connect(_ context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	db, err := gorm.Open(mysql.Open(a.dsn), &gorm.Config{
		Logger: newGormLogger(),
	})
	if err != nil {
		return fmt.Errorf("mysql connect: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("mysql get db: %w", err)
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
func (a *MySQLAdapter) Close() error {
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
func (a *MySQLAdapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.db == nil {
		return fmt.Errorf("mysql not connected")
	}
	sqlDB, err := a.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// DB 返回 GORM DB 实例
func (a *MySQLAdapter) DB() interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}

// DriverName 返回驱动名称
func (a *MySQLAdapter) DriverName() string { return "mysql" }

// GORM 返回 *gorm.DB 实例（类型安全的访问方式）
func (a *MySQLAdapter) GORM() *gorm.DB {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}
