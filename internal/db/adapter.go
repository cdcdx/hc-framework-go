// Package db 多数据源（MySQL/Postgres/SQLite 等）适配与读写分离。
package db

import (
	"context"
	"database/sql"
)

// Adapter 数据库适配器统一接口
type Adapter interface {
	// Name 返回适配器名称
	Name() string
	// Connect 建立连接
	Connect(ctx context.Context) error
	// Close 关闭连接
	Close() error
	// Ping 健康检查
	Ping(ctx context.Context) error
	// DB 返回原生数据库连接（用于 GORM）
	DB() interface{}
	// DriverName 返回驱动名称
	DriverName() string
}

// RelationalDB 关系型数据库接口
type RelationalDB interface {
	Adapter
	// BeginTx 开启事务
	BeginTx(ctx context.Context) (interface{}, error)
	// Exec 执行 SQL
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	// QueryRow 查询单行
	QueryRow(ctx context.Context, query string, args ...interface{}) Row
	// Query 查询多行
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
}

// Row 行接口
type Row interface {
	Scan(dest ...interface{}) error
}

// Rows 多行接口
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
}

// ReadWriteSplitter 读写分离接口
type ReadWriteSplitter interface {
	// Master 返回主库
	Master() Adapter
	// Slave 返回从库
	Slave() Adapter
	// UseMaster 强制使用主库（事务中）
	UseMaster(ctx context.Context) context.Context
}
