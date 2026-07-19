package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouseAdapter ClickHouse 适配器
type ClickHouseAdapter struct {
	cfg    ClickHouseCfg
	conn   clickhouse.Conn
	dbName string
}

// ClickHouseCfg ClickHouse 连接配置
// DSN 格式: "clickhouse://user:pass@host:port/database?params..."
// 如果 DSN 未设置，则使用 Addresses/Database/Username/Password 分散字段（兼容旧配置）
type ClickHouseCfg struct {
	DSN       string   // 优先使用的 DSN URL
	Addresses []string // 兼容旧配置
	Database  string   // 兼容旧配置
	Username  string   // 兼容旧配置
	Password  string   // 兼容旧配置
}

// NewClickHouseAdapter 创建 CH 适配器
func NewClickHouseAdapter(cfg ClickHouseCfg) *ClickHouseAdapter {
	dbName := cfg.Database
	if dbName == "" && cfg.DSN != "" {
		dbName = extractCHDatabase(cfg.DSN)
	}
	return &ClickHouseAdapter{
		cfg:    cfg,
		dbName: dbName,
	}
}

// Name 返回适配器名称
func (a *ClickHouseAdapter) Name() string { return "clickhouse" }

// Connect 建立连接
func (a *ClickHouseAdapter) Connect(ctx context.Context) error {
	var opts *clickhouse.Options

	if a.cfg.DSN != "" {
		// 优先使用 DSN URL 格式
		o, err := clickhouse.ParseDSN(a.cfg.DSN)
		if err != nil {
			return fmt.Errorf("clickhouse parse dsn: %w", err)
		}
		opts = o
	} else {
		// 兼容旧的分散字段格式
		opts = &clickhouse.Options{
			Addr: a.cfg.Addresses,
			Auth: clickhouse.Auth{
				Database: a.cfg.Database,
				Username: a.cfg.Username,
				Password: a.cfg.Password,
			},
		}
	}

	opts.DialTimeout = 10 * time.Second
	opts.MaxOpenConns = 10
	opts.MaxIdleConns = 5
	opts.ConnMaxLifetime = time.Hour

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}

	a.conn = conn
	return nil
}

// extractCHDatabase 从 clickhouse:// DSN 中提取数据库名
func extractCHDatabase(dsn string) string {
	// clickhouse DSN 格式: clickhouse://user:pass@host:port/database?params
	o, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return ""
	}
	return o.Auth.Database
}

// Close 关闭连接
func (a *ClickHouseAdapter) Close() error {
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}

// Ping 健康检查
func (a *ClickHouseAdapter) Ping(ctx context.Context) error {
	if a.conn == nil {
		return fmt.Errorf("clickhouse not connected")
	}
	return a.conn.Ping(ctx)
}

// DB 返回原生连接
func (a *ClickHouseAdapter) DB() interface{} {
	return a.conn
}

// DriverName 返回驱动名称
func (a *ClickHouseAdapter) DriverName() string {
	return "clickhouse"
}

// Conn 返回 CH 连接
func (a *ClickHouseAdapter) Conn() clickhouse.Conn {
	return a.conn
}

// Database 返回数据库名
func (a *ClickHouseAdapter) Database() string {
	return a.dbName
}
