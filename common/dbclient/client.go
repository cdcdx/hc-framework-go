// Package dbclient 统一数据库客户端抽象。
// 支持 SQL（gorm）和 NoSQL（mongodb/clickhouse/elasticsearch）的统一生命周期管理。
//
// 使用方式:
//
//	factory := dbclient.NewFactory(cfg.Databases.ToSpecs())
//	sqlDB := factory.SQL("business")           // *gorm.DB
//	noSQL := factory.NoSQL("log")               // NoSQLClient (mongodb/clickhouse/es)
//	mongo := factory.MongoDB("user")            // *mongo.Client (快捷方法)
//	es := factory.Elasticsearch("log")          // *elasticsearch.Client (快捷方法)
//	factory.Close()                              // 关闭所有连接
//
// 分层约定: 本包属于 common 层，只依赖 common/gormx 与自有的 DBSpec 契约，
// 不 import 任何 app 层包。上层配置通过 ToSpecs() 适配为 Specs 后传入。
package dbclient

import (
	"fmt"
	"sync"

	"github.com/cdcdx/hc-framework-go/common/gormx"
	"gorm.io/gorm"
)

// Factory 数据库客户端工厂。根据配置中的 Driver 字段自动创建对应类型的客户端。
// SQL 类（sqlite/mysql/postgres）走 gorm 连接；NoSQL 类按需延迟创建。
type Factory struct {
	mu      sync.Mutex
	configs Specs

	// SQL 数据库实例
	sqlDBs map[string]*gorm.DB

	// NoSQL 实例（按需延迟创建）
	noSQLClients map[string]NoSQLClient
}

// NoSQLClient NoSQL 数据库客户端统一接口。
// 实现者: mongodb.Client / clickhouse.Conn / elasticsearch.Client。
type NoSQLClient interface {
	// Driver 返回驱动类型（mongodb/clickhouse/elasticsearch）。
	Driver() string
	// Ping 健康检查。
	Ping() error
	// Close 关闭连接。
	Close() error
}

// NewFactory 根据多库连接规格创建客户端工厂。
// 只初始化 SQL 类数据库（sqlite/mysql/postgres），NoSQL 类按需延迟创建。
func NewFactory(cfgs Specs) *Factory {
	f := &Factory{
		configs:      cfgs,
		sqlDBs:       make(map[string]*gorm.DB),
		noSQLClients: make(map[string]NoSQLClient),
	}
	return f
}

// SQL 获取指定名称的 SQL 数据库客户端（*gorm.DB）。
// 首次调用时按配置自动创建连接；后续调用返回缓存实例。
func (f *Factory) SQL(name string) (*gorm.DB, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sqlLocked(name)
}

func (f *Factory) sqlLocked(name string) (*gorm.DB, error) {
	if db, ok := f.sqlDBs[name]; ok {
		return db, nil
	}

	cfg, ok := f.configs[name]
	if !ok {
		return nil, fmt.Errorf("db %s not configured", name)
	}
	if !cfg.IsSQL() {
		return nil, fmt.Errorf("db %s is not a SQL database (driver=%s)", name, cfg.Driver)
	}
	if cfg.DSN == "" {
		return nil, fmt.Errorf("db %s: empty DSN", name)
	}

	label := cfg.PoolLabel
	if label == "" {
		label = name
	}

	db, err := gormx.OpenWithPool(cfg.Driver, cfg.DSN, gormx.PoolConfig{
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		Label:           label,
	})
	if err != nil {
		return nil, fmt.Errorf("open db %s: %w", name, err)
	}

	f.sqlDBs[name] = db
	return db, nil
}

// NoSQL 获取 NoSQL 数据库客户端（按 Driver 自动选择适配器）。
// 当前所有 NoSQL 驱动返回 "not yet implemented"，待后续补充 mongodb/clickhouse/es 适配器。
func (f *Factory) NoSQL(name string) (NoSQLClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if c, ok := f.noSQLClients[name]; ok {
		return c, nil
	}

	cfg, ok := f.configs[name]
	if !ok {
		return nil, fmt.Errorf("db %s not configured", name)
	}
	if cfg.IsSQL() {
		return nil, fmt.Errorf("db %s is a SQL database (driver=%s), use SQL() instead", name, cfg.Driver)
	}

	driver := cfg.Driver

	// NoSQL 适配器尚未实现，统一返回未实现错误。
	// 各分支预留接入点：mongodb/clickhouse/elasticsearch。
	var err error
	switch driver {
	case "mongodb":
		err = fmt.Errorf("mongodb adapter not yet implemented")
	case "clickhouse":
		err = fmt.Errorf("clickhouse adapter not yet implemented")
	case "elasticsearch":
		err = fmt.Errorf("elasticsearch adapter not yet implemented")
	default:
		err = fmt.Errorf("unsupported NoSQL driver: %s", driver)
	}
	return nil, err
}

// MongoDB 获取 MongoDB 客户端（快捷方法）。
func (f *Factory) MongoDB(name string) (NoSQLClient, error) {
	return f.NoSQL(name)
}

// Elasticsearch 获取 Elasticsearch 客户端（快捷方法）。
func (f *Factory) Elasticsearch(name string) (NoSQLClient, error) {
	return f.NoSQL(name)
}

// Clickhouse 获取 ClickHouse 客户端（快捷方法）。
func (f *Factory) Clickhouse(name string) (NoSQLClient, error) {
	return f.NoSQL(name)
}

// SQLOrDefault 同 SQL，但 name 不存在时返回 defaultDB。
func (f *Factory) SQLOrDefault(name string, defaultDB *gorm.DB) *gorm.DB {
	db, err := f.SQL(name)
	if err != nil {
		return defaultDB
	}
	return db
}

// ListSQL 返回所有已创建的 SQL 库名列表。
func (f *Factory) ListSQL() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.sqlDBs))
	for n := range f.sqlDBs {
		names = append(names, n)
	}
	return names
}

// Health 对所有已创建的 SQL 库执行 Ping 健康检查。
// 返回第一个失败的错误；全部通过返回 nil。
func (f *Factory) Health() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for name, db := range f.sqlDBs {
		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("db %s: %w", name, err)
		}
		if err := sqlDB.Ping(); err != nil {
			return fmt.Errorf("db %s ping: %w", name, err)
		}
	}
	return nil
}

// Close 关闭所有已创建的 SQL 和 NoSQL 连接。
func (f *Factory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for name, db := range f.sqlDBs {
		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("get sql.DB for %s: %w", name, err)
		}
		if err := sqlDB.Close(); err != nil {
			return fmt.Errorf("close db %s: %w", name, err)
		}
	}
	for name, c := range f.noSQLClients {
		if err := c.Close(); err != nil {
			return fmt.Errorf("close NoSQL %s: %w", name, err)
		}
	}
	return nil
}
