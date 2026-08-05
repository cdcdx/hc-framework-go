package dbclient

import "time"

// DBSpec 单库连接规格。这是 dbclient 对上层配置的**唯一依赖契约**：
// 上层（app/rpc/config 等）负责把自己的配置结构体转换成 DBSpec，
// dbclient 不反向 import 任何 app 层包，从而保持 common 层的可复用性。
type DBSpec struct {
	// Driver 数据库驱动: sqlite / mysql / postgres / mongodb / clickhouse / elasticsearch。
	Driver string
	// DSN 已解析完成的连接串。上层负责按 vendor 段优先级解析后填入。
	DSN string
	// PoolLabel 用于 db_pool_* 指标的 db 标签；为空时取库名。
	PoolLabel string

	// ---- 连接池（0 表示使用驱动默认值）----
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// IsSQL 判断是否为 SQL 类数据库（走 gorm 连接）。
func (s DBSpec) IsSQL() bool {
	switch s.Driver {
	case "sqlite", "mysql", "postgres", "postgresql":
		return true
	default:
		return false
	}
}

// Specs 按库名索引的多库连接规格。
type Specs map[string]DBSpec
