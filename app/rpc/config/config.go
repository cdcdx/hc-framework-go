package config

import (
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

// DBConfig 单库配置（驱动 + 连接池 + 读写分离）。
//
// Driver 指定数据库类型（sqlite / mysql / postgres / mongodb / clickhouse / elasticsearch）。
// DSN 可通过三种方式提供：
//  1. 顶层 Dsn 字段（最简，适用于 sqlite）
//  2. vendor 段（对标 gin 版 database.<name>.<vendor>，如 Mysql.Master / Postgres.Master）
//  3. ResolveDSN() 自动按优先级解析：vendor 段 > Dsn
//
// 示例（切换数据库只需改 Driver 和对应 vendor 段）：
//
//	# postgres
//	Driver: postgres
//	Postgres:
//	  Master: postgres://user:pass@host:5432/db?sslmode=disable
//
//	# mysql
//	Driver: mysql
//	Mysql:
//	  Master: user:pass@tcp(host:3306)/db?charset=utf8mb4&parseTime=true
//
//	# sqlite
//	Driver: sqlite
//	Dsn: file:./data/hc.db?cache=shared
//
//	# mongodb
//	Driver: mongodb
//	Mongodb:
//	  Dsn: mongodb://user:pass@host:27017/db
//
//	# clickhouse
//	Driver: clickhouse
//	Clickhouse:
//	  Dsn: clickhouse://user:pass@host:9000/db
//
//	# elasticsearch
//	Driver: elasticsearch
//	Elasticsearch:
//	  Addresses: ["http://host:9200"]
type DBConfig struct {
	// Driver 数据库驱动: sqlite / mysql / postgres / mongodb / clickhouse / elasticsearch。
	Driver string `json:",optional"`
	// Dsn 连接串。若 vendor 段提供了 Master/Dsn，则优先使用 vendor 段。
	Dsn string `json:",optional"`
	// PoolLabel 用于 db_pool_* 指标的 db 标签，多库部署时区分实例。默认为库名。
	PoolLabel string `json:",optional"`

	// ---- 连接池（gorm / sql.DB 通用）----
	MaxOpenConns    int           `json:",default=50"`
	MaxIdleConns    int           `json:",default=10"`
	ConnMaxLifetime time.Duration `json:",default=1h"`

	// ---- vendor 段：对标 gin 版 database.<name>.<vendor> ----
	// 若 vendor 段已配置 Master/Dsn，则顶层 Dsn 可省略。
	Mysql         DBVendorConfig   `json:",optional"`
	Postgres      DBVendorConfig   `json:",optional"`
	Mongodb       MongoDBConfig    `json:",optional"`
	Clickhouse    ClickhouseConfig `json:",optional"`
	Elasticsearch ESConfig         `json:",optional"`
}

// DBVendorConfig SQL 类数据库的连接信息（对标 gin 版 database.<name>.mysql/postgres）。
type DBVendorConfig struct {
	Master          string        `json:",optional"`
	Slaves          []string      `json:",optional"`
	ReplicationLag  time.Duration `json:",optional"`
	MaxIdleConns    int           `json:",optional"`
	MaxOpenConns    int           `json:",optional"`
	ConnMaxLifetime time.Duration `json:",optional"`
}

// MongoDBConfig MongoDB 连接配置。
type MongoDBConfig struct {
	Dsn          string `json:",optional"`
	MinPoolSize  int    `json:",optional,default=10"`
	MaxPoolSize  int    `json:",optional,default=100"`
	MaxIdleTime  time.Duration
}

// ClickhouseConfig ClickHouse 连接配置。
type ClickhouseConfig struct {
	Dsn          string `json:",optional"`
	MaxOpenConns int    `json:",optional,default=10"`
	MaxIdleConns int    `json:",optional,default=5"`
}

// ESConfig Elasticsearch 连接配置。
type ESConfig struct {
	Addresses   []string `json:",optional"`
	Username    string   `json:",optional"`
	Password    string   `json:",optional"`
	IndexPrefix string   `json:",optional,default=hc_logs"`
}

// ResolveDSN 解析最终的 DSN 和 Driver。
// 优先级: vendor 段 > 顶层 Dsn。
// Driver 为空时，从 vendor 段推断。
func (c *DBConfig) ResolveDSN() (driver, dsn string, err error) {
	driver = c.Driver

	// SQL 类数据库：优先从 vendor 段取 Master
	switch {
	case c.Mysql.Master != "":
		dsn = c.Mysql.Master
		if driver == "" {
			driver = "mysql"
		}
	case c.Postgres.Master != "":
		dsn = c.Postgres.Master
		if driver == "" {
			driver = "postgres"
		}
	case c.Mongodb.Dsn != "":
		dsn = c.Mongodb.Dsn
		if driver == "" {
			driver = "mongodb"
		}
	case c.Clickhouse.Dsn != "":
		dsn = c.Clickhouse.Dsn
		if driver == "" {
			driver = "clickhouse"
		}
	case c.Elasticsearch.Addresses != nil && len(c.Elasticsearch.Addresses) > 0:
		// elasticsearch 不走 DSN，走 Addresses 列表
		dsn = c.Elasticsearch.Addresses[0]
		if driver == "" {
			driver = "elasticsearch"
		}
	case c.Dsn != "":
		dsn = c.Dsn
	default:
		return "", "", fmt.Errorf("no DSN provided for driver=%q (set Dsn or vendor segment)", driver)
	}

	if driver == "" {
		return "", "", fmt.Errorf("driver is empty, cannot determine database type")
	}

	return driver, dsn, nil
}

// IsSQL 判断是否为 SQL 类数据库（走 gorm 连接）。
func (c *DBConfig) IsSQL() bool {
	d, _, _ := c.ResolveDSN()
	switch d {
	case "sqlite", "mysql", "postgres":
		return true
	default:
		return false
	}
}

// Databases 按名称索引多库配置。
type Databases map[string]DBConfig

// Config 单一领域后端配置（合并 user/idle/task/shop 四个域）。
type Config struct {
	Name string
	Log  logx.LogConf
	Telemetry struct {
		Name string
	}

	// ---- 数据源 ----
	Databases Databases `json:",optional"`

	// DB 旧版单库配置（向后兼容）。若 Databases 不为空，DB 被忽略。
	DB struct {
		Driver string
		Dsn    string
		MaxOpenConns    int
		MaxIdleConns    int
		ConnMaxLifetime time.Duration
		PoolLabel string
	} `json:",optional"`

	// ---- 缓存 ----
	Cache CacheConfig `json:",optional"`

	// ---- 消息队列 ----
	MQ MQConfig `json:",optional"`

	// ---- 认证 / 业务参数 ----
	Jwt struct {
		Algorithm      string
		SigningKey     string
		Issuer         string
		AccessTTL      time.Duration
		RefreshTTL     time.Duration
		PrivateKeyPath string
		PublicKeyPath  string
	}

	BcryptCost int

	GoogleOAuth struct {
		ClientID     string
		ClientSecret string
		RedirectURI  string
	}

	Idle struct {
		MaxActiveDevices int
		PointsPerMinute  int
		DailyPointsLimit int64
		TimeoutMinutes   int
		ScanInterval     time.Duration
	}

	FlashSale struct {
		Timeout time.Duration
	}
}

// CacheConfig 缓存配置。
type CacheConfig struct {
	Enabled bool           `json:",default=true"`
	L1      L1CacheConfig  `json:",optional"`
	L2      L2CacheConfig  `json:",optional"`
}

type L1CacheConfig struct {
	Enabled       bool          `json:",default=true"`
	MaxMemoryMB   int           `json:",default=256"`
	DefaultTTL    time.Duration `json:",default=5m"`
	NumCounters   int64         `json:",default=10000000"`
	MaxCost       int64         `json:",default=268435456"`
}

type L2CacheConfig struct {
	Enabled   bool     `json:",default=true"`
	Type      string   `json:",default=redis"`
	Addresses []string `json:",optional"`
	Password  string   `json:",optional"`
	DB        int      `json:",default=0"`
	PoolSize  int      `json:",default=200"`
}

type MQConfig struct {
	Enabled bool   `json:",default=false"`
	Type    string `json:",default=memory"`
}
