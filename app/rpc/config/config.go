package config

import (
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/common/dbclient"
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
	Dsn         string        `json:",optional"`
	MinPoolSize int           `json:",optional,default=10"`
	MaxPoolSize int           `json:",optional,default=100"`
	MaxIdleTime time.Duration `json:",optional"`
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
// 优先级: 与 driver 匹配的 vendor 段 > 顶层 Dsn。
// Driver 为空时，从 vendor 段推断。
func (c *DBConfig) ResolveDSN() (driver, dsn string, err error) {
	driver = c.Driver

	// 仅当 driver 与 vendor 段匹配时才使用 vendor 段。
	// 例如 driver=sqlite 时不会误取 mysql 段的 DSN。
	switch {
	case driver == "sqlite":
		dsn = c.Dsn
	case driver == "mysql":
		dsn = c.Mysql.Master
		if dsn == "" {
			dsn = c.Dsn
		}
	case driver == "postgres" || driver == "postgresql":
		dsn = c.Postgres.Master
		if dsn == "" {
			dsn = c.Dsn
		}
	case driver == "mongodb":
		dsn = c.Mongodb.Dsn
		if dsn == "" {
			dsn = c.Dsn
		}
	case driver == "clickhouse":
		dsn = c.Clickhouse.Dsn
		if dsn == "" {
			dsn = c.Dsn
		}
	case driver == "elasticsearch":
		if c.Elasticsearch.Addresses != nil && len(c.Elasticsearch.Addresses) > 0 {
			dsn = c.Elasticsearch.Addresses[0]
		} else {
			dsn = c.Dsn
		}
	case driver == "":
		// Driver 为空时自动推断（保留原行为）
		switch {
		case c.Mysql.Master != "":
			driver = "mysql"
			dsn = c.Mysql.Master
		case c.Postgres.Master != "":
			driver = "postgres"
			dsn = c.Postgres.Master
		case c.Mongodb.Dsn != "":
			driver = "mongodb"
			dsn = c.Mongodb.Dsn
		case c.Clickhouse.Dsn != "":
			driver = "clickhouse"
			dsn = c.Clickhouse.Dsn
		case c.Elasticsearch.Addresses != nil && len(c.Elasticsearch.Addresses) > 0:
			driver = "elasticsearch"
			dsn = c.Elasticsearch.Addresses[0]
		case c.Dsn != "":
			driver = "sqlite"
			dsn = c.Dsn
		default:
			return "", "", fmt.Errorf("no DSN provided and driver is empty")
		}
	default:
		// 未知 driver：兜底取顶层 Dsn
		dsn = c.Dsn
	}

	if dsn == "" {
		return "", "", fmt.Errorf("no DSN resolved for driver=%q", driver)
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
	case "sqlite", "mysql", "postgres", "postgresql":
		return true
	default:
		return false
	}
}

// resolvePool 计算最终连接池参数。
// 优先级: 与 driver 匹配的 vendor 段 > 顶层通用字段。
func (c *DBConfig) resolvePool(driver string) (maxOpen, maxIdle int, connMax time.Duration) {
	maxOpen, maxIdle, connMax = c.MaxOpenConns, c.MaxIdleConns, c.ConnMaxLifetime

	var v DBVendorConfig
	switch driver {
	case "mysql":
		v = c.Mysql
	case "postgres", "postgresql":
		v = c.Postgres
	default:
		return
	}

	if v.MaxOpenConns > 0 {
		maxOpen = v.MaxOpenConns
		maxIdle = v.MaxIdleConns
	}
	if v.ConnMaxLifetime > 0 {
		connMax = v.ConnMaxLifetime
	}
	return
}

// Databases 按名称索引多库配置。
type Databases map[string]DBConfig

// ToSpecs 将配置层的多库配置适配为 dbclient.Specs。
//
// 这是 config → dbclient 的**单向适配点**：DSN 解析与连接池优先级在此完成，
// dbclient 只消费解析后的结果，因此 common 层无需反向依赖 app 层配置结构。
// 解析失败的库会被跳过（保留 Driver 以便 Factory 返回有意义的错误）。
func (d Databases) ToSpecs() dbclient.Specs {
	specs := make(dbclient.Specs, len(d))
	for name, cfg := range d {
		driver, dsn, err := cfg.ResolveDSN()
		if err != nil {
			logx.Errorf("[db] resolve %s failed, skipped: %v", name, err)
			specs[name] = dbclient.DBSpec{Driver: cfg.Driver, PoolLabel: cfg.PoolLabel}
			continue
		}
		maxOpen, maxIdle, connMax := cfg.resolvePool(driver)
		specs[name] = dbclient.DBSpec{
			Driver:          driver,
			DSN:             dsn,
			PoolLabel:       cfg.PoolLabel,
			MaxOpenConns:    maxOpen,
			MaxIdleConns:    maxIdle,
			ConnMaxLifetime: connMax,
		}
	}
	return specs
}

// Config 单一领域后端配置（合并 user/idle/task/shop 四个域）。
type Config struct {
	Name      string
	Log       logx.LogConf
	Telemetry struct {
		Name string
	}

	// ---- 数据源 ----
	Databases Databases `json:",optional"`

	// DB 旧版单库配置（向后兼容）。若 Databases 不为空，DB 被忽略。
	DB struct {
		Driver          string
		Dsn             string
		MaxOpenConns    int
		MaxIdleConns    int
		ConnMaxLifetime time.Duration
		PoolLabel       string
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

	Idle IdleConfig

	FlashSale struct {
		Timeout time.Duration
	}
}

// IdleConfig 挂机(idle)域配置
type IdleConfig struct {
	MaxActiveDevices int
	PointsPerMinute  int
	DailyPointsLimit int64
	TimeoutMinutes   int
	ScanInterval     time.Duration
}

// CacheConfig 缓存配置。
type CacheConfig struct {
	Enabled bool             `json:",default=true"`
	L1      L1CacheConfig    `json:",optional"`
	L2      L2CacheConfig    `json:",optional"`
	Bloom   BloomCacheConfig `json:",optional"`
}

// BloomCacheConfig 布隆过滤器配置（缓存穿透保护）。
//
// 布隆只增不删，容量按业务 key 总量预估：实际插入量超过 ExpectedKeys 时
// 假阳性率上升（穿透保护变弱），但不会产生假阴性，不影响数据正确性。
// 默认 100 万 key / 1% 假阳性约占用 1.2 MB 内存。
type BloomCacheConfig struct {
	ExpectedKeys      uint64  `json:",default=1000000"`
	FalsePositiveRate float64 `json:",default=0.01"`
}

type L1CacheConfig struct {
	Enabled     bool          `json:",default=true"`
	MaxMemoryMB int           `json:",default=256"`
	DefaultTTL  time.Duration `json:",default=5m"`
	NumCounters int64         `json:",default=10000000"`
	MaxCost     int64         `json:",default=268435456"`
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
