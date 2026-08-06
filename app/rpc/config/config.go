package config

import (
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/common/cache"
	"github.com/cdcdx/hc-framework-go/common/dbclient"
	"github.com/cdcdx/hc-framework-go/common/mq"
	"github.com/zeromicro/go-zero/core/logx"
)

// DBConfig 单库配置（驱动 + 连接池）。
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
	// 默认值与 gormx.resolvePoolDefaults 对齐（20/10），避免单库默认 50 导致多库总和
	// 超过 MySQL 默认 max_connections(151)。压测场景建议显式调大并同步调 MySQL 参数。
	MaxOpenConns    int           `json:",default=20"`
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
// 仅支持单主连接串 + 连接池覆盖，读写分离暂未实现。
type DBVendorConfig struct {
	Master          string        `json:",optional"`
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
// 直接按 Driver 字段判定，不触发 ResolveDSN（避免空 Driver 时不必要的推断副作用）。
func (c *DBConfig) IsSQL() bool {
	switch c.Driver {
	case "sqlite", "mysql", "postgres", "postgresql":
		return true
	default:
		return false
	}
}

// resolvePool 计算最终连接池参数。
// 优先级: 与 driver 匹配的 vendor 段 > 顶层通用字段。
// vendor 段各字段独立覆盖：MaxOpenConns>0 仅覆盖 maxOpen，MaxIdleConns>0 仅覆盖 maxIdle，
// ConnMaxLifetime>0 仅覆盖 connMax。避免 MaxOpenConns>0 时连带把 maxIdle 刷为 0。
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
	}
	if v.MaxIdleConns > 0 {
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

	// InitialPointsBalance 新用户注册时的初始积分余额。
	// GORM 在 Create 时会把结构体零值一并写入，DB 层 DEFAULT 不会生效，
	// 因此注册逻辑必须显式赋值。
	InitialPointsBalance int64 `json:",default=1000"`

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
	// Enabled 缓存总开关。默认 false（须显式开启，避免未配置 Redis 时启动报错）。
	Enabled bool             `json:",default=false"`
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

// ToCacheBloom 桥接到 common/cache 的 BloomConfig，供 ServiceContext 装配。
func (c BloomCacheConfig) ToCacheBloom() cache.BloomConfig {
	return cache.BloomConfig{
		ExpectedKeys:      c.ExpectedKeys,
		FalsePositiveRate: c.FalsePositiveRate,
	}
}

type L1CacheConfig struct {
	Enabled     bool          `json:",default=true"`
	MaxMemoryMB int           `json:",default=256"`
	DefaultTTL  time.Duration `json:",default=5m"`
	NumCounters int64         `json:",default=10000000"`
	MaxCost     int64         `json:",default=268435456"`
	// BufferItems Ristretto 写缓冲容量，默认 64。越大写吞吐越高但内存占用略增。
	BufferItems int64 `json:",default=64"`
}

// ToCacheL1 桥接到 common/cache 的 L1Config，供 ServiceContext 装配两级缓存。
// MaxMemoryMB 仅作为文档参考，实际以 MaxCost 为准（在 ServiceContext 中换算）。
func (c L1CacheConfig) ToCacheL1() cache.L1Config {
	return cache.L1Config{
		Enabled:     c.Enabled,
		MaxMemoryMB: c.MaxMemoryMB,
		DefaultTTL:  c.DefaultTTL,
		NumCounters: c.NumCounters,
		MaxCost:     c.MaxCost,
		BufferItems: c.BufferItems,
	}
}

type L2CacheConfig struct {
	Enabled   bool     `json:",default=true"`
	Type      string   `json:",default=redis"`
	Addresses []string `json:",optional"`
	Username  string   `json:",optional"`
	Password  string   `json:",optional"`
	DB        int      `json:",default=0"`
	PoolSize  int      `json:",default=200"`
	// 连接/读写超时，0 表示使用 Redis 客户端默认（5s）。
	DialTimeout  time.Duration `json:",optional"`
	ReadTimeout  time.Duration `json:",optional"`
	WriteTimeout time.Duration `json:",optional"`
	// TLS 是否启用（云 Redis / 公网部署建议开启）。
	TLS bool `json:",optional"`
}

// ToCacheL2 桥接到 common/cache 的 L2Config，供 ServiceContext 装配两级缓存。
//
// Type 可为 "redis" 或 "valkey"（二者协议兼容，共用同一客户端实现），
// 超时与 TLS 配置一并透传，支持云托管/高延迟网络场景。
func (c L2CacheConfig) ToCacheL2() cache.L2Config {
	return cache.L2Config{
		Enabled:      c.Enabled,
		Type:         c.Type,
		Addresses:    c.Addresses,
		Username:     c.Username,
		Password:     c.Password,
		DB:           c.DB,
		PoolSize:     c.PoolSize,
		DialTimeout:  c.DialTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		TLS:          c.TLS,
	}
}

type MQConfig struct {
	// Enabled 默认开启：memory 模式无外部依赖，开箱即用，使任务进度上报等
	// 异步解耦默认生效；生产可切 kafka/rocketmq 等并显式配置。
	Enabled bool   `json:",default=true"`
	Type    string `json:",default=memory"`
	// 各后端配置完全独立、互不互通。切换 Type 后只读取对应子段。
	Kafka    KafkaMQConfig    `json:",optional"`
	RocketMQ RocketMQConfig   `json:",optional"`
	RabbitMQ RabbitMQConfig   `json:",optional"`
	Memory   MemoryMQConfig   `json:",optional"`
}

// KafkaMQConfig Kafka 专用连接参数（完全独立）。
type KafkaMQConfig struct {
	// Brokers broker 地址列表（如 ["127.0.0.1:9092"]）。
	Brokers []string `json:",optional"`
	// ConsumerGroup 消费者组 ID。
	ConsumerGroup string `json:",optional"`
	// Topic 默认主题（未显式指定 topic 时使用）。
	Topic string `json:",optional"`
	// RequiredAcks：0 不等待 / 1 等待 leader / -1 等待全部副本（默认 1）。
	RequiredAcks int `json:",optional"`
	// BatchSize 批量攒批上限（条）。
	BatchSize int `json:",optional"`
	// BatchBytes 批量字节上限。
	BatchBytes int `json:",optional"`
}

// RocketMQConfig RocketMQ 专用连接参数（完全独立）。
type RocketMQConfig struct {
	// NameServer 地址列表（如 ["127.0.0.1:9876"]）。
	NameServer []string `json:",optional"`
	// Group 消费/生产组名。
	Group string `json:",optional"`
	// Retry 发送重试次数。
	Retry int `json:",optional"`
	// Topic 默认主题。
	Topic string `json:",optional"`
	// AccessKey / SecretKey 用于开启 ACL 的 NameServer 集群。
	AccessKey string `json:",optional"`
	SecretKey string `json:",optional"`
	// Namespace 命名空间（如阿里云 ONS 实例 ID）。
	Namespace string `json:",optional"`
}

// RabbitMQConfig RabbitMQ 专用连接参数（完全独立）。
type RabbitMQConfig struct {
	// URL 完整的 amqp 连接串（如 amqp://user:pass@host:5672/vhost）。
	URL string `json:",optional"`
	// Exchange 交换机名称（发布/订阅时声明）。
	Exchange string `json:",optional"`
	// ExchangeType 交换机类型：direct / topic / fanout（默认 topic）。
	ExchangeType string `json:",default=topic"`
	// Queue 默认队列名（消费时声明并绑定；空则按 topic 派生）。
	Queue string `json:",optional"`
	// RoutingKey 路由键（默认与 topic 一致）。
	RoutingKey string `json:",optional"`
	// Topic 未显式指定 topic 时使用的默认 routing key。
	Topic string `json:",optional"`
	// Prefetch 消费者预取条数（QoS），默认 1（公平分发）。
	Prefetch int `json:",default=1"`
}

// MemoryMQConfig 内存队列专用参数（完全独立）。
type MemoryMQConfig struct {
	// BufferSize 通道缓冲大小，默认 1024。
	BufferSize int `json:",default=1024"`
}

// ToMQConfig 桥接到 common/mq 的 Config，供 ServiceContext 装配消息队列。
// 各后端配置完全独立透传，切换 Type 后只读取对应子段。
func (c MQConfig) ToMQConfig() mq.Config {
	return mq.Config{
		Enabled: c.Enabled,
		Type:    c.Type,
		Kafka: mq.KafkaConfig{
			Brokers:       c.Kafka.Brokers,
			ConsumerGroup: c.Kafka.ConsumerGroup,
			Topic:         c.Kafka.Topic,
			RequiredAcks:  c.Kafka.RequiredAcks,
			BatchSize:     c.Kafka.BatchSize,
			BatchBytes:    c.Kafka.BatchBytes,
		},
		RocketMQ: mq.RocketMQConfig{
			NameServer: c.RocketMQ.NameServer,
			Group:      c.RocketMQ.Group,
			Retry:      c.RocketMQ.Retry,
			Topic:      c.RocketMQ.Topic,
			AccessKey:  c.RocketMQ.AccessKey,
			SecretKey:  c.RocketMQ.SecretKey,
			Namespace:  c.RocketMQ.Namespace,
		},
		RabbitMQ: mq.RabbitMQConfig{
			URL:          c.RabbitMQ.URL,
			Exchange:     c.RabbitMQ.Exchange,
			ExchangeType: c.RabbitMQ.ExchangeType,
			Queue:        c.RabbitMQ.Queue,
			RoutingKey:   c.RabbitMQ.RoutingKey,
			Topic:        c.RabbitMQ.Topic,
			Prefetch:     c.RabbitMQ.Prefetch,
		},
		Memory: mq.MemoryConfig{
			BufferSize: c.Memory.BufferSize,
		},
	}
}
