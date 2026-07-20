// Package config 配置加载、解析与热更新管理。
package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config 顶层配置结构
type Config struct {
	Server         ServerConfig         `mapstructure:"server"`
	Cache          CacheConfig          `mapstructure:"cache"`
	Database       DatabaseConfig       `mapstructure:"database"`
	MQ             MQConfig             `mapstructure:"mq"`
	Auth           AuthConfig           `mapstructure:"auth"`
	OAuth          OAuthConfig          `mapstructure:"oauth"`
	Captcha        CaptchaConfig        `mapstructure:"captcha"`
	Security       SecurityConfig       `mapstructure:"security"`
	RateLimit      RateLimitConfig      `mapstructure:"ratelimit"`
	CircuitBreaker CircuitBreakerConfig `mapstructure:"circuit_breaker"`
	Logging        LoggingConfig        `mapstructure:"logging"`
	Tracing        TracingConfig        `mapstructure:"tracing"`
	Metrics        MetricsConfig        `mapstructure:"metrics"`
	Health         HealthConfig         `mapstructure:"health"`
	Task           TaskConfig           `mapstructure:"task"`
	Idle           IdleConfig           `mapstructure:"idle"`
	Migration      MigrationConfig      `mapstructure:"migration"`
	Mail           MailConfig           `mapstructure:"mail"`
	Admin          AdminConfig          `mapstructure:"admin"`
}

// AdminConfig 运营管理接口配置（抢购活动创建/预热/对账等需此令牌，见 docs/21）。
// Token 为空时所有 /api/v1/admin/* 接口一律返回 403，避免误暴露运营能力。
type AdminConfig struct {
	Token string `mapstructure:"token"`
}

// ServerConfig 服务配置
type ServerConfig struct {
	Port              int           `mapstructure:"port"`
	Host              string        `mapstructure:"host"`
	Mode              string        `mapstructure:"mode"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout"`
	WriteTimeout      time.Duration `mapstructure:"write_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout"`
	ShutdownTimeout   time.Duration `mapstructure:"shutdown_timeout"`
	RequestTimeout    time.Duration `mapstructure:"request_timeout"`
	// MaxHeaderBytes 限制请求头最大字节数（默认 1MB）。高并发下防御超大/畸形请求头占用内存与慢头攻击。
	// <=0 时回落 net/http 默认 1MB。
	MaxHeaderBytes int `mapstructure:"max_header_bytes"`
	// MaxConnsPerIP 单 IP 最大并发连接数（0=不限）。防止单一客户端耗尽连接，提供基础连接级限流。
	MaxConnsPerIP int `mapstructure:"max_conns_per_ip"`
	// MaxConns 全局最大并发连接数（0=不限）。作为进程级连接耗尽防线（K8s 下仍依赖 readiness/HPA）。
	// 超过上限的新连接被立即拒绝（连接级快速失败），保护后端资源不被连接风暴打垮。
	MaxConns int `mapstructure:"max_conns"`
	// ConcurrencyLimit 应用层在途请求并发硬上限（0=不限，默认关闭）。
	// 在「连接级上限 MaxConns」与「令牌桶限流」之后、业务处理器之前，对同时在途的请求数再设一道
	// 进程级上限：达到上限即快速失败返回 503（load shedding），避免无界 goroutine 在 DB/下游抖动时
	// 堆积压垮后端。容量按容量规划设定（≈ DB 连接池上限 / 单请求平均下游并发度），而非越大越好。
	// 注意：信号量容量在启动期固定、不可动态伸缩，故本上限为「重启生效」，不受配置热更新影响（与 MaxConns 一致）。
	ConcurrencyLimit int        `mapstructure:"concurrency_limit"`
	CORS             CORSConfig `mapstructure:"cors"`
}

type CORSConfig struct {
	AllowedOrigins []string `mapstructure:"allowed_origins"`
	AllowedMethods []string `mapstructure:"allowed_methods"`
	AllowedHeaders []string `mapstructure:"allowed_headers"`
}

// CacheConfig 缓存配置
type CacheConfig struct {
	L1                L1CacheConfig `mapstructure:"l1"`
	L2                L2CacheConfig `mapstructure:"l2"`
	HotKey            HotKeyConfig  `mapstructure:"hotkey"`
	TTLJitter         float64       `mapstructure:"ttl_jitter"`
	Bloom             BloomConfig   `mapstructure:"bloom"`
	NullCacheTTL      time.Duration `mapstructure:"null_cache_ttl"`
	DoubleDeleteDelay time.Duration `mapstructure:"double_delete_delay"`
	ReplicationLag    time.Duration `mapstructure:"replication_lag"` // 预估主从复制延迟
	KeyPrefix         string        `mapstructure:"key_prefix"`
	InvalidateChannel string        `mapstructure:"invalidate_channel"`
	Degrade           DegradeConfig `mapstructure:"degrade"` // 读写降级（需求 §11）
}

// DegradeConfig 缓存读写降级配置（需求 §11）。
//   - Enabled：总开关。仅当 Enabled=true 且对应子项开启时，按配置降级；
//   - ReadSkipL2：读降级，跳过 L2（Redis）直读 DB；
//   - WriteAsync：写降级，同步写 L2 转异步（投递 Kafka 事件，由消费者异步刷新；无 Kafka 时退化为本地异步写）。
//
// 注：熔断器（circuit_breaker）触发打开时会强制全量降级（忽略此处开关，read/write 均降级），
// 见 internal/degrade 与 middleware/circuitbreaker.go。
type DegradeConfig struct {
	Enabled    bool `mapstructure:"enabled"`
	ReadSkipL2 bool `mapstructure:"read_skip_l2"`
	WriteAsync bool `mapstructure:"write_async"`
}

type L1CacheConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	MaxMemoryMB int           `mapstructure:"max_memory_mb"`
	DefaultTTL  time.Duration `mapstructure:"default_ttl"`
	NumCounters int64         `mapstructure:"num_counters"`
	MaxCost     int64         `mapstructure:"max_cost"`
}

type L2CacheConfig struct {
	Enabled         bool           `mapstructure:"enabled"`
	Type            string         `mapstructure:"type"`
	Addresses       []string       `mapstructure:"addresses"`
	Password        string         `mapstructure:"password"`
	DB              int            `mapstructure:"db"`
	PoolSize        int            `mapstructure:"pool_size"`
	MinIdleConns    int            `mapstructure:"min_idle_conns"`
	DialTimeout     time.Duration  `mapstructure:"dial_timeout"`
	ReadTimeout     time.Duration  `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration  `mapstructure:"write_timeout"`
	MaxPipelineSize int            `mapstructure:"max_pipeline_size"`
	Sentinel        SentinelConfig `mapstructure:"sentinel"`
	Cluster         ClusterConfig  `mapstructure:"cluster"`
}

type SentinelConfig struct {
	Enabled    bool     `mapstructure:"enabled"`
	MasterName string   `mapstructure:"master_name"`
	Addresses  []string `mapstructure:"addresses"`
}

type ClusterConfig struct {
	Enabled   bool     `mapstructure:"enabled"`
	Addresses []string `mapstructure:"addresses"`
}

type HotKeyConfig struct {
	Enabled       bool `mapstructure:"enabled"`
	WindowSeconds int  `mapstructure:"window_seconds"`
	TopN          int  `mapstructure:"top_n"`
	TTLMultiplier int  `mapstructure:"ttl_multiplier"`
}

type BloomConfig struct {
	Enabled   bool    `mapstructure:"enabled"`
	Capacity  int64   `mapstructure:"capacity"`
	ErrorRate float64 `mapstructure:"error_rate"`
	RedisKey  string  `mapstructure:"redis_key"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Business       DBConfig       `mapstructure:"business"`
	User           DriverConfig   `mapstructure:"user"`
	Monitor        DriverConfig   `mapstructure:"monitor"`
	Log            DriverConfig   `mapstructure:"log"`
	Fallback       FallbackConfig `mapstructure:"fallback"`
	ReadWriteSplit RWSplitConfig  `mapstructure:"read_write_split"`
}

type DBConfig struct {
	Driver   string         `mapstructure:"driver"`
	DSN      string         `mapstructure:"dsn"`
	MySQL    MySQLConfig    `mapstructure:"mysql"`
	Postgres PostgresConfig `mapstructure:"postgres"`
}

type DriverConfig struct {
	Driver        string              `mapstructure:"driver"`
	DSN           string              `mapstructure:"dsn"`
	MongoDB       MongoDBConfig       `mapstructure:"mongodb"`
	Elasticsearch ElasticsearchConfig `mapstructure:"elasticsearch"`
	ClickHouse    ClickHouseConfig    `mapstructure:"clickhouse"`
	// MySQL 子段：当 driver 为 "mysql" 时生效（测试环境常用，与 business 同构的 GORM/MySQL 适配器）。
	// 使 user/monitor/log 在测试环境可切换到 MySQL，而不必依赖生产用的 MongoDB/ClickHouse/Elasticsearch。
	// user/monitor/log 切到 MySQL 时的连接池即取自本子段（max_idle_conns 等），由 driverConn 解析。
	MySQL MySQLConfig `mapstructure:"mysql"`
	// Pool 连接池配置：仅当 driver 为「网络型 GORM 驱动」且非 mysql 时生效（如未来为 user/monitor/log
	// 启用 postgres 测试库）。注意两点：
	//   - driver 为 mysql 时连接池来自上面的 MySQL 子段（mysql.max_idle_conns 等），此处 Pool 不生效；
	//   - driver 为 sqlite（user/monitor/log 的默认兜底，以及异构后端 mongodb/clickhouse/elasticsearch
	//     不可用时的回退）时，连接池由 SQLite 适配器硬性设为 1/1（文件型 SQLite 单写者，刻意不可调），
	//     此处 Pool 同样不生效。故本字段当前对 user/monitor/log 多为占位，真正生效的池配置在 MySQL 子段。
	Pool DBPoolConfig `mapstructure:"pool"`
}

// DBPoolConfig 连接池配置（与 db.WithMySQLPool / db.WithPostgreSQLPool 对齐）
type DBPoolConfig struct {
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type MySQLConfig struct {
	Master            string        `mapstructure:"master"`
	Slaves            []string      `mapstructure:"slaves"`
	MaxIdleConns      int           `mapstructure:"max_idle_conns"`
	MaxOpenConns      int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime   time.Duration `mapstructure:"conn_max_lifetime"`
	SlavePingInterval time.Duration `mapstructure:"slave_ping_interval"`
	ReplicationLag    time.Duration `mapstructure:"replication_lag"` // 预估主从复制延迟（用于延迟双删）
}

type PostgresConfig struct {
	Master          string        `mapstructure:"master"`
	Slaves          []string      `mapstructure:"slaves"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type MongoDBConfig struct {
	DSN         string `mapstructure:"dsn"`
	Database    string `mapstructure:"database"`
	Username    string `mapstructure:"username"`
	Password    string `mapstructure:"password"`
	MinPoolSize uint64 `mapstructure:"min_pool_size"`
	MaxPoolSize uint64 `mapstructure:"max_pool_size"`
}

type ClickHouseConfig struct {
	DSN       string   `mapstructure:"dsn"`       // "clickhouse://user:pass@host:port/database"
	Addresses []string `mapstructure:"addresses"` // 兼容旧配置（DSN 优先）
	Database  string   `mapstructure:"database"`  // 兼容旧配置
	Username  string   `mapstructure:"username"`  // 兼容旧配置
	Password  string   `mapstructure:"password"`  // 兼容旧配置
}

type ElasticsearchConfig struct {
	Addresses   []string `mapstructure:"addresses"`
	Username    string   `mapstructure:"username"`
	Password    string   `mapstructure:"password"`
	IndexPrefix string   `mapstructure:"index_prefix"`
}

type FallbackConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	FallbackDriver string `mapstructure:"fallback_driver"`
}

type RWSplitConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// MQConfig 消息队列配置
// 统一由 mq.type 选择底层实现（kafka/rabbitmq/rocketmq/memory/none），所有队列共用同一套
// 事件→主题映射与消费语义；none/空表示不启用事件总线（仅落库、不发事件）。
//
// 消费组与消费重试：
//   - kafka 沿用自身 mq.kafka.consumer.*（含 group_id 与重试参数）；
//   - rabbitmq/rocketmq/memory 优先使用顶层 mq.consumer_group 与 mq.consumer.*，
//     未配置时回落 mq.kafka.consumer.*（向后兼容早期把所有配置都写在 kafka 段下的用法）。
//
// 见 internal/mq/factory.go 的 consumerGroup / consumerRetryOf。
type MQConfig struct {
	Type          string           `mapstructure:"type"`
	ConsumerGroup string           `mapstructure:"consumer_group"` // 统一消费组（所有类型共用）；缺省落 kafka.consumer.group_id
	Kafka         KafkaConfig      `mapstructure:"kafka"`
	RabbitMQ      RabbitMQConfig   `mapstructure:"rabbitmq"`
	RocketMQ      RocketMQConfig   `mapstructure:"rocketmq"`
	Consumer      MQConsumerConfig `mapstructure:"consumer"` // 统一消费侧（重试等）；非 kafka 类型优先使用，缺省回落 kafka.consumer
}

// MQConsumerConfig 统一消费侧配置，供 rabbitmq/rocketmq/memory 类型使用（kafka 沿用自身 kafka.consumer）。
type MQConsumerConfig struct {
	RetryMax       int           `mapstructure:"retry_max"`        // 消费失败重试上限（>=0 至少尝试 1 次）；rocketmq 交由服务端重试，此项不生效
	RetryBaseDelay time.Duration `mapstructure:"retry_base_delay"` // 重试退避基准
	RetryMaxDelay  time.Duration `mapstructure:"retry_max_delay"`  // 重试退避上限
	// Prefetch 仅 rabbitmq 生效：channel 预取上限（未 Ack 在途消息数），同时作为有界并发处理的上限，
	// 防止消息洪峰时无限起 goroutine / 占用内存。<=0 时用默认值 64。
	Prefetch int `mapstructure:"prefetch"`
	// Concurrency 仅 memory 消费者生效：每个 topic 的处理 goroutine 数（默认 1，串行）。memory 是进程内
	// 总线（at-most-once，缓冲满即丢弃），仅用于本地开发/测试；提高并发可缓解「单 goroutine 串行消费」的
	// 吞吐瓶颈，但无法改变“缓冲满丢弃”的语义。<=0 用默认 1。
	Concurrency int `mapstructure:"concurrency"`
}

type KafkaConfig struct {
	Brokers  []string            `mapstructure:"brokers"`
	Producer KafkaProducerConfig `mapstructure:"producer"`
	Consumer KafkaConsumerConfig `mapstructure:"consumer"`
	Topics   KafkaTopicsConfig   `mapstructure:"topics"`
}

type KafkaProducerConfig struct {
	Acks               string        `mapstructure:"acks"`
	EnableIdempotence  bool          `mapstructure:"enable_idempotence"`
	Compression        string        `mapstructure:"compression"`
	MaxRetries         int           `mapstructure:"max_retries"`
	DeliveryTimeout    time.Duration `mapstructure:"delivery_timeout"`
	BufferMemory       int           `mapstructure:"buffer_memory"`
	BatchSize          int           `mapstructure:"batch_size"`
	MaxInFlight        int           `mapstructure:"max_in_flight"`
	QueueSize          int           `mapstructure:"queue_size"`           // 内存发送队列长度（异步缓冲）
	MaxOverflowWorkers int           `mapstructure:"max_overflow_workers"` // 队列满时溢出发送的最大并发 goroutine 数（C3 限流，默认 256）
	DLQEnabled         bool          `mapstructure:"dlq_enabled"`          // Broker 不可用时降级到本地文件
	DLQLocalPath       string        `mapstructure:"dlq_local_path"`       // DLQ 目录（JSONL，按 topic 分文件）
}

type KafkaConsumerConfig struct {
	GroupID              string        `mapstructure:"group_id"`
	MaxPollRecords       int           `mapstructure:"max_poll_records"`
	BatchTriggerCount    int           `mapstructure:"batch_trigger_count"`
	BatchTriggerInterval time.Duration `mapstructure:"batch_trigger_interval"`
	EnableAutoCommit     bool          `mapstructure:"enable_auto_commit"`
	WorkerPoolSize       int           `mapstructure:"worker_pool_size"`
	RetryMax             int           `mapstructure:"retry_max"`
	RetryBaseDelay       time.Duration `mapstructure:"retry_base_delay"`
	RetryMaxDelay        time.Duration `mapstructure:"retry_max_delay"`
	DLQSuffix            string        `mapstructure:"dlq_suffix"`
	DLQLocalPath         string        `mapstructure:"dlq_local_path"`
	CloseTimeout         time.Duration `mapstructure:"close_timeout"` // reader.Close() 兜底超时（C2，默认 10s）

	// 以下字段对齐参考项目 high-concurrency-framework-go 的可用写法。
	// 注意：参考项目能跑是因为它运行在 ZooKeeper 模式的 broker 上；本项目是 Kafka 4.0
	// KRaft，kafka-go v0.4.51 的 group coordinator 协议与之不兼容，只要带 GroupID
	// 就会报 -1 Unknown。因此本项目改为「无消费组 + 按分区直读」，这些字段仍被 Reader 使用。
	MinBytes        int  `mapstructure:"min_bytes"`         // 单次拉取最小字节（参考：1024）
	MaxBytes        int  `mapstructure:"max_bytes"`         // 单次拉取最大字节（参考：10MB）
	StartFromLatest bool `mapstructure:"start_from_latest"` // true=跳过积压从最新消费；false=从头消费

	// 无消费组模式下的 offset 持久化路径（默认 ./data/kafka-offsets.json）。
	// 重启后从已处理位置续读，弥补无消费组「跨重启 offset 不持久」的缺陷。
	// 多实例（InstanceCount>1）时框架会在文件名（扩展名前）追加 .<InstanceID> 自动区分，
	// 避免多个实例写同一文件互相覆盖（见 §6.5 #2 根因）。
	OffsetStorePath string `mapstructure:"offset_store_path"`

	// 无消费组模式下的多实例分区分配（水平扩展，替代消费组再均衡，见 13 §6.5 #1 / §3.53）。
	// 仅当 InstanceCount>1 时生效：本实例仅消费 partition % InstanceCount == InstanceID 的分区，
	// N 个实例各持互不重叠的分区子集，消除「无消费组多实例重复消费」。InstanceID 取 [0, InstanceCount)。
	// InstanceCount<=1（默认）时不裁剪，全量消费，向后兼容。
	// 注意：该分配是静态的，实例数变化时需同步调整 InstanceCount（配合 scan_sharding / leader_election 使用）。
	InstanceID    int `mapstructure:"instance_id"`
	InstanceCount int `mapstructure:"instance_count"`

	// PartitionFetchStrict 分区查询失败时的启动策略（见 13 §6.5 #7 / §3.54）。
	//   - false（默认，韧性优先）：某个 topic 查询 partition 失败（如 topic 尚未创建、broker 未开
	//     auto.create.topics.enable）时，仅告警并跳过该 topic，继续订阅其余就绪 topic；
	//     仅当【全部】 topic 都失败（零 reader、无任何分区可消费）时才返回错误。
	//     恢复类似消费组「部分 topic 就绪也能跑」的韧性，避免「一个 topic 不存在拖垮整个消费端」。
	//   - true（严格 fail-fast）：任一 topic 查询失败即整体启动失败（旧行为）。
	//     适合「所有 topic 必须预先存在」的强约束部署，可尽早暴露配置笔误 / 漏建 topic。
	PartitionFetchStrict bool `mapstructure:"partition_fetch_strict"`

	// PartitionRefreshInterval 分区扩容热感知轮询间隔（见 13 §6.5 #4 / §3.57）。
	//   - <=0（默认，禁用）：保持旧行为——启动时固定分区集合，运行中扩容需重启生效。
	//   - >0：每间隔重查各 topic 分区数，发现新增分区即动态建 reader 消费，无需重启
	//     （规避「topic 运行中扩容、新分区长期不被消费」的盲区）。瞬时查询失败仅告警跳过本轮。
	PartitionRefreshInterval time.Duration `mapstructure:"partition_refresh_interval"`

	// OffsetFlushInterval 文件 offset 落盘间隔（见 13 §6.5 #3 / §3.58）。
	// 仅文件存储生效（SQL 存储每条 Commit 即落库，无视此值）。默认 5s；调小可缩短
	// 「进程崩溃 → 重放最近窗口」的 at-least-once 重复窗口，代价是更频繁的磁盘写。
	// 对不丢消息要求更高的单分区场景，可同时开 ordered_commit=true（每条消息同步落盘）。
	OffsetFlushInterval time.Duration `mapstructure:"offset_flush_interval"`

	// ConnectionWarnThreshold 多连接开销告警阈值（见 13 §6.5 #8）。每个 (topic, partition)
	// 一个 Reader 一个 broker TCP 连接，分区多时连接数=分区数。reader 数超过该阈值时启动告警，
	// 提示考虑合并分区或迁移消费组。<=0（默认）不告警。
	ConnectionWarnThreshold int `mapstructure:"connection_warn_threshold"`

	// LagScrapeInterval 消费 lag/offset 指标抓取间隔（见 13 §6.5 / §3.59）。后台周期读取各
	// reader 的 Stats().Lag / Stats().Offset 并经 ConsumerMetrics 暴露到 Prometheus
	// （mq_consumer_lag / mq_consumer_offset），用于观测消费滞后。<=0 时使用默认 15s。
	// 指标未注入（如未启用 Prometheus）时为 no-op，不影响消费。
	LagScrapeInterval time.Duration `mapstructure:"lag_scrape_interval"`

	// Mode 消费模式显式声明（见 13 §6.5 / §3.60「配置收敛」）。
	//   - "direct"（默认）：无消费组 + 按分区直读，规避 kafka-go v0.4.51 与 Kafka 4.0 KRaft
	//     group coordinator 不兼容导致的 -1 Unknown。
	//   - "group"：消费组模式——当前 kafka-go v0.4.51 在 KRaft 4.0 下不可用（coordinator 握手
	//     返回 UNKNOWN_SERVER_ERROR(-1) 且无法自愈），故显式配置为 group 会启动报错（P0 待升级客户端）。
	//   空值回落为 "direct"；其它未知值启动时告警并回落 direct。
	Mode string `mapstructure:"mode"`

	// OrderedCommit 同分区顺序提交（默认 false）。
	//   - false（默认）：每条消息派发到共享 worker 池并发处理，吞吐高；offset 采用「只增不减」推进，
	//     依赖后台周期落盘（默认 5s）；进程在「已处理完、尚未落盘」的窗口崩溃时，该区间消息会被重放
	//     （at-least-once，非 exactly-once）。
	//   - true：每个分区由其 readerLoop 串行处理（拉一条 → 处理完 → 再同步落盘推进 offset），保证
	//     同分区严格有序，并把「已处理未持久化」的崩溃重放窗口缩到最小（每条消息提交即落盘）。
	//     代价是单分区并发降为 1（跨分区仍并行，并发度=分区数）。
	//
	// 重要边界（与「跨分区一致性」相关）：
	//   - 无论 true/false，本机制只保证「分区内」顺序与进度；Kafka 不保证跨分区顺序/原子提交，故
	//     ordered_commit 也不提供任何「跨分区一致性」——不同分区的 readerLoop 之间无协调、无全局顺序。
	//   - 要真正「不丢消息 + 恰好一次」，需把相关事件路由到同一分区（相同 key），并在本实现基础上
	//     注入 ConsumerProgressStore（如 SQLProgressStore，与业务库同实例）以获得跨分区 event_id
	//     幂等去重（消费侧 Outbox/Dedup）。
	//   - 仍非 Kafka 事务级 exactly-once：若 handler 的业务写与 offset 推进需「原子提交」，应由 handler
	//     在同一 DB 事务内提交业务数据并（经注入的 SQLProgressStore）推进 offset，框架本身不隐式提供。
	OrderedCommit bool `mapstructure:"ordered_commit"`
}

// CheckDeadConfig 返回「无消费组 + 按分区直读」模式下已失效（死配置）的告警清单（见 13 §6.5 #5 / §3.55）。
// kafka-go v0.4.51 的 group coordinator 与 Kafka 4.0 KRaft 不兼容，本项目改用无消费组直读，
// 故以下字段在本实现中无任何作用，配置它们纯属误导（如让人误以为 kafka 仍走消费组）：
//   - group_id：无消费组，仅记录不生效；
//   - enable_auto_commit：无消费组、无 auto-commit 概念；
//   - max_poll_records：逐条 FetchMessage，无批量 poll；
//   - batch_trigger_count：逐条处理，无攒批触发。
//
// 注意 batch_trigger_interval 仍生效（映射为 Reader.MaxWait，攒批上界），故不在此列。
// 调用方（NewKafkaConsumer）应在启动时逐个 Warnf 提示，便于收敛配置（P2 配置收敛）。
// 仅当字段被设为「有意义值」时才告警（零值视为未配置，避免误报默认镜像值）。
func (kc *KafkaConsumerConfig) CheckDeadConfig() []string {
	var warns []string
	if kc.GroupID != "" {
		warns = append(warns, fmt.Sprintf(
			"mq.kafka.consumer.group_id=%q is ignored in no-group direct mode (kafka-go v0.4.51 group coordinator is incompatible with Kafka 4.0 KRaft); the value is only recorded, not used. Remove it or migrate to group mode",
			kc.GroupID))
	}
	if kc.EnableAutoCommit {
		warns = append(warns,
			"mq.kafka.consumer.enable_auto_commit=true is ignored in no-group direct mode (no auto-commit concept; offset is managed by the local offset store)")
	}
	if kc.MaxPollRecords > 0 {
		warns = append(warns, fmt.Sprintf(
			"mq.kafka.consumer.max_poll_records=%d is ignored in no-group direct mode (messages are fetched one-by-one via FetchMessage)",
			kc.MaxPollRecords))
	}
	if kc.BatchTriggerCount > 0 {
		warns = append(warns, fmt.Sprintf(
			"mq.kafka.consumer.batch_trigger_count=%d is ignored in no-group direct mode (messages are handled one-by-one)",
			kc.BatchTriggerCount))
	}
	return warns
}

type KafkaTopicsConfig struct {
	UserEvents  string `mapstructure:"user_events"`
	IdleEvents  string `mapstructure:"idle_events"`
	TaskEvents  string `mapstructure:"task_events"`
	ShopEvents  string `mapstructure:"shop_events"`
	UserPoints  string `mapstructure:"user_points"`  // 积分可靠投递（Outbox）事件主题
	CacheEvents string `mapstructure:"cache_events"` // 写降级时投递的缓存失效/刷新事件主题
}

type RabbitMQConfig struct {
	URL string `mapstructure:"url"`
	// SendTimeout 发布确认（publisher confirm）等待超时上界。默认 10s。仅作安全网：broker 关闭
	// channel 会立即返回零值回执（Ack=false），不会真正等到超时；但若确认回执丢失/极慢，超时可避免
	// 持锁永久阻塞、冻结整个生产者。映射代码中的 rabbitSendTimeoutOf。
	SendTimeout time.Duration `mapstructure:"send_timeout"`
	// PoolSize 生产者 channel 池大小（= 异步模型下后台 worker 数）。单连接多 channel 并发发布以提升吞吐。
	// 默认 8。每个 channel 仍串行发布（publisher confirm 要求确认顺序与发布顺序一致），故并发度上限=PoolSize。
	// 映射 rabbitPoolSizeOf。
	PoolSize int `mapstructure:"pool_size"`
	// QueueSize 异步发送队列长度（Send 入队即返回，后台 worker 经 channel 池发布）。<=0 用默认 8192。
	// 队列满时转入溢出池后台发送，溢出池也满则降级到本地 DLQ。
	QueueSize int `mapstructure:"queue_size"`
	// MaxOverflowWorkers 队列满时溢出发送的最大并发 goroutine 数（限流，避免无限起协程）。<=0 用默认 256。
	MaxOverflowWorkers int `mapstructure:"max_overflow_workers"`
	// DLQEnabled Broker 不可达 / 溢出池打满时，降级到本地文件 DLQ（replay 协程在恢复后自动补发）。
	// 默认 false（与 Kafka/RocketMQ 一致：默认开启本地 DLQ 兜底）。
	DLQEnabled bool `mapstructure:"dlq_enabled"`
	// DLQLocalPath 本地 DLQ 目录（JSONL，按 topic 分文件）。
	DLQLocalPath string            `mapstructure:"dlq_local_path"`
	Topics       KafkaTopicsConfig `mapstructure:"topics"` // 事件→主题映射；未配置时回落到 mq.kafka.topics.*
}

type RocketMQConfig struct {
	Endpoints []string `mapstructure:"endpoints"`
	AccessKey string   `mapstructure:"access_key"`
	SecretKey string   `mapstructure:"secret_key"`
	// BrokerAddr 单个 broker 的地址（如 127.0.0.1:10911），用于启动期通过 admin API 显式预建主题。
	// 留空时改用“探针消息”方式（依赖 broker 的 autoCreateTopicEnable）预建；两项都能避免
	// PushConsumer 订阅不存在的主题而在 Start() 直接失败。
	BrokerAddr string `mapstructure:"broker_addr"`
	// SendTimeout 单次同步发送（SendSync）超时。默认 3s。避免 broker 阻塞时 SendSync 无限期挂起，
	// 拖住调用方（结算/下单等）关键路径或 outbox relay 协程。
	SendTimeout time.Duration `mapstructure:"send_timeout"`
	// SendRetries 同步发送失败时客户端内部的重试次数（DefaultProducer 层面）。默认 2。
	// 与消费端服务端 %RETRY% 重试互补：这里覆盖「发送阶段」的瞬时失败（如 broker 短暂不可达）。
	SendRetries int `mapstructure:"send_retries"`
	// QueueSize 异步发送队列长度（Send 入队即返回，后台 loop 经 SendSync 发送）。<=0 用默认 8192。
	// 队列满时转入溢出池后台发送，溢出池也满则降级到本地 DLQ。
	QueueSize int `mapstructure:"queue_size"`
	// MaxOverflowWorkers 队列满时溢出发送的最大并发 goroutine 数（限流，避免无限起协程）。<=0 用默认 256。
	MaxOverflowWorkers int `mapstructure:"max_overflow_workers"`
	// BatchSize 后台 loop 批量发送时每批最多聚合的消息条数（非阻塞凑批，高吞吐时凑满、低吞吐时退化为 1）。
	// 按主题分组后逐主题调用一次 SendSync（RocketMQ 批量发送要求同批同主题），显著减少 broker RTT 次数、
	// 压低发送尾延迟（实测结算 p99 约 10× 于其它 MQ，主因即逐条 SendSync）。<=0 用默认 32。
	// 受 RocketMQ 客户端约束：单批总大小上限 4MB、单条 1MB、最多 4096 条；超限客户端会直接报错，故不宜过大。
	BatchSize int `mapstructure:"batch_size"`
	// DLQEnabled Broker 不可达 / 溢出池打满时，降级到本地文件 DLQ（replay 协程在恢复后自动补发）。
	// 默认 false（与 Kafka 一致：默认开启本地 DLQ 兜底）。
	DLQEnabled bool `mapstructure:"dlq_enabled"`
	// DLQLocalPath 本地 DLQ 目录（JSONL，按 topic 分文件）。
	DLQLocalPath string            `mapstructure:"dlq_local_path"`
	Topics       KafkaTopicsConfig `mapstructure:"topics"` // 事件→主题映射；未配置时回落到 mq.kafka.topics.*
}

// AuthConfig 认证配置
type AuthConfig struct {
	JWT      JWTConfig      `mapstructure:"jwt"`
	Password PasswordConfig `mapstructure:"password"`
}

type JWTConfig struct {
	Algorithm      string        `mapstructure:"algorithm"`
	AccessTTL      time.Duration `mapstructure:"access_ttl"`
	RefreshTTL     time.Duration `mapstructure:"refresh_ttl"`
	Issuer         string        `mapstructure:"issuer"`
	PrivateKeyPath string        `mapstructure:"private_key_path"`
	PublicKeyPath  string        `mapstructure:"public_key_path"`
	SigningKey     string        `mapstructure:"signing_key"`
}

type PasswordConfig struct {
	BcryptCost        int  `mapstructure:"bcrypt_cost"`
	MinLength         int  `mapstructure:"min_length"`
	RequireUpper      bool `mapstructure:"require_upper"`
	RequireLower      bool `mapstructure:"require_lower"`
	RequireDigit      bool `mapstructure:"require_digit"`
	RequireSpecial    bool `mapstructure:"require_special"`
	RequireCategories int  `mapstructure:"require_categories"`
}

// OAuthConfig OAuth 配置
type OAuthConfig struct {
	Google GoogleOAuthConfig `mapstructure:"google"`
}

type GoogleOAuthConfig struct {
	ClientID     string   `mapstructure:"client_id"`
	ClientSecret string   `mapstructure:"client_secret"`
	RedirectURL  string   `mapstructure:"redirect_url"`
	Scopes       []string `mapstructure:"scopes"`
}

// CaptchaConfig 验证码配置
type CaptchaConfig struct {
	Type         string               `mapstructure:"type"`
	Tencent      TencentCaptchaConfig `mapstructure:"tencent"`
	Turnstile    TurnstileConfig      `mapstructure:"turnstile"`
	ReCAPTCHA    ReCAPTCHAConfig      `mapstructure:"recaptcha"`
	HCAPTCHA     HCAPTCHAConfig       `mapstructure:"hcaptcha"`
	Trigger      CaptchaTriggerConfig `mapstructure:"trigger"`
	WhitelistTTL time.Duration        `mapstructure:"whitelist_ttl"`
}

type TencentCaptchaConfig struct {
	AppID     string `mapstructure:"app_id"`
	SecretKey string `mapstructure:"secret_key"`
}

type TurnstileConfig struct {
	SiteKey   string `mapstructure:"site_key"`
	SecretKey string `mapstructure:"secret_key"`
}

type ReCAPTCHAConfig struct {
	SiteKey   string `mapstructure:"site_key"`
	SecretKey string `mapstructure:"secret_key"`
}

type HCAPTCHAConfig struct {
	SiteKey   string `mapstructure:"site_key"`
	SecretKey string `mapstructure:"secret_key"`
}

type CaptchaTriggerConfig struct {
	EmailPerMinute int `mapstructure:"email_per_minute"`
	IPPerMinute    int `mapstructure:"ip_per_minute"`
}

// SecurityConfig 安全配置
type SecurityConfig struct {
	IPLimit               IPLimitConfig     `mapstructure:"ip_limit"`
	AccountLock           AccountLockConfig `mapstructure:"account_lock"`
	LoginFailDelays       []int             `mapstructure:"login_fail_delays"`
	TokenBlacklistEnabled bool              `mapstructure:"token_blacklist_enabled"`
}

type IPLimitConfig struct {
	RegisterPerMinute int `mapstructure:"register_per_minute"`
	LoginPerMinute    int `mapstructure:"login_per_minute"`
}

type AccountLockConfig struct {
	MaxFailures  int           `mapstructure:"max_failures"`
	LockDuration time.Duration `mapstructure:"lock_duration"`
}

// RateLimitConfig 限流配置
type RateLimitConfig struct {
	Enabled   bool            `mapstructure:"enabled"`
	Global    RateLimitBucket `mapstructure:"global"`
	PerUser   RateLimitBucket `mapstructure:"per_user"`
	PerIP     RateLimitBucket `mapstructure:"per_ip"`
	Whitelist []string        `mapstructure:"whitelist"`
	// ExemptPaths 豁免限流的请求路径前缀（含全局/用户/IP 三层）。
	// 用于保护心跳等“在线判活命脉”接口：被限流返回 429 会直接误判设备离线，
	// 故心跳不应计入系统级全局配额。前缀匹配，如 ["/api/v1/idle/heartbeat"]。
	ExemptPaths []string `mapstructure:"exempt_paths"`
}

type RateLimitBucket struct {
	Enabled bool `mapstructure:"enabled"`
	Rate    int  `mapstructure:"rate"`
	Burst   int  `mapstructure:"burst"`
}

// CircuitBreakerConfig 熔断器配置
type CircuitBreakerConfig struct {
	Enabled             bool          `mapstructure:"enabled"`
	FailureThreshold    float64       `mapstructure:"failure_threshold"`
	OpenDuration        time.Duration `mapstructure:"open_duration"`
	HalfOpenMaxRequests int           `mapstructure:"half_open_max_requests"`
	Interval            time.Duration `mapstructure:"interval"`
	Timeout             time.Duration `mapstructure:"timeout"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	Level              string   `mapstructure:"level"`
	Format             string   `mapstructure:"format"`
	Output             string   `mapstructure:"output"`
	FilePath           string   `mapstructure:"file_path"`
	MaxSizeMB          int      `mapstructure:"max_size_mb"`
	MaxBackups         int      `mapstructure:"max_backups"`
	MaxAgeDays         int      `mapstructure:"max_age_days"`
	Compress           bool     `mapstructure:"compress"`
	RequestBodyMaxSize int      `mapstructure:"request_body_max_size"`
	SensitiveFields    []string `mapstructure:"sensitive_fields"`
}

// TracingConfig 链路追踪配置
type TracingConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	ServiceName string        `mapstructure:"service_name"`
	Exporter    string        `mapstructure:"exporter"`
	OTLP        OTLPConfig    `mapstructure:"otlp"`
	Sampler     SamplerConfig `mapstructure:"sampler"`
}

type OTLPConfig struct {
	Endpoint string `mapstructure:"endpoint"`
	Insecure bool   `mapstructure:"insecure"`
}

type SamplerConfig struct {
	Type      string  `mapstructure:"type"`
	Rate      float64 `mapstructure:"rate"`
	ErrorRate float64 `mapstructure:"error_rate"`
}

// MetricsConfig 指标配置
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

// HealthConfig 健康检查配置
type HealthConfig struct {
	Enabled    bool               `mapstructure:"enabled"`
	HealthPath string             `mapstructure:"health_path"`
	ReadyPath  string             `mapstructure:"ready_path"`
	Checks     HealthChecksConfig `mapstructure:"checks"`
}

type HealthChecksConfig struct {
	Redis    bool `mapstructure:"redis"`
	Database bool `mapstructure:"database"`
	Kafka    bool `mapstructure:"kafka"`
}

// TaskConfig 任务系统配置
type TaskConfig struct {
	DailyResetHour  int    `mapstructure:"daily_reset_hour"`
	WeeklyResetDay  string `mapstructure:"weekly_reset_day"`
	WeeklyResetHour int    `mapstructure:"weekly_reset_hour"`
}

// IdleConfig 挂机系统配置
type IdleConfig struct {
	HeartbeatInterval    time.Duration `mapstructure:"heartbeat_interval"`
	TimeoutThreshold     time.Duration `mapstructure:"timeout_threshold"`
	PointsPerMinute      int64         `mapstructure:"points_per_minute"`
	DailyPointsLimit     int64         `mapstructure:"daily_points_limit"`
	MaxDevices           int           `mapstructure:"max_devices"`
	RedisKeyPrefix       string        `mapstructure:"redis_key_prefix"`
	OfflineCheckInterval string        `mapstructure:"offline_check_interval"`
	// ActiveSetShards 活跃会话集合的分片数（离线检测 scanner 遍历用）。
	// 集群模式下分片可将大集合打散到多个 slot/节点，避免单一大 key 成为热点；
	// 单机模式下无害（仅 256 个小集合）。<=0 时退化为单集合（idle:active:set:0）。
	ActiveSetShards int `mapstructure:"active_set_shards"`
	// EventDrivenSettle 启用 Keyspace Notification 事件驱动结算（心跳 key 过期即结算，零轮询）。
	// 仅 Redis 单机/哨兵模式可靠；集群模式（cache.l2.cluster.enabled=true）下自动禁用并保留轮询 scanner 兜底。
	EventDrivenSettle bool `mapstructure:"event_driven_settle"`
	// HeartbeatPersistEnabled 启用心跳时间批量落库（持久化 last_heartbeat_at）。
	// 心跳已去 DB 化（判活仅写 Redis 标记），但若需保留 last_heartbeat_at（离线分析、重启后近似心跳等），
	// 启用此项：以「每 Pod 本地聚合 + 定时批量 flush」替代「逐次 UPDATE」，将 DB 写从「每次心跳一次」降为「每 interval 一次」。
	// 默认关闭（完全去 DB 化）；降级路径（Redis 不可用）在未启用时回退为旧逐次 UPDATE 兜底。
	HeartbeatPersistEnabled bool `mapstructure:"heartbeat_persist_enabled"`
	// HeartbeatPersistInterval 批量 flush 间隔（建议 5~10s，默认 10s）。
	HeartbeatPersistInterval time.Duration `mapstructure:"heartbeat_persist_interval"`
	// ScanSharding 离线检测 scanner 按 Pod 分片：将活跃会话集合的扫描负载分散到多个 Pod，
	// 消除“每副本全量扫描”的 O(K×N) 冗余（K=副本数，N=活跃会话数）。
	// 依赖 active_set_shards（默认 256）已将活跃集合打散为多个小集合；本配置决定每个 Pod
	// 负责其中哪些集合分片（按 i % pod_total == pod_index 取模分配）。
	// pod_index / pod_total 可由环境变量 HC_POD_INDEX / HC_POD_TOTAL 覆盖（适配 StatefulSet/Deployment 滚动发布）。
	// 未启用时退化为全量扫描（单实例或双实例场景保持原有行为）。
	ScanSharding ScanShardingConfig `mapstructure:"scan_sharding"`
	// ScanMaxSettlePerCycle 离线检测单周期最大结算量（防止突发离线导致 scanner 长阻塞）。
	// >0 时超出预算的离线成员留待下个周期处理；<=0 表示不限制（默认 0）。
	ScanMaxSettlePerCycle int `mapstructure:"scan_max_settle_per_cycle"`
	// ScanSettleWorkers 离线检测并发结算的 worker 数（有界 goroutine pool）。<=0 时默认 32。
	ScanSettleWorkers int `mapstructure:"scan_settle_workers"`
	// LeaderElection 选主：多副本部署下，仅 Leader 执行“恰好一次”类任务（回填、每日/每周任务重置、
	// 事件去重清理、Keyspace 通知配置等），避免重复执行。scanner 分片模式下各 Pod 扫描自身分片、
	// 不依赖选主；选主仅用于上述单例任务。L2(Redis) 不可用时自动降级为“每 Pod 各自执行”（乐观锁兜底）。
	LeaderElection LeaderElectionConfig `mapstructure:"leader_election"`
}

// ScanShardingConfig 离线检测扫描分片配置
type ScanShardingConfig struct {
	Enabled  bool `mapstructure:"enabled"`
	PodIndex int  `mapstructure:"pod_index"` // 本 Pod 分片序号（覆盖环境变量 HC_POD_INDEX）
	PodTotal int  `mapstructure:"pod_total"` // 分片总数（覆盖环境变量 HC_POD_TOTAL），<=1 退化为全量
	// Failover 死分片接管：依赖 Leader 选举（leader_election.enabled）。
	// 开启后，各 Pod 周期性向 Redis 上报存活（idle:pod-alive:{podIndex}，短 TTL）；
	// Leader 发现某 Pod 失活（其存活 Key 过期）即接管该 Pod 的全部集合分片，避免静态分片下
	// “Pod 宕机 → 其分片永不扫描”的可用性缺口。未启用 Leader 选举时本开关无效（保持静态分片语义）。
	Failover bool `mapstructure:"failover"`
	// PodDiscovery 副本数（pod_total）的发现方式：
	//   "env"（默认）：pod_index/pod_total 由配置或环境变量 HC_POD_INDEX/HC_POD_TOTAL 提供（需随 replicas 手动同步）。
	//   "headless"：pod_total 通过解析 Headless Service 的 DNS A 记录（就绪 Pod 数）动态获得，
	//       pod_index 优先取 HC_POD_INDEX，否则从 POD_NAME/HOSTNAME 提取 StatefulSet ordinal。
	//       适配 HPA 自动扩缩：扩容后各 Pod 经 DNS 自动感知新副本总数，无需手改配置；
	//       缩容时的分片归属缺口由“死分片接管（failover）”兜底。
	PodDiscovery string `mapstructure:"pod_discovery"`
	// HeadlessService Headless Service 名称（pod_discovery=headless 时必填），用于 DNS 解析副本数。
	// 解析时使用 FQDN：<name>.<POD_NAMESPACE>.svc.cluster.local（POD_NAMESPACE 取环境变量，缺省 default）。
	HeadlessService string `mapstructure:"headless_service"`
}

// LeaderElectionConfig 选主配置
type LeaderElectionConfig struct {
	Enabled bool          `mapstructure:"enabled"`
	LockKey string        `mapstructure:"lock_key"` // 选主锁 Key（默认 idle:leader）
	TTL     time.Duration `mapstructure:"ttl"`      // 领导租约时长（默认 15s）
	Renewal time.Duration `mapstructure:"renewal"`  // 续期间隔（默认 TTL/2）
}

// HeartbeatPersistIntervalOrDefault 返回批量 flush 间隔，<=0 时回退为 10s。
func (c IdleConfig) HeartbeatPersistIntervalOrDefault() time.Duration {
	if c.HeartbeatPersistInterval <= 0 {
		return 10 * time.Second
	}
	return c.HeartbeatPersistInterval
}

// MigrationConfig 迁移配置
type MigrationConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	AutoMigrate bool   `mapstructure:"auto_migrate"`
	Path        string `mapstructure:"path"`
}

// MailConfig 邮件（注册确认等事务邮件）配置。
// 由 pkg/mail 消费；Enabled=false 时 AuthService 跳过发信，不影响注册主流程。
type MailConfig struct {
	Enabled  bool           `mapstructure:"enabled"` // 总开关（默认 false，未配置不发信）
	SMTP     MailSMTPConfig `mapstructure:"smtp"`
	SiteName string         `mapstructure:"site_name"` // 邮件中展示的站点名
}

// MailSMTPConfig SMTP 客户端配置（字段含义见 pkg/mail.SMTPConfig）。
type MailSMTPConfig struct {
	Host               string `mapstructure:"host"`
	Port               int    `mapstructure:"port"`
	Username           string `mapstructure:"username"`
	Password           string `mapstructure:"password"`
	From               string `mapstructure:"from"`
	FromName           string `mapstructure:"from_name"`
	UseTLS             bool   `mapstructure:"use_tls"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// Load 加载配置，优先级：文件默认值 → 环境变量覆盖
func Load(configPath string) (*Config, error) {
	cfg, err := loadFromFile(configPath)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config invalid: %w", err)
	}
	return cfg, nil
}

// loadFromFile 读取并解析配置文件（不含校验），供 Load 与 Manager 复用。
func loadFromFile(configPath string) (*Config, error) {
	v := viper.New()

	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// 环境变量覆盖
	v.SetEnvPrefix("APP")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config failed: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config failed: %w", err)
	}

	// 应用 Kafka 生产者默认值（与 NewKafkaProducer 的下限兜底一致）：未配置（<=0）时回退默认
	// 8192 / 256，确保部署不写这些字段也能获得合理的队列与溢出缓冲。注意此处仅对 <=0 兜底，
	// 显式配置正值（含 MaxOverflowWorkers=0 表示「禁用溢出池」）不会被静默覆盖。
	if cfg.MQ.Kafka.Producer.QueueSize <= 0 {
		cfg.MQ.Kafka.Producer.QueueSize = 8192
	}
	if cfg.MQ.Kafka.Producer.MaxOverflowWorkers <= 0 {
		cfg.MQ.Kafka.Producer.MaxOverflowWorkers = 256
	}

	return &cfg, nil
}

// Validate 校验配置基本可用性（必填项与取值范围）。
// 用于启动加载与运行时热更新：校验失败时，热更新保留旧配置不替换，启动则直接报错退出。
func (c *Config) Validate() error {
	// 未配置 driver（空字符串）按原语义回退 SQLite：空值属于「未配置」而非「误配」，不应被下方
	// 白名单判为非法，否则会破坏「未配置即默认 sqlite」的既有契约（Kafka/Manager 等不关心 DB 的
	// 最小配置测试也会触发校验失败）。仅「显式配置了 driver」才走白名单严格校验。
	if c.Database.Business.Driver == "" {
		c.Database.Business.Driver = "sqlite"
	}
	if c.Database.User.Driver == "" {
		c.Database.User.Driver = "sqlite"
	}
	if c.Database.Monitor.Driver == "" {
		c.Database.Monitor.Driver = "sqlite"
	}
	if c.Database.Log.Driver == "" {
		c.Database.Log.Driver = "sqlite"
	}

	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be in (0, 65535], got %d", c.Server.Port)
	}
	if c.Server.Mode == "" {
		return fmt.Errorf("server.mode must not be empty")
	}
	if c.Server.MaxHeaderBytes < 0 {
		return fmt.Errorf("server.max_header_bytes must be >= 0")
	}
	if c.Server.MaxConnsPerIP < 0 {
		return fmt.Errorf("server.max_conns_per_ip must be >= 0")
	}
	if c.Server.MaxConns < 0 {
		return fmt.Errorf("server.max_conns must be >= 0")
	}
	if c.Server.ConcurrencyLimit < 0 {
		return fmt.Errorf("server.concurrency_limit must be >= 0")
	}

	// 数据库驱动合法性校验：避免误配 driver 静默回退 SQLite 而掩盖配置错误。
	// 生产用 PostgreSQL/MongoDB/ClickHouse/Elasticsearch，测试用 MySQL/SQLite（见需求）。
	if !contains([]string{"mysql", "postgres", "sqlite"}, c.Database.Business.Driver) {
		return fmt.Errorf("database.business.driver must be one of mysql/postgres/sqlite, got %q", c.Database.Business.Driver)
	}
	if !contains([]string{"mongodb", "mysql", "sqlite"}, c.Database.User.Driver) {
		return fmt.Errorf("database.user.driver must be one of mongodb/mysql/sqlite, got %q", c.Database.User.Driver)
	}
	if !contains([]string{"clickhouse", "mysql", "sqlite"}, c.Database.Monitor.Driver) {
		return fmt.Errorf("database.monitor.driver must be one of clickhouse/mysql/sqlite, got %q", c.Database.Monitor.Driver)
	}
	if !contains([]string{"elasticsearch", "mysql", "sqlite"}, c.Database.Log.Driver) {
		return fmt.Errorf("database.log.driver must be one of elasticsearch/mysql/sqlite, got %q", c.Database.Log.Driver)
	}
	// 选 mysql 驱动时必须给出主库 DSN：否则会静默回退到空/sqlite 而掩盖配置错误。
	if c.Database.Business.Driver == "mysql" && c.Database.Business.MySQL.Master == "" {
		return fmt.Errorf("database.business.mysql.master must be set when driver=mysql")
	}
	if c.Database.Business.Driver == "postgres" && c.Database.Business.Postgres.Master == "" {
		return fmt.Errorf("database.business.postgres.master must be set when driver=postgres")
	}
	if c.Database.User.Driver == "mysql" && c.Database.User.MySQL.Master == "" {
		return fmt.Errorf("database.user.mysql.master must be set when driver=mysql")
	}
	if c.Database.Monitor.Driver == "mysql" && c.Database.Monitor.MySQL.Master == "" {
		return fmt.Errorf("database.monitor.mysql.master must be set when driver=mysql")
	}
	if c.Database.Log.Driver == "mysql" && c.Database.Log.MySQL.Master == "" {
		return fmt.Errorf("database.log.mysql.master must be set when driver=mysql")
	}

	// 超时关系合理性（仅当两侧均显式设置时校验；零值视为未配置/测试环境）
	if c.Server.IdleTimeout > 0 && c.Idle.TimeoutThreshold > 0 && c.Server.IdleTimeout < c.Idle.TimeoutThreshold {
		return fmt.Errorf("server.idle_timeout (%v) must be >= idle.timeout_threshold (%v) to avoid closing heartbeat connections",
			c.Server.IdleTimeout, c.Idle.TimeoutThreshold)
	}

	// 认证：JWT 签名密钥在 HS256 模式下必填
	if c.Auth.JWT.Algorithm == "HS256" && c.Auth.JWT.SigningKey == "" {
		return fmt.Errorf("auth.jwt.signing_key must not be empty for HS256 algorithm")
	}

	// 安全：账号锁定的阈值/时长合理性（仅当显式配置时校验）
	if c.Security.AccountLock.MaxFailures < 0 {
		return fmt.Errorf("security.account_lock.max_failures must be >= 0, got %d", c.Security.AccountLock.MaxFailures)
	}

	// 限流：启用时速率不可为负
	if c.RateLimit.Enabled && c.RateLimit.Global.Rate < 0 {
		return fmt.Errorf("ratelimit.global.rate must be >= 0")
	}

	// 熔断器：阈值范围检查（仅当启用且显式配置时）
	if c.CircuitBreaker.Enabled {
		if c.CircuitBreaker.FailureThreshold < 0 || c.CircuitBreaker.FailureThreshold > 1 {
			return fmt.Errorf("circuit_breaker.failure_threshold must be in [0, 1]")
		}
	}

	// 挂机：心跳超时必须大于心跳间隔（仅当两侧均显式设置时）
	if c.Idle.TimeoutThreshold > 0 && c.Idle.HeartbeatInterval > 0 && c.Idle.TimeoutThreshold < c.Idle.HeartbeatInterval {
		return fmt.Errorf("idle.timeout_threshold (%v) must be >= heartbeat_interval (%v)",
			c.Idle.TimeoutThreshold, c.Idle.HeartbeatInterval)
	}

	// Kafka 无消费组多实例分区分配（见 13 §6.5 #1 / §3.53）：InstanceCount>1 时 InstanceID 必须落在
	// [0, InstanceCount)。越界会让运行期 assignPartitions 退化全量消费，多实例部署下造成分区被重复处理
	// （比「静默不消费」更糟）。在此启动期 / 热更新期拦截，避免隐蔽的重复消费。
	if c.MQ.Type == "kafka" && c.MQ.Kafka.Consumer.InstanceCount > 1 {
		id, ic := c.MQ.Kafka.Consumer.InstanceID, c.MQ.Kafka.Consumer.InstanceCount
		if id < 0 || id >= ic {
			return fmt.Errorf("mq.kafka.consumer.instance_id (%d) must be in [0, %d) when instance_count=%d",
				id, ic, ic)
		}
	}

	return nil
}

// contains 判断 s 是否在 list 中（用于驱动白名单校验）。
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
