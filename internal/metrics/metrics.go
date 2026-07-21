// Package metrics 指标采集与 Prometheus 暴露。
package metrics

import (
	"database/sql"
	"net/http"
	"runtime"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/mq"
)

// Registry 是框架独立的 Prometheus 注册表，与默认 registry 隔离，避免第三方依赖重复注册冲突。
var Registry = prometheus.NewRegistry()

// 基础指标（HTTP / 限流 / 熔断），在 init 中注册一次。
var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests handled, partitioned by method, path and status code.",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	RateLimitTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_total",
			Help: "Total number of rate limit decisions, partitioned by bucket type and result.",
		},
		[]string{"type", "result"}, // type: global|per_user|per_ip; result: allowed|rejected
	)

	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Current circuit breaker state (0=closed, 1=half-open, 2=open), partitioned by route.",
		},
		[]string{"route"},
	)

	CircuitBreakerTransitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "circuit_breaker_transitions_total",
			Help: "Total number of circuit breaker state transitions, partitioned by route and from/to state.",
		},
		[]string{"route", "from", "to"},
	)

	// 应用层有界并发 / 负载卸载（load shedding）指标（高并发高可用加固，见 20）。
	// 在「连接级上限(MaxConns)」与「令牌桶限流(RateLimit)」之后，对进入业务层的在途请求数再设一道
	// 进程级上限：达到上限即快速失败返回 503，避免无界 goroutine 在 DB/下游抖动时堆积压垮后端。
	HTTPConcurrencyInFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_concurrency_in_flight",
			Help: "Current number of in-flight requests constrained by the application-level concurrency limiter.",
		},
	)
	HTTPConcurrencyRejectedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "http_concurrency_rejected_total",
			Help: "Total number of requests rejected (HTTP 503) because the application-level concurrency limit was reached (load shedding).",
		},
	)

	// 挂机系统容量指标（百万设备场景关键观测项）
	IdleScanDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "idle_scan_duration_seconds",
			Help:    "Offline detection scan duration in seconds.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		},
	)
	IdleScanMembers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "idle_scan_members_total",
			Help: "Number of active sessions scanned in the last offline detection cycle.",
		},
	)
	IdleScanDead = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "idle_scan_dead_total",
			Help: "Number of dead (heartbeat-expired) sessions found in the last offline detection cycle.",
		},
	)
	IdleSettleTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "idle_settle_total",
			Help: "Total idle session settlements, partitioned by reason (timeout/completed/skipped).",
		},
		[]string{"reason"}, // timeout | completed | skipped
	)

	// 仓储层关键 best-effort 操作失败计数（DB 冷启动回源 / Redis 计数累加等）。
	// 这些操作失败仅记日志不向上抛错（不影响结算主流程），但长期失败说明 L2 Redis 或
	// DB 抖动，需可观测——否则只能翻日志抽样。标签 operation 区分具体操作。
	IdleRepoErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "idle_repo_errors_total",
			Help: "Total idle repository best-effort operation failures (DB fallback / Redis counter prewarm), partitioned by operation. Failures are non-fatal (logged, not propagated) but indicate L2/DB instability.",
		},
		[]string{"operation"}, // get_daily_points_db | read_daily_points_summary | acquire_prewarm
	)

	// MQ 生产者发送计数（统一各适配器口径），用于发送失败率告警与容量观测。
	MQProducerSendTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mq_producer_send_total",
			Help: "Total MQ producer sends, partitioned by mq type, topic and result (success/failure).",
		},
		[]string{"type", "topic", "result"}, // type: kafka|rabbitmq|rocketmq|memory|none; result: success|failure
	)

	// MQ 生产者降级到本地 DLQ 计数（broker 不可达 / 溢出池打满时落盘，replay 协程补发，不丢消息）。
	MQProducerDLQTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mq_producer_dlq_total",
			Help: "Total MQ producer sends degraded to local DLQ (broker unreachable/overflow), partitioned by mq type and topic. Replayed on recovery, not lost.",
		},
		[]string{"type", "topic"}, // type: kafka|rabbitmq
	)

	// MQ 生产者真正丢弃计数（内存缓冲满 / 总线禁用 no-op，无法补发）。
	MQProducerDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mq_producer_dropped_total",
			Help: "Total MQ producer events actually dropped (cannot be replayed): memory buffer full or bus disabled (mq.type=none). Partitioned by mq type and topic.",
		},
		[]string{"type", "topic"}, // type: memory|none
	)

	// MQ 生产者 DLQ 当前积压（待 replay 补发的消息数，gauge）。broker 不可达 / 过载时上升，
	// 恢复补发后回落；持续 >0 是「DLQ 未清空」的直接告警信号。按 type/topic 切分。
	MQDLQBacklog = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mq_dlq_backlog",
			Help: "Current number of messages awaiting DLQ replay (not yet redelivered), partitioned by mq type and topic. Rises when broker unreachable/overloaded, falls as replay succeeds.",
		},
		[]string{"type", "topic"}, // type: kafka|rabbitmq|rocketmq
	)

	// 消费侧：成功处理的消费消息计数（result: processed|dedup_skipped|dlq）。
	MQConsumerMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mq_consumer_messages_total",
			Help: "Total messages consumed, partitioned by mq type, topic and result (processed/dedup_skipped/dlq).",
		},
		[]string{"type", "topic", "result"}, // type: kafka|rabbitmq|rocketmq|memory; result: processed|dedup_skipped|dlq
	)

	// 消费侧：本地 DLQ 当前存量（gauge）。与生产者 mq_dlq_backlog 不同——消费者 DLQ（毒消息 /
	// 重试耗尽落盘）无自动 replay，属「存档」语义：本进程内只随落盘增长，进程启动时由扫描已有
	// DLQ 文件行数初始化，反映磁盘真实存量（人工清理文件后重启归零）。持续 >0 = 有毒消息待人工处理。
	MQConsumerDLQBacklog = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mq_consumer_dlq_backlog",
			Help: "Current number of messages archived in the consumer-side local DLQ (poison/retry-exhausted, no auto replay), partitioned by mq type and topic. Grows on append; initialized from on-disk file line counts at startup. Persistently >0 means poison messages await manual handling.",
		},
		[]string{"type", "topic"}, // type: kafka
	)

	// 消费侧：partition reader 拉取失败计数（broker 瞬时错误 / 不可用）。
	MQConsumerFetchErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mq_consumer_fetch_errors_total",
			Help: "Total partition reader fetch errors (broker transient errors/unavailable), partitioned by mq type and topic.",
		},
		[]string{"type", "topic"}, // type: kafka|rabbitmq|rocketmq|memory
	)

	// 消费侧：消费 lag（落后于 partition 末尾的消息数），按 topic/partition 切分。
	// 经 kafka.Reader.Stats().Lag 周期暴露，用于观测消费滞后（见 13 §6.5 / §3.59）。
	MQConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mq_consumer_lag",
			Help: "Consumer lag (messages behind partition tail) per topic/partition, partitioned by mq type. Updated periodically from the reader stats.",
		},
		[]string{"type", "topic", "partition"},
	)

	// 消费侧：已消费到的 offset（reader 最后读到的 offset），按 topic/partition 切分。
	MQConsumerOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mq_consumer_offset",
			Help: "Last consumed offset (reader's last-read offset) per topic/partition, partitioned by mq type. Updated periodically from the reader stats.",
		},
		[]string{"type", "topic", "partition"},
	)

	// 调度器任务 panic 计数（scheduler 内部 recover 后上报，避免持续 panic 只能翻日志发现）。
	// 标签 job 取任务名（idle-timeout-scan / daily-task-reset / weekly-task-reset / event-dedup-cleanup）。
	SchedulerJobPanicsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_job_panics_total",
			Help: "Total scheduler job panics recovered (per job name), partitioned by job name.",
		},
		[]string{"job"},
	)

	// 调度器任务执行失败计数（任务闭包返回 error 后上报，与 panic 区分：失败是预期内的错误返回，
	// panic 是非预期崩溃）。持续上升表示对应周期任务底层的 DB/Redis 等依赖持续不可用。
	SchedulerJobFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_job_failures_total",
			Help: "Total scheduler job executions that returned an error (per job name), partitioned by job name.",
		},
		[]string{"job"},
	)

	// 链路追踪 span 被静默丢弃计数（OTLP 导出器队列满时丢弃，collector 不可达/背压时累积）。
	// 与 exporter.Dropped() 同源，暴露到 Prometheus 以便配置「丢 span」告警。
	TraceSpansDroppedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "trace_spans_dropped_total",
			Help: "Total tracing spans dropped (OTLP exporter queue full / collector unreachable), accumulated across the process.",
		},
	)

	// 定时抢购兑换结果计数（高并发场景关键观测项）：按 result 区分
	// success（入库成功）/ sold_out（超出限量直接拒绝不入库）/ user_limit（超出每人限购）/
	// not_started / ended / timeout（写事务 deadline 超时，已回滚）/ other。sold_out 与
	// user_limit 的占比直接反映「削峰拦截」效果；timeout 持续抬升提示 DB 连接池/行锁成为
	// 瓶颈（对应业务码 10309）；other 持续抬升提示 DB/缓存/积分等下游异常。
	FlashSaleRedeemTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shop_flash_redeem_total",
			Help: "Total flash sale redeem attempts, partitioned by result (success/sold_out/user_limit/not_started/ended/timeout/other).",
		},
		[]string{"result"},
	)

	// 定时抢购「处理超时」独立容量告警指标：写事务等待行锁/连接池超过 request_timeout deadline
	// 被取消（context.DeadlineExceeded）时累加。该错误必伴随事务回滚、不落库，客户端可安全
	// 重试（不超卖），但持续 >0 是「DB 连接池/行锁饱和」的直接信号。与
	// FlashSaleRedeemTotal{result="timeout"} 同源，单独暴露便于配置独立告警阈值
	// （如 rate(shop_flash_redeem_timeout_total[1m]) 突增、或 timeout 占总抢购比超阈值即告警），
	// 不被 other 标签淹没。对应业务码 10309。
	FlashSaleRedeemTimeoutTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "shop_flash_redeem_timeout_total",
			Help: "Total flash sale redeem attempts that timed out (context.DeadlineExceeded on the write transaction, rolled back safely). Directly indicates DB connection-pool / row-lock saturation under spike. Business code 10309.",
		},
	)

	// 定时抢购 Redis 库存预热/对账计数：result = warmed（已预热/已对齐）| skipped（L2 不可用跳过）| error。
	FlashSaleWarmupTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shop_flash_warmup_total",
			Help: "Total flash sale stock warmup/reconcile operations, partitioned by result (warmed/skipped/error).",
		},
		[]string{"result"},
	)

	// 当前进行中的抢购活动数（gauge），用于容量与活动生命周期观测。
	FlashSaleActiveActivities = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "shop_flash_active_activities",
			Help: "Current number of active (in-window) flash sale activities.",
		},
	)

	// 普通商品兑换结果计数（可观测性加固，对应 P1/P2）：区分削峰层售罄(sold_out_peak) 与
	// DB 行锁售罄(sold_out_db)、并发冲突(concurrent)。sold_out_peak 与 sold_out_db 的占比
	// 直接反映「削峰拦截」效果（此前普通兑换 149967 次失败全为 10302，无法区分拦截层与 DB 兜底）；
	// concurrent 持续 >0 提示同一用户并发兑换被锁拦截（码 10310，可重试，非故障）。
	RedeemTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shop_redeem_total",
			Help: "Total normal redeem attempts, partitioned by result (success/sold_out_peak/sold_out_db/points_insufficient/concurrent/item_offline/other).",
		},
		[]string{"result"},
	)

	// hc_build_info 暴露构建信息（version/commit 由 SetBuildInfo 注入，默认 unknown），
	// 便于监控中识别当前部署版本、按版本分组、检测发版。值恒为 1。
	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hc_build_info",
			Help: "hc-framework build information (version/commit/goversion). Value is always 1.",
		},
		[]string{"version", "commit", "goversion"},
	)
)

// appVersion / appCommit 由启动早期通过 SetBuildInfo 注入（如 ldflags / 配置）；
// 未注入时默认为 unknown，hc_build_info 仍会暴露 goversion。
var (
	appVersion = "unknown"
	appCommit  = "unknown"
)

// SetBuildInfo 注入应用版本信息到 hc_build_info 指标（幂等，重复调用以最后一次为准）。
// 先 Reset 再 Set，确保 init 阶段注册的默认 "unknown" 序列被清除，避免指标里残留陈旧版本标签。
func SetBuildInfo(version, commit string) {
	if version != "" {
		appVersion = version
	}
	if commit != "" {
		appCommit = commit
	}
	BuildInfo.Reset()
	BuildInfo.WithLabelValues(appVersion, appCommit, runtime.Version()).Set(1)
}

func init() {
	Registry.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDuration,
		RateLimitTotal,
		CircuitBreakerState,
		CircuitBreakerTransitionsTotal,
		IdleScanDuration,
		IdleScanMembers,
		IdleScanDead,
		IdleSettleTotal,
		IdleRepoErrorsTotal,
		MQProducerSendTotal,
		MQProducerDLQTotal,
		MQProducerDroppedTotal,
		MQDLQBacklog,
		MQConsumerMessagesTotal,
		MQConsumerDLQBacklog,
		MQConsumerFetchErrorsTotal,
		MQConsumerLag,
		MQConsumerOffset,
		SchedulerJobPanicsTotal,
		SchedulerJobFailuresTotal,
		TraceSpansDroppedTotal,
		HTTPConcurrencyInFlight,
		HTTPConcurrencyRejectedTotal,
		FlashSaleRedeemTotal,
		FlashSaleRedeemTimeoutTotal,
		FlashSaleWarmupTotal,
		FlashSaleActiveActivities,
		RedeemTotal,
		BuildInfo,
		// 运行时指标：goroutine 数、GC/堆内存（go_*）、进程 CPU / 常驻内存 / 打开 FD（process_*）。
		// 原自定义 Registry 缺这些指标，运维无法观测 goroutine 泄漏、内存增长、FD 耗尽等。
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		// DB 连接池指标：由 bootstrap.InitDatabases 调用 RegisterDBPool 注入各库 *sql.DB，
		// 每次 scrape 实时读取 db.Stats()，观测连接池饱和（抢购 10309 容量根因）。
		dbPoolCollector{},
	)
	// 暴露构建信息（goversion 始终可用；version/commit 默认 unknown，可由 SetBuildInfo 覆盖）
	SetBuildInfo(appVersion, appCommit)
}

// cacheCollector 动态采集缓存统计（每次 scrape 实时读取，避免主动轮询更新）。
type cacheCollector struct {
	mgr *cache.Manager
}

var (
	cacheL1HitsTotal      = prometheus.NewDesc("cache_l1_hits_total", "Cumulative L1 cache hits.", nil, nil)
	cacheL1MissesTotal    = prometheus.NewDesc("cache_l1_misses_total", "Cumulative L1 cache misses.", nil, nil)
	cacheL1HitRatio       = prometheus.NewDesc("cache_l1_hit_ratio", "L1 cache hit ratio in [0,1].", nil, nil)
	cacheL1SizeBytes      = prometheus.NewDesc("cache_l1_size_bytes", "Approximate L1 cache size in bytes.", nil, nil)
	cacheL1EvictionsTotal = prometheus.NewDesc("cache_l1_evictions_total", "Cumulative L1 cache evictions.", nil, nil)
	cacheL1Enabled        = prometheus.NewDesc("cache_l1_enabled", "Whether L1 cache is enabled (1) or not (0).", nil, nil)
	cacheL2Enabled        = prometheus.NewDesc("cache_l2_enabled", "Whether L2 cache is enabled (1) or not (0).", nil, nil)
	cacheBloomEnabled     = prometheus.NewDesc("cache_bloom_enabled", "Whether bloom filter is enabled (1) or not (0).", nil, nil)
	cacheHotkeyEnabled    = prometheus.NewDesc("cache_hotkey_enabled", "Whether hot-key detection is enabled (1) or not (0).", nil, nil)
	cacheHotkeyCount      = prometheus.NewDesc("cache_hotkey_count", "Current number of detected hot keys.", nil, nil)
)

func (c *cacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cacheL1HitsTotal
	ch <- cacheL1MissesTotal
	ch <- cacheL1HitRatio
	ch <- cacheL1SizeBytes
	ch <- cacheL1EvictionsTotal
	ch <- cacheL1Enabled
	ch <- cacheL2Enabled
	ch <- cacheBloomEnabled
	ch <- cacheHotkeyEnabled
	ch <- cacheHotkeyCount
}

func (c *cacheCollector) Collect(ch chan<- prometheus.Metric) {
	if c.mgr == nil {
		return
	}
	hits, misses := c.mgr.L1Hits(), c.mgr.L1Misses()
	ch <- prometheus.MustNewConstMetric(cacheL1HitsTotal, prometheus.CounterValue, float64(hits))
	ch <- prometheus.MustNewConstMetric(cacheL1MissesTotal, prometheus.CounterValue, float64(misses))
	ch <- prometheus.MustNewConstMetric(cacheL1Enabled, prometheus.GaugeValue, b2f(c.mgr.L1Enabled()))
	ch <- prometheus.MustNewConstMetric(cacheL2Enabled, prometheus.GaugeValue, b2f(c.mgr.L2Enabled()))
	ch <- prometheus.MustNewConstMetric(cacheBloomEnabled, prometheus.GaugeValue, b2f(c.mgr.BloomEnabled()))
	ch <- prometheus.MustNewConstMetric(cacheHotkeyEnabled, prometheus.GaugeValue, b2f(c.mgr.HotKeyEnabled()))

	if c.mgr.L1Enabled() {
		ch <- prometheus.MustNewConstMetric(cacheL1HitRatio, prometheus.GaugeValue, c.mgr.L1HitRatio())
		ch <- prometheus.MustNewConstMetric(cacheL1SizeBytes, prometheus.GaugeValue, float64(c.mgr.L1Size()))
		ch <- prometheus.MustNewConstMetric(cacheL1EvictionsTotal, prometheus.CounterValue, float64(c.mgr.L1Evictions()))
	}
	if c.mgr.HotKeyEnabled() {
		ch <- prometheus.MustNewConstMetric(cacheHotkeyCount, prometheus.GaugeValue, float64(c.mgr.HotKeyCount()))
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// dbPoolCollector 动态采集各数据库连接池统计（每次 scrape 实时读取 db.Stats()），
// 直接观测「DB 连接池/行锁饱和」这一头号容量风险（对应抢购 10309 超时）。
// 由 metrics.RegisterDBPool 在数据库初始化后注入 *sql.DB；采集器在每次 scrape 时遍历读取。
type dbPoolSource struct {
	name string
	db   *sql.DB
}

var dbPoolSources []dbPoolSource

// RegisterDBPool 注册一个数据库连接池供指标采集（db 为 nil 时忽略）。
// 在 bootstrap.InitDatabases 打开各库后调用；采集器在每次 scrape 时读取其 Stats()。
func RegisterDBPool(name string, db *sql.DB) {
	if db != nil {
		dbPoolSources = append(dbPoolSources, dbPoolSource{name: name, db: db})
	}
}

// dbPoolCollector 实现 prometheus.Collector，按 db 标签暴露各连接池统计。
type dbPoolCollector struct{}

var (
	dbPoolOpenConns      = prometheus.NewDesc("db_pool_open_connections", "Current number of open connections (in-use + idle) in the pool.", []string{"db"}, nil)
	dbPoolInUseConns     = prometheus.NewDesc("db_pool_in_use_connections", "Connections currently in use (checked out) from the pool.", []string{"db"}, nil)
	dbPoolIdleConns      = prometheus.NewDesc("db_pool_idle_connections", "Idle (free) connections in the pool.", []string{"db"}, nil)
	dbPoolMaxOpenConns   = prometheus.NewDesc("db_pool_max_open_connections", "Maximum allowed open connections (0 = unlimited).", []string{"db"}, nil)
	dbPoolWaitCount      = prometheus.NewDesc("db_pool_wait_count_total", "Total times a connection was waited for because the pool was exhausted.", []string{"db"}, nil)
	dbPoolWaitDuration   = prometheus.NewDesc("db_pool_wait_duration_seconds_total", "Total time spent waiting for a connection (pool exhausted).", []string{"db"}, nil)
	dbPoolLifetimeClosed = prometheus.NewDesc("db_pool_lifetime_closed_total", "Connections closed due to SetConnMaxLifetime.", []string{"db"}, nil)
)

func (dbPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dbPoolOpenConns
	ch <- dbPoolInUseConns
	ch <- dbPoolIdleConns
	ch <- dbPoolMaxOpenConns
	ch <- dbPoolWaitCount
	ch <- dbPoolWaitDuration
	ch <- dbPoolLifetimeClosed
}

func (dbPoolCollector) Collect(ch chan<- prometheus.Metric) {
	for _, src := range dbPoolSources {
		st := src.db.Stats()
		ch <- prometheus.MustNewConstMetric(dbPoolOpenConns, prometheus.GaugeValue, float64(st.OpenConnections), src.name)
		ch <- prometheus.MustNewConstMetric(dbPoolInUseConns, prometheus.GaugeValue, float64(st.InUse), src.name)
		ch <- prometheus.MustNewConstMetric(dbPoolIdleConns, prometheus.GaugeValue, float64(st.Idle), src.name)
		ch <- prometheus.MustNewConstMetric(dbPoolMaxOpenConns, prometheus.GaugeValue, float64(st.MaxOpenConnections), src.name)
		ch <- prometheus.MustNewConstMetric(dbPoolWaitCount, prometheus.CounterValue, float64(st.WaitCount), src.name)
		ch <- prometheus.MustNewConstMetric(dbPoolWaitDuration, prometheus.CounterValue, st.WaitDuration.Seconds(), src.name)
		ch <- prometheus.MustNewConstMetric(dbPoolLifetimeClosed, prometheus.CounterValue, float64(st.MaxLifetimeClosed), src.name)
	}
}

// mqMetricsAdapter 实现 mq.Metrics，把各 MQ 适配器的发送计数落到 MQProducerSendTotal。
// 放在本包（metrics → mq，无反向依赖，不构成 import 环）而非 mq 包，避免 mq 直接依赖 prometheus。
type mqMetricsAdapter struct{}

func (mqMetricsAdapter) IncProducerSend(mqType, topic, result string) {
	MQProducerSendTotal.WithLabelValues(mqType, topic, result).Inc()
}

func (mqMetricsAdapter) IncProducerDLQ(mqType, topic string) {
	MQProducerDLQTotal.WithLabelValues(mqType, topic).Inc()
}

func (mqMetricsAdapter) IncProducerDropped(mqType, topic string) {
	MQProducerDroppedTotal.WithLabelValues(mqType, topic).Inc()
}

func (mqMetricsAdapter) SetDLQBacklog(mqType, topic string, backlog int64) {
	MQDLQBacklog.WithLabelValues(mqType, topic).Set(float64(backlog))
}

var initOnce sync.Once

// Init 注册缓存采集器（缓存管理器可为 nil；多次调用安全，仅首次生效），并把 MQ 生产者 /
// 消费者指标接收器接到本包的 Prometheus 计数器。
func Init(cacheMgr *cache.Manager) {
	initOnce.Do(func() {
		if cacheMgr != nil {
			Registry.MustRegister(&cacheCollector{mgr: cacheMgr})
		}
		mq.SetMetrics(mqMetricsAdapter{})
		mq.SetConsumerMetrics(mqConsumerMetricsAdapter{})
	})
}

// mqConsumerMetricsAdapter 实现 mq.ConsumerMetrics，把消费侧指标落到本包的 Prometheus
// 计数器 / 仪表。partition 以字符串标签写入（Prometheus 标签均为字符串）。
type mqConsumerMetricsAdapter struct{}

func (mqConsumerMetricsAdapter) IncConsumed(mqType, topic, result string) {
	MQConsumerMessagesTotal.WithLabelValues(mqType, topic, result).Inc()
}

func (mqConsumerMetricsAdapter) IncFetchError(mqType, topic string) {
	MQConsumerFetchErrorsTotal.WithLabelValues(mqType, topic).Inc()
}

func (mqConsumerMetricsAdapter) SetLag(mqType, topic string, partition int, lag int64) {
	MQConsumerLag.WithLabelValues(mqType, topic, strconv.Itoa(partition)).Set(float64(lag))
}

func (mqConsumerMetricsAdapter) SetOffset(mqType, topic string, partition int, offset int64) {
	MQConsumerOffset.WithLabelValues(mqType, topic, strconv.Itoa(partition)).Set(float64(offset))
}

func (mqConsumerMetricsAdapter) SetDLQBacklog(mqType, topic string, backlog int64) {
	MQConsumerDLQBacklog.WithLabelValues(mqType, topic).Set(float64(backlog))
}

// SchedulerMetricsAdapter 实现 scheduler.Metrics，把调度任务 panic 落到 SchedulerJobPanicsTotal。
// 任务执行失败（闭包返回 error）由 bootstrap 在任务闭包内直接上报 SchedulerJobFailuresTotal
// （因 scheduler.Job 签名无返回值、失败在闭包内消化，无法经此钩子统一捕获）。
type SchedulerMetricsAdapter struct{}

func (SchedulerMetricsAdapter) RecordPanic(name string) {
	SchedulerJobPanicsTotal.WithLabelValues(name).Inc()
}

// Handler 返回 Prometheus exposition 格式的 HTTP handler。
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}
