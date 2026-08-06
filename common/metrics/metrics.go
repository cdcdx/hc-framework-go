// Package metrics 业务自定义 Prometheus 指标。
//
// 注意：本包使用 promauto（默认注册到 prometheus 的全局 default registry）。
// app/gateway/api/handler/routes.go 的 promhttp.Handler() 与 go-zero 框架自带的
// http_server_requests_* / go_goroutines / process_* 指标同样注册在 default registry，
// 因此 /metrics 端点能一次性暴露全部指标，无需自建 registry。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// FlashRedeemTimeout 抢购兑换超时计数（对应容量风险 10309）。
// reason 维度便于区分：context_deadline / db_slow / downstream。
var FlashRedeemTimeout = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "shop_flash_redeem_timeout_total",
		Help: "抢购兑换超时次数（含 context deadline 与下游依赖超时），按 reason 区分。",
	},
	[]string{"reason"},
)

// FlashRedeemTotal 抢购兑换总请求数（成功+失败），用于计算超时占比。
var FlashRedeemTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "shop_flash_redeem_total",
		Help: "抢购兑换总请求数（成功与失败均计入）。",
	},
)

// IdleScanDurationSeconds 挂机离线扫描单次耗时（Histogram）。
// 当前 idle 离线扫描逻辑若已实现，应在扫描前后记录；本指标已注册，供埋点使用。
var IdleScanDurationSeconds = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "idle_scan_duration_seconds",
		Help:    "挂机离线扫描单次耗时（秒）。",
		Buckets: prometheus.DefBuckets,
	},
)

// DBPoolUtilization 数据库连接池利用率 = InUse / MaxOpen，标签 db 区分库实例。
var DBPoolUtilization = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "db_pool_utilization",
		Help: "数据库连接池利用率（在途连接 / 最大连接），标签 db 区分实例。",
	},
	[]string{"db"},
)

// DBPoolWaitCount 数据库连接池等待次数（池满导致请求排队），标签 db 区分实例。
var DBPoolWaitCount = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "db_pool_wait_count_total",
		Help: "数据库连接池等待次数（池耗尽导致等待获取连接），标签 db 区分实例。",
	},
	[]string{"db"},
)

// DBOperationsTotal 数据库表级操作计数，按 db + table + operation 三维区分。
// operation 取值: select / insert / update / delete / raw。
// 用于快速定位热点表、排查慢查询表分布、评估分桶/分片优先级。
var DBOperationsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "db_operations_total",
		Help: "数据库表级操作总数，按 db + table + operation 区分。",
	},
	[]string{"db", "table", "operation"},
)

// DBOperationDurationSeconds 数据库操作耗时（Histogram），按 db + table + operation 区分。
// 用于发现慢查询热点表，配合 db_operations_total 计算平均延迟。
var DBOperationDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "db_operation_duration_seconds",
		Help:    "数据库单次操作耗时（秒），按 db + table + operation 区分。",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	},
	[]string{"db", "table", "operation"},
)

// DBOperationErrorsTotal 数据库操作错误计数，按 db + table + operation 区分。
var DBOperationErrorsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "db_operation_errors_total",
		Help: "数据库操作错误总数，按 db + table + operation 区分。",
	},
	[]string{"db", "table", "operation"},
)

// CacheHitTotal 缓存命中计数，按 store (l1/l2) 区分。
var CacheHitTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cache_hit_total",
		Help: "缓存命中次数，按 store(l1/l2) 区分。",
	},
	[]string{"store"},
)

// CacheMissTotal 缓存未命中计数，按 store (l1/l2) 区分。
var CacheMissTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cache_miss_total",
		Help: "缓存未命中次数，按 store(l1/l2) 区分。",
	},
	[]string{"store"},
)

// CacheOperationDurationSeconds L2 缓存操作耗时（Histogram），按 operation (get/set/del) 区分。
// 仅统计 L2（Redis/Valkey）网络耗时；L1（Ristretto 本地）为内存操作不计。
var CacheOperationDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "cache_operation_duration_seconds",
		Help:    "L2 缓存操作耗时（秒），按 operation 区分。",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	},
	[]string{"operation"},
)

// CacheBloomRejectTotal 布隆过滤器拒绝次数（缓存穿透保护）。
var CacheBloomRejectTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "cache_bloom_reject_total",
		Help: "布隆过滤器拒绝次数（key 一定不存在，直接返回避免穿透）。",
	},
)

// MQProduceTotal 消息生产计数，按 topic + result (ok/error/drop) 区分。
var MQProduceTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "mq_produce_total",
		Help: "消息生产总数，按 topic + result(ok/error/drop) 区分。",
	},
	[]string{"topic", "result"},
)

// MQConsumeTotal 消息消费计数，按 topic + result (ok/error) 区分。
var MQConsumeTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "mq_consume_total",
		Help: "消息消费总数，按 topic + result(ok/error) 区分。",
	},
	[]string{"topic", "result"},
)

// MQConsumeDurationSeconds 消息消费耗时（Histogram），按 topic 区分。
var MQConsumeDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "mq_consume_duration_seconds",
		Help:    "消息消费耗时（秒），按 topic 区分。",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"topic"},
)

// MQDeadLetterTotal 死信消息计数，按 topic 区分（重试耗尽进入死信队列）。
var MQDeadLetterTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "mq_dead_letter_total",
		Help: "进入死信队列的消息总数（重试耗尽），按 topic 区分。",
	},
	[]string{"topic"},
)
