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
