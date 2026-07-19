package mq

import "sync/atomic"

// MQ 类型标识（用于指标标签，统一各适配器上报口径）。
const (
	typeKafka    = "kafka"
	typeRabbitMQ = "rabbitmq"
	typeRocketMQ = "rocketmq"
	typeMemory   = "memory"
	typeNone     = "none" // 总线禁用（mq.type=none / 未配置 / 连接缺失）时的 no-op 生产者
)

// 发送结果标签值。
const (
	resultSuccess = "success"
	resultFailure = "failure"
)

// Metrics 是 MQ 生产侧的指标观测钩子：统一各适配器（kafka/rabbitmq/rocketmq/memory）的
// 「发送成功 / 失败 / 降级到 DLQ / 真正丢弃」计数，便于告警与容量观测。设计为接口 + 包级默认
// no-op，避免 mq 直接依赖 prometheus（与本包的 Logger 接口同一风格），也让各适配器统一上报而
// 无需改造 New* 签名。
//
// 由 main.go 在启动时通过 SetMetrics 注入 Prometheus 实现（见 cmd/server/main.go）。
type Metrics interface {
	// IncProducerSend 累加一次生产者发送计数。
	//   - mqType：kafka|rabbitmq|rocketmq|memory|none；
	//   - topic：事件映射出的主题名；
	//   - result：success|failure。
	IncProducerSend(mqType, topic, result string)
	// IncProducerDLQ 累加「降级到本地 DLQ（待 replay 协程补发，不丢消息）」的事件数，
	// 用于量化 broker 不可达 / 过载时的降级量（区别于真正丢弃）。
	IncProducerDLQ(mqType, topic string)
	// IncProducerDropped 累加「真正丢弃、无法补发」的事件数：内存缓冲满（memory 总线 at-most-once）
	// 或总线禁用（none / 未配置）时的 no-op 发送。用于量化实际丢失的事件量。
	IncProducerDropped(mqType, topic string)
	// SetDLQBacklog 设置某 (type, topic) 当前 DLQ 待补发的积压消息数（gauge）。
	// 由 dlqStore 在 append(+1) / replay 成功(-1) 时维护，反映「还有多少消息躺在 DLQ 未补发」，
	// 是比累计计数更直接的运维观测项（告警阈值可给例如 backlog>0 持续 N 分钟）。
	SetDLQBacklog(mqType, topic string, backlog int64)
}

// nopMetrics 空实现（未注入时使用，保证调用方无需判空）。
type nopMetrics struct{}

func (nopMetrics) IncProducerSend(string, string, string) {}
func (nopMetrics) IncProducerDLQ(string, string)          {}
func (nopMetrics) IncProducerDropped(string, string)      {}
func (nopMetrics) SetDLQBacklog(string, string, int64)    {}

// metricsHolder 固定包装类型：atomic.Value 要求每次 Store 的具体类型一致，
// 直接存不同动态类型的 Metrics 接口会 panic，故统一包成该结构体存取。
type metricsHolder struct{ m Metrics }

// metricsSink 包级指标接收器，默认 no-op。用 atomic.Value 持有以便运行期安全替换/读取。
var metricsSink atomic.Value // 存 metricsHolder

func init() { metricsSink.Store(metricsHolder{m: nopMetrics{}}) }

// SetMetrics 注入指标实现（main.go 启动时调用）；传 nil 恢复为 no-op。多次调用以最后一次为准。
func SetMetrics(m Metrics) {
	if m == nil {
		metricsSink.Store(metricsHolder{m: nopMetrics{}})
		return
	}
	metricsSink.Store(metricsHolder{m: m})
}

// currentMetrics 返回当前指标接收器（永不为 nil）。
func currentMetrics() Metrics {
	if h, ok := metricsSink.Load().(metricsHolder); ok && h.m != nil {
		return h.m
	}
	return nopMetrics{}
}

// recordSend 依据 err 是否为 nil 上报一次发送成功/失败计数（各生产者统一调用）。
func recordSend(mqType, topic string, err error) {
	result := resultSuccess
	if err != nil {
		result = resultFailure
	}
	currentMetrics().IncProducerSend(mqType, topic, result)
}

// recordDLQ 累加一次「降级到本地 DLQ」事件计数（消息落盘、待 replay 补发，不丢）。
func recordDLQ(mqType, topic string) {
	currentMetrics().IncProducerDLQ(mqType, topic)
}

// recordDropped 累加一次「真正丢弃」事件计数（内存缓冲满 / 总线禁用 no-op）。
func recordDropped(mqType, topic string) {
	currentMetrics().IncProducerDropped(mqType, topic)
}

// recordDLQBacklog 上报一次 DLQ 当前积压量（待补发的消息数），供 SetDLQBacklog gauge 使用。
func recordDLQBacklog(mqType, topic string, backlog int64) {
	currentMetrics().SetDLQBacklog(mqType, topic, backlog)
}

// ─────────────────────────────────────────────────────────────────────────────
// 消费侧指标（ConsumerMetrics）：与 Metrics / Logger 同一风格（接口 + 包级 no-op），
// 让 mq 不直接依赖 prometheus。由 metrics.Init 注入 Prometheus 实现（见 internal/metrics）。
// ─────────────────────────────────────────────────────────────────────────────

// 消费结果标签值（mq_consumer_messages_total 的 result 维度）。
const (
	resultProcessed    = "processed"     // 业务 handler 成功处理
	resultDedupSkipped = "dedup_skipped" // 幂等去重跳过（重复 event_id）
	resultDLQ          = "dlq"           // handler 重试耗尽落 DLQ
)

// ConsumerMetrics 是 MQ 消费侧的指标观测钩子：统一各适配器（kafka/rabbitmq/rocketmq/memory）
// 的「消费计数 / fetch 错误 / 消费 lag / 已提交 offset」，便于告警与容量观测。
// 设计为接口 + 包级默认 no-op，避免 mq 直接依赖 prometheus（与 Metrics / Logger 接口同一风格）。
type ConsumerMetrics interface {
	// IncConsumed 累加一次消费计数（result: processed|dedup_skipped|dlq）。
	IncConsumed(mqType, topic, result string)
	// IncFetchError 累加一次 partition reader 拉取失败（broker 瞬时错误 / 不可用）。
	IncFetchError(mqType, topic string)
	// SetLag 设置某 (topic, partition) 的消费 lag（落后于 partition 末尾的消息数）。
	SetLag(mqType, topic string, partition int, lag int64)
	// SetOffset 设置某 (topic, partition) 已消费到的 offset（reader 最后读到的 offset）。
	SetOffset(mqType, topic string, partition int, offset int64)
	// SetDLQBacklog 设置某 (type, topic) 消费侧本地 DLQ 当前存量（gauge）。
	// 与生产者侧 backlog 不同：消费者 DLQ（毒消息/重试耗尽）无自动 replay，属「存档」语义，
	// 该值在本进程内只随 appendDLQ 增长；进程启动时由扫描已有 DLQ 文件行数初始化，
	// 反映磁盘真实存量（人工清理文件后重启即归零）。持续 >0 是「有毒消息待人工处理」的告警信号。
	SetDLQBacklog(mqType, topic string, backlog int64)
}

type nopConsumerMetrics struct{}

func (nopConsumerMetrics) IncConsumed(string, string, string)   {}
func (nopConsumerMetrics) IncFetchError(string, string)         {}
func (nopConsumerMetrics) SetLag(string, string, int, int64)    {}
func (nopConsumerMetrics) SetOffset(string, string, int, int64) {}
func (nopConsumerMetrics) SetDLQBacklog(string, string, int64)  {}

// consumerMetricsHolder 固定包装类型：与 metricsHolder 同理，atomic.Value 要求存入类型一致。
type consumerMetricsHolder struct{ m ConsumerMetrics }

// consumerMetricsSink 包级消费指标接收器，默认 no-op。
var consumerMetricsSink atomic.Value

func init() { consumerMetricsSink.Store(consumerMetricsHolder{m: nopConsumerMetrics{}}) }

// SetConsumerMetrics 注入消费侧指标实现（metrics.Init 启动时调用）；传 nil 恢复为 no-op。
func SetConsumerMetrics(m ConsumerMetrics) {
	if m == nil {
		consumerMetricsSink.Store(consumerMetricsHolder{m: nopConsumerMetrics{}})
		return
	}
	consumerMetricsSink.Store(consumerMetricsHolder{m: m})
}

// currentConsumerMetrics 返回当前消费指标接收器（永不为 nil）。
func currentConsumerMetrics() ConsumerMetrics {
	if h, ok := consumerMetricsSink.Load().(consumerMetricsHolder); ok && h.m != nil {
		return h.m
	}
	return nopConsumerMetrics{}
}

// recordConsumed 上报一次消费计数（result: processed|dedup_skipped|dlq）。
func recordConsumed(mqType, topic, result string) {
	currentConsumerMetrics().IncConsumed(mqType, topic, result)
}

// recordConsumeFetchError 累加一次 partition reader 拉取失败。
func recordConsumeFetchError(mqType, topic string) {
	currentConsumerMetrics().IncFetchError(mqType, topic)
}

func setConsumerLag(mqType, topic string, partition int, lag int64) {
	currentConsumerMetrics().SetLag(mqType, topic, partition, lag)
}

func setConsumerOffset(mqType, topic string, partition int, offset int64) {
	currentConsumerMetrics().SetOffset(mqType, topic, partition, offset)
}

// setConsumerDLQBacklog 上报消费侧本地 DLQ 当前存量（毒消息/重试耗尽落盘、无自动 replay 的存档量）。
func setConsumerDLQBacklog(mqType, topic string, backlog int64) {
	currentConsumerMetrics().SetDLQBacklog(mqType, topic, backlog)
}
