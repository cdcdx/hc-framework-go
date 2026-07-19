package mq

import (
	"strings"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// eventTopicsMap 事件类型 → 主题名（单一来源）。
//
// 所有 MQ 生产者/消费者、以及 Kafka EnsureTopics 都应以它为基准来构造事件→主题映射，
// 避免某处漏配某个事件（例如曾经漏掉 EventCacheInvalidate，导致该事件被错误回落到 "events"、
// 与 EnsureTopics 建的 CacheEvents 队列不一致）。新增事件类型时只需改这一处。
//
// src 为已选好的主题配置：kafka 用 mq.kafka.topics.*；rocketmq/rabbitmq 用 overlay 后的 topics。
func eventTopicsMap(src config.KafkaTopicsConfig) map[string]string {
	return map[string]string{
		event.EventIdleSettled:      src.IdleEvents,
		event.EventUserRegistered:   src.UserEvents,
		event.EventUserLoggedIn:     src.UserEvents,
		event.EventTaskCompleted:    src.TaskEvents,
		event.EventShopRedeemed:     src.ShopEvents,
		event.EventUserPointsAdjust: src.UserPoints,
		event.EventCacheInvalidate:  src.CacheEvents,
	}
}

// topicMapOf 将事件类型映射到主题名。各 MQ 适配器（Kafka/RabbitMQ/RocketMQ/内存）统一使用此映射，
// 保证事件→主题的语义一致。其内部委托 eventTopicsMap（单一来源），避免映射散落多处导致漏配。
//
// 主题来源按当前 mq.type 选择：
//   - rocketmq：优先用 mq.rocketmq.topics.*；
//   - rabbitmq：优先用 mq.rabbitmq.topics.*；
//   - kafka / memory / none 等：用 mq.kafka.topics.*。
//
// 专用 topics 中未配置（空）的字段回落到 kafka.topics 同名字段，保证早期只配 mq.kafka.topics.*
// 的配置切到 rocketmq/rabbitmq 时仍能沿用，无需改动即可平滑迁移。
func topicMapOf(cfg *config.MQConfig) map[string]string {
	base := cfg.Kafka.Topics
	src := base
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "rocketmq":
		src = overlayTopics(base, cfg.RocketMQ.Topics)
	case "rabbitmq":
		src = overlayTopics(base, cfg.RabbitMQ.Topics)
	}
	return eventTopicsMap(src)
}

// overlayTopics 以 kafka.topics 为基线，用专用 topics 中非空字段覆盖，实现「专用优先、未配回落」。
func overlayTopics(base, over config.KafkaTopicsConfig) config.KafkaTopicsConfig {
	out := base
	if over.UserEvents != "" {
		out.UserEvents = over.UserEvents
	}
	if over.IdleEvents != "" {
		out.IdleEvents = over.IdleEvents
	}
	if over.TaskEvents != "" {
		out.TaskEvents = over.TaskEvents
	}
	if over.ShopEvents != "" {
		out.ShopEvents = over.ShopEvents
	}
	if over.UserPoints != "" {
		out.UserPoints = over.UserPoints
	}
	if over.CacheEvents != "" {
		out.CacheEvents = over.CacheEvents
	}
	return out
}

// resolveTopic 事件类型 → 主题（映射为空时回退 "events"）。
func resolveTopic(topics map[string]string, eventType string) string {
	if t, ok := topics[eventType]; ok && t != "" {
		return t
	}
	return "events"
}

// TopicMapOf 导出 topicMapOf，供调用方（如 cmd/server）按当前 MQ 类型解析事件→主题。
func TopicMapOf(cfg *config.MQConfig) map[string]string { return topicMapOf(cfg) }

// ResolveTopic 导出 resolveTopic。
func ResolveTopic(m map[string]string, eventType string) string { return resolveTopic(m, eventType) }

// ConsumerTopics 返回事件消费者应订阅的主题列表（与生产者路由完全一致，且去重），依据当前 MQ 类型解析。
// 顺序固定为：idle.settled、shop.redeemed、task.completed、user.points.adjust。
// 关键：必须与生产者 resolveTopic(topicMapOf(cfg), ...) 的结果一一对应，否则消费者收不到消息
// （如 rocketmq 配置下主题不一致会导致 outbox 始终 pending、无法落库）。
//
// 去重：当多个事件映射到同一主题（如用户自定义重叠、或主题未配置全部回落 "events"）时，
// 若不去重会导致消费者对同一主题重复订阅（RocketMQ/RabbitMQ）或 Kafka 为同一分区重复建 Reader，
// 进而同一条消息被处理多次。这里按首次出现顺序去重，保证每个主题只订阅一次。
func ConsumerTopics(cfg *config.MQConfig) []string {
	m := topicMapOf(cfg)
	raw := []string{
		resolveTopic(m, event.EventIdleSettled),
		resolveTopic(m, event.EventShopRedeemed),
		resolveTopic(m, event.EventTaskCompleted),
		resolveTopic(m, event.EventUserPointsAdjust),
	}
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, t := range raw {
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}
