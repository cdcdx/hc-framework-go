package mq

import (
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// TestTopicMapOf 验证事件类型 → 主题映射与配置一致。
func TestTopicMapOf(t *testing.T) {
	cfg := mqCfg(nil)
	m := topicMapOf(cfg)
	want := map[string]string{
		event.EventUserRegistered:   "user-events",
		event.EventUserLoggedIn:     "user-events",
		event.EventIdleSettled:      "idle-events",
		event.EventTaskCompleted:    "task-events",
		event.EventShopRedeemed:     "shop-events",
		event.EventUserPointsAdjust: "user-points",
	}
	for k, v := range want {
		if m[k] != v {
			t.Fatalf("topicMapOf[%q] = %q, want %q", k, m[k], v)
		}
	}
}

// TestTopicMapOf_RocketMQSpecificTopics 验证：mq.type=rocketmq 时优先采用 mq.rocketmq.topics.*，
// 而非误用 mq.kafka.topics.*；空缺字段回落到 kafka.topics。
func TestTopicMapOf_RocketMQSpecificTopics(t *testing.T) {
	cfg := mqCfg(func(c *config.MQConfig) {
		c.Type = "rocketmq"
		c.RocketMQ.Endpoints = []string{"127.0.0.1:9876"}
		c.Kafka.Topics.UserPoints = "kafka-user-points" // 故意与 rocketmq 不同
		c.RocketMQ.Topics = config.KafkaTopicsConfig{
			UserPoints: "rmq-user-points", // 专用字段优先
			// 其余留空 → 回落 kafka.topics
		}
	})
	m := topicMapOf(cfg)
	if m[event.EventUserPointsAdjust] != "rmq-user-points" {
		t.Fatalf("rocketmq user_points = %q, want rmq-user-points (specific topics must win)", m[event.EventUserPointsAdjust])
	}
	if m[event.EventIdleSettled] != "idle-events" {
		t.Fatalf("rocketmq idle_events = %q, want idle-events (fallback to kafka.topics)", m[event.EventIdleSettled])
	}
}

// TestTopicMapOf_RabbitMQSpecificTopics 验证：mq.type=rabbitmq 时优先采用 mq.rabbitmq.topics.*，
// 空缺字段回落到 kafka.topics。
func TestTopicMapOf_RabbitMQSpecificTopics(t *testing.T) {
	cfg := mqCfg(func(c *config.MQConfig) {
		c.Type = "rabbitmq"
		c.RabbitMQ.URL = "amqp://guest:guest@127.0.0.1:5672/"
		c.Kafka.Topics.ShopEvents = "kafka-shop-events"
		c.RabbitMQ.Topics = config.KafkaTopicsConfig{ShopEvents: "rmq-shop-events"}
	})
	m := topicMapOf(cfg)
	if m[event.EventShopRedeemed] != "rmq-shop-events" {
		t.Fatalf("rabbitmq shop_events = %q, want rmq-shop-events (specific topics must win)", m[event.EventShopRedeemed])
	}
	if m[event.EventIdleSettled] != "idle-events" {
		t.Fatalf("rabbitmq idle_events = %q, want idle-events (fallback to kafka.topics)", m[event.EventIdleSettled])
	}
}

// TestResolveTopic 验证已知事件走映射主题、未知事件回落 "events"。
func TestResolveTopic(t *testing.T) {
	topics := map[string]string{event.EventShopRedeemed: "shop-events"}
	if got := resolveTopic(topics, event.EventShopRedeemed); got != "shop-events" {
		t.Fatalf("resolveTopic(shop) = %q, want shop-events", got)
	}
	if got := resolveTopic(topics, "unknown.event"); got != "events" {
		t.Fatalf("resolveTopic(unknown) = %q, want events", got)
	}
	if got := resolveTopic(topics, ""); got != "events" {
		t.Fatalf("resolveTopic(empty) = %q, want events", got)
	}
}

// TestConsumerTopics_RocketMQConsistent 验证：当 mq.type=rocketmq 时，消费者订阅的主题
// 必须与生产者 resolveTopic(topicMapOf) 的结果完全一致，否则 rocketmq 收不到消息、outbox 无法落库。
func TestConsumerTopics_RocketMQConsistent(t *testing.T) {
	cfg := &config.MQConfig{
		Type:     "rocketmq",
		RocketMQ: config.RocketMQConfig{Endpoints: []string{"127.0.0.1:9876"}},
		Kafka: config.KafkaConfig{Topics: config.KafkaTopicsConfig{
			IdleEvents:  "idle-events",
			ShopEvents:  "shop-events",
			TaskEvents:  "task-events",
			UserPoints:  "user-points",
			CacheEvents: "cache-events",
		}},
	}
	topics := ConsumerTopics(cfg)
	wantSet := map[string]bool{"idle-events": true, "shop-events": true, "task-events": true, "user-points": true}
	gotSet := map[string]bool{}
	for _, tp := range topics {
		if tp == "" || tp == "events" {
			t.Fatalf("rocketmq consumer topic resolved to %q (empty/fallback) — producer/consumer would mismatch", tp)
		}
		gotSet[tp] = true
	}
	if len(gotSet) != len(wantSet) {
		t.Fatalf("consumer topic set = %v, want %v", gotSet, wantSet)
	}
	for k := range wantSet {
		if !gotSet[k] {
			t.Fatalf("consumer missing topic %q (producer would send here but consumer not subscribed)", k)
		}
	}
	// 逐个事件确认生产者路由主题 == 消费者订阅主题
	m := TopicMapOf(cfg)
	for et, wt := range map[string]string{
		event.EventIdleSettled:      "idle-events",
		event.EventShopRedeemed:     "shop-events",
		event.EventTaskCompleted:    "task-events",
		event.EventUserPointsAdjust: "user-points",
	} {
		if got := ResolveTopic(m, et); got != wt {
			t.Fatalf("rocketmq producer topic for %s = %q, want %q (consumer subscribes %q)", et, got, wt, wt)
		}
	}
}

// TestConsumerTopics_EmptyFallbackConsistent 验证：未配置主题时，生产者与消费者均回落 "events"，
// 二者仍一致（不会因主题错配导致消息丢失）。且去重后只应有一个 "events"，避免对同一主题重复订阅。
func TestConsumerTopics_EmptyFallbackConsistent(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq", RocketMQ: config.RocketMQConfig{Endpoints: []string{"127.0.0.1:9876"}}}
	topics := ConsumerTopics(cfg)
	for _, tp := range topics {
		if tp != "events" {
			t.Fatalf("empty config should fall back to 'events', got %q", tp)
		}
	}
	if len(topics) != 1 {
		t.Fatalf("ConsumerTopics should dedup identical topics, got %d entries: %v", len(topics), topics)
	}
}

// TestConsumerTopics_Dedup 验证：多个事件映射到同一主题时，ConsumerTopics 去重，
// 避免消费者对同一主题重复订阅（进而重复处理消息）。
func TestConsumerTopics_Dedup(t *testing.T) {
	cfg := mqCfg(func(c *config.MQConfig) {
		c.Type = "rocketmq"
		c.RocketMQ.Endpoints = []string{"127.0.0.1:9876"}
		// idle/shop/task/points 全部映射到同一主题
		c.RocketMQ.Topics = config.KafkaTopicsConfig{
			IdleEvents: "all-events",
			ShopEvents: "all-events",
			TaskEvents: "all-events",
			UserPoints: "all-events",
		}
	})
	topics := ConsumerTopics(cfg)
	if len(topics) != 1 || topics[0] != "all-events" {
		t.Fatalf("expected deduped [all-events], got %v", topics)
	}
}

// allEventTypes 列出全部已知事件类型，供映射完整性回归测试使用。
// 新增事件类型时必须在此登记，并在 eventTopicsMap 中提供映射，否则会触发下面的覆盖测试失败，
// 避免再次出现「某事件漏配、被错误回落到 "events"、与 EnsureTopics 建的队列不一致」的问题（曾漏 CacheEvents）。
var allEventTypes = []string{
	event.EventIdleSettled,
	event.EventUserRegistered,
	event.EventUserLoggedIn,
	event.EventTaskCompleted,
	event.EventShopRedeemed,
	event.EventUserPointsAdjust,
	event.EventCacheInvalidate,
}

// TestEventTopicsMap_CoversAllEvents 验证 eventTopicsMap（Kafka 生产者与 topicMapOf 共用的
// 事件→主题唯一来源）覆盖全部事件类型，防止新增事件时漏配（如早期漏掉 CacheEvents 的事故）。
func TestEventTopicsMap_CoversAllEvents(t *testing.T) {
	m := eventTopicsMap(config.KafkaTopicsConfig{})
	for _, et := range allEventTypes {
		if _, ok := m[et]; !ok {
			t.Fatalf("eventTopicsMap missing key for event type %q (add it to eventTopicsMap single source)", et)
		}
	}
}

// TestKafkaProducerTopicMap_UsesSingleSource 验证 Kafka 生产者的事件→主题映射与 eventTopicsMap
// 一致（即统一复用唯一来源），不会出现旧版「内联 map 漏掉 CacheEvents」导致的不一致。
func TestKafkaProducerTopicMap_UsesSingleSource(t *testing.T) {
	p := newTestProducer(t)
	defer p.Close()
	for _, et := range allEventTypes {
		if _, ok := p.topics[et]; !ok {
			t.Fatalf("kafka producer topics missing event %q (must derive from eventTopicsMap)", et)
		}
	}
}
