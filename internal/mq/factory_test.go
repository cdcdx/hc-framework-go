package mq

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// mqCfg 构造最小 MQ 配置，topics 段与 factory 分发逻辑一致。
func mqCfg(tune func(*config.MQConfig)) *config.MQConfig {
	cfg := &config.MQConfig{
		Kafka: config.KafkaConfig{
			Topics: config.KafkaTopicsConfig{
				UserEvents: "user-events",
				IdleEvents: "idle-events",
				TaskEvents: "task-events",
				ShopEvents: "shop-events",
				UserPoints: "user-points",
			},
		},
	}
	if tune != nil {
		tune(cfg)
	}
	return cfg
}

// ───────────────────────── Producer 分发 ─────────────────────────

func TestNewProducer_Memory(t *testing.T) {
	p, err := NewProducer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = "memory" }), nil)
	if err != nil {
		t.Fatalf("NewProducer(memory): %v", err)
	}
	if p == nil {
		t.Fatal("want non-nil memory producer")
	}
}

func TestNewProducer_Kafka(t *testing.T) {
	p, err := NewProducer(context.Background(), mqCfg(func(c *config.MQConfig) {
		c.Type = "kafka"
		c.Kafka.Brokers = []string{"127.0.0.1:9092"}
	}), nil)
	if err != nil {
		t.Fatalf("NewProducer(kafka): %v", err)
	}
	kp, ok := p.(*KafkaProducer)
	if !ok {
		t.Fatalf("want *KafkaProducer, got %T", p)
	}
	kp.Close()
}

// NewProducer 对 rabbitmq 但 url 为空 → 返回 no-op 生产者（非 nil，Send 为空操作）。
func TestNewProducer_RabbitMQ_EmptyURLNoop(t *testing.T) {
	p, err := NewProducer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = "rabbitmq" }), nil)
	if err != nil {
		t.Fatalf("NewProducer(rabbitmq empty url): %v", err)
	}
	if p == nil {
		t.Fatal("want non-nil no-op producer for empty rabbitmq url")
	}
	if _, ok := p.(nopProducer); !ok {
		t.Fatalf("want nopProducer, got %T", p)
	}
}

// NewProducer 对 rocketmq 但 endpoints 为空 → 返回 no-op 生产者（非 nil）。
func TestNewProducer_RocketMQ_EmptyEndpointsNoop(t *testing.T) {
	p, err := NewProducer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = "rocketmq" }), nil)
	if err != nil {
		t.Fatalf("NewProducer(rocketmq empty endpoints): %v", err)
	}
	if p == nil {
		t.Fatal("want non-nil no-op producer for empty rocketmq endpoints")
	}
	if _, ok := p.(nopProducer); !ok {
		t.Fatalf("want nopProducer, got %T", p)
	}
}

// none / 未知 / 空 type → 返回 no-op 生产者（非 nil，接口层强制 no-op 语义）。
func TestNewProducer_NoneAndUnknownNil(t *testing.T) {
	for _, typ := range []string{"none", "bogus", ""} {
		p, err := NewProducer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = typ }), nil)
		if err != nil {
			t.Fatalf("NewProducer(type=%q): %v", typ, err)
		}
		if p == nil {
			t.Fatalf("type=%q want non-nil no-op producer, got nil", typ)
		}
		if _, ok := p.(nopProducer); !ok {
			t.Fatalf("type=%q want nopProducer, got %T", typ, p)
		}
	}
}

// ───────────────────────── Consumer 分发 ─────────────────────────

func TestNewConsumer_Memory(t *testing.T) {
	c, err := NewConsumer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = "memory" }), nil)
	if err != nil {
		t.Fatalf("NewConsumer(memory): %v", err)
	}
	if c == nil {
		t.Fatal("want non-nil memory consumer")
	}
}

func TestNewConsumer_Kafka(t *testing.T) {
	c, err := NewConsumer(context.Background(), mqCfg(func(c *config.MQConfig) {
		c.Type = "kafka"
		c.Kafka.Brokers = []string{"127.0.0.1:9092"}
	}), nil)
	if err != nil {
		t.Fatalf("NewConsumer(kafka): %v", err)
	}
	kc, ok := c.(*KafkaConsumer)
	if !ok {
		t.Fatalf("want *KafkaConsumer, got %T", c)
	}
	kc.Close()
}

func TestNewConsumer_RabbitMQ_EmptyURLNil(t *testing.T) {
	c, err := NewConsumer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = "rabbitmq" }), nil)
	if err != nil {
		t.Fatalf("NewConsumer(rabbitmq empty url): %v", err)
	}
	if c == nil {
		t.Fatal("want non-nil no-op consumer for empty rabbitmq url")
	}
	if _, ok := c.(nopConsumer); !ok {
		t.Fatalf("want nopConsumer, got %T", c)
	}
}

func TestNewConsumer_RocketMQ_EmptyEndpointsNil(t *testing.T) {
	c, err := NewConsumer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = "rocketmq" }), nil)
	if err != nil {
		t.Fatalf("NewConsumer(rocketmq empty endpoints): %v", err)
	}
	if c == nil {
		t.Fatal("want non-nil no-op consumer for empty rocketmq endpoints")
	}
	if _, ok := c.(nopConsumer); !ok {
		t.Fatalf("want nopConsumer, got %T", c)
	}
}

func TestNewConsumer_NoneAndUnknownNil(t *testing.T) {
	for _, typ := range []string{"none", "bogus", ""} {
		c, err := NewConsumer(context.Background(), mqCfg(func(c *config.MQConfig) { c.Type = typ }), nil)
		if err != nil {
			t.Fatalf("NewConsumer(type=%q): %v", typ, err)
		}
		if c == nil {
			t.Fatalf("type=%q want non-nil no-op consumer, got nil", typ)
		}
		if _, ok := c.(nopConsumer); !ok {
			t.Fatalf("type=%q want nopConsumer, got %T", typ, c)
		}
	}
}

// ───────────────────────── 统一消费配置解析 ─────────────────────────

// TestConsumerGroup_TopLevelWins 验证顶层 mq.consumer_group 优先于 kafka.consumer.group_id，
// 保证切到 rabbitmq/rocketmq/memory 时消费组不再静默回落默认值。
func TestConsumerGroup_TopLevelWins(t *testing.T) {
	if got := consumerGroup(mqCfg(func(c *config.MQConfig) {
		c.ConsumerGroup = "uni-group"
		c.Kafka.Consumer.GroupID = "kafka-group"
	})); got != "uni-group" {
		t.Fatalf("consumerGroup = %q, want uni-group", got)
	}
	// 未配顶层时回落 kafka.consumer.group_id
	if got := consumerGroup(mqCfg(func(c *config.MQConfig) {
		c.Kafka.Consumer.GroupID = "kafka-group"
	})); got != "kafka-group" {
		t.Fatalf("consumerGroup = %q, want kafka-group (fallback)", got)
	}
	// 全不配 → 默认
	if got := consumerGroup(mqCfg(func(c *config.MQConfig) {})); got != "hc-framework-consumer" {
		t.Fatalf("consumerGroup = %q, want default hc-framework-consumer", got)
	}
}

// TestConsumerRetryOf_PrefersUnified 验证非 kafka 类型优先使用统一 mq.consumer.*。
func TestConsumerRetryOf_PrefersUnified(t *testing.T) {
	max, base, maxDelay := consumerRetryOf(mqCfg(func(c *config.MQConfig) {
		c.Type = "rabbitmq"
		c.Consumer = config.MQConsumerConfig{RetryMax: 9, RetryBaseDelay: 2 * time.Second, RetryMaxDelay: 20 * time.Second}
		c.Kafka.Consumer.RetryMax = 5 // 不应被采用
	}))
	if max != 9 || base != 2*time.Second || maxDelay != 20*time.Second {
		t.Fatalf("consumerRetryOf = (%d,%v,%v), want (9,2s,20s)", max, base, maxDelay)
	}
}

// TestConsumerRetryOf_FallbackToKafka 验证统一段全零时回落 mq.kafka.consumer.*（向后兼容）。
func TestConsumerRetryOf_FallbackToKafka(t *testing.T) {
	max, _, _ := consumerRetryOf(mqCfg(func(c *config.MQConfig) {
		c.Type = "rocketmq"
		c.Kafka.Consumer.RetryMax = 7
	}))
	if max != 7 {
		t.Fatalf("consumerRetryOf max = %d, want 7 (fallback to kafka.consumer)", max)
	}
}

// TestConsumerRetryOf_Defaults 验证完全未配置时套用默认值 (5, 1s, 16s)，
// 避免切到非 kafka 队列因缺省而“零重试”。
func TestConsumerRetryOf_Defaults(t *testing.T) {
	max, base, maxDelay := consumerRetryOf(mqCfg(func(c *config.MQConfig) {
		c.Type = "memory"
	}))
	if max != 5 || base != time.Second || maxDelay != 16*time.Second {
		t.Fatalf("consumerRetryOf = (%d,%v,%v), want defaults (5,1s,16s)", max, base, maxDelay)
	}
	// kafka 类型不套用此默认（沿用自身 kafka.consumer，此处为 0 也只影响 kafka 自身实现）
	if max, _, _ := consumerRetryOf(mqCfg(func(c *config.MQConfig) {
		c.Type = "kafka"
	})); max != 0 {
		t.Fatalf("kafka consumerRetryOf max = %d, want 0 (kafka uses its own config)", max)
	}
}

// TestIsEnabled 验证各类型启用判定：连接齐全才启用，none/空/未知/连接缺失均禁用。
func TestIsEnabled(t *testing.T) {
	cases := []struct {
		name string
		tune func(c *config.MQConfig)
		want bool
	}{
		{"kafka-ok", func(c *config.MQConfig) { c.Type = "kafka"; c.Kafka.Brokers = []string{"b:9092"} }, true},
		{"kafka-empty", func(c *config.MQConfig) { c.Type = "kafka" }, false},
		{"rabbitmq-ok", func(c *config.MQConfig) { c.Type = "rabbitmq"; c.RabbitMQ.URL = "amqp://x" }, true},
		{"rabbitmq-empty", func(c *config.MQConfig) { c.Type = "rabbitmq" }, false},
		{"rocketmq-ok", func(c *config.MQConfig) { c.Type = "rocketmq"; c.RocketMQ.Endpoints = []string{"ns:9876"} }, true},
		{"rocketmq-empty", func(c *config.MQConfig) { c.Type = "rocketmq" }, false},
		{"memory", func(c *config.MQConfig) { c.Type = "memory" }, true},
		{"none", func(c *config.MQConfig) { c.Type = "none" }, false},
		{"empty", func(c *config.MQConfig) { c.Type = "" }, false},
		{"bogus", func(c *config.MQConfig) { c.Type = "bogus" }, false},
	}
	for _, tc := range cases {
		if got := IsEnabled(mqCfg(tc.tune)); got != tc.want {
			t.Fatalf("%s: IsEnabled = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestDescribeConfig 验证状态文案对启用/禁用及连接缺失原因的区分。
func TestDescribeConfig(t *testing.T) {
	if s := DescribeConfig(mqCfg(func(c *config.MQConfig) { c.Type = "none" })); s != "disabled (mq.type=none)" {
		t.Fatalf("DescribeConfig(none) = %q", s)
	}
	if s := DescribeConfig(mqCfg(func(c *config.MQConfig) { c.Type = "kafka" })); s == "" || !contains(s, "brokers empty") {
		t.Fatalf("DescribeConfig(kafka no brokers) = %q, want reason", s)
	}
	if s := DescribeConfig(mqCfg(func(c *config.MQConfig) { c.Type = "rabbitmq"; c.RabbitMQ.URL = "amqp://x" })); s != "enabled (rabbitmq)" {
		t.Fatalf("DescribeConfig(rabbitmq ok) = %q", s)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
