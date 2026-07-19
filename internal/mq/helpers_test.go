package mq

import (
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// TestRabbitPoolSizeOf 验证 channel 池大小解析：未配置（<=0）回落默认值，配置时取配置值。
func TestRabbitPoolSizeOf(t *testing.T) {
	if got := rabbitPoolSizeOf(mqCfg(func(c *config.MQConfig) {
		c.RabbitMQ.PoolSize = 0
	})); got != defaultRabbitPoolSize {
		t.Fatalf("pool size default: want %d, got %d", defaultRabbitPoolSize, got)
	}
	const want = 16
	if got := rabbitPoolSizeOf(mqCfg(func(c *config.MQConfig) {
		c.RabbitMQ.PoolSize = want
	})); got != want {
		t.Fatalf("pool size configured: want %d, got %d", want, got)
	}
}

// TestKafkaSendTimeoutOf 验证 Kafka 发送超时上界解析：复用 DeliveryTimeout，未配置回落默认 5s。
func TestKafkaSendTimeoutOf(t *testing.T) {
	if got := kafkaSendTimeoutOf(&mqCfg(nil).Kafka); got != 5*time.Second {
		t.Fatalf("kafka send timeout default: want 5s, got %v", got)
	}
	const want = 7 * time.Second
	if got := kafkaSendTimeoutOf(&mqCfg(func(c *config.MQConfig) {
		c.Kafka.Producer.DeliveryTimeout = want
	}).Kafka); got != want {
		t.Fatalf("kafka send timeout configured: want %v, got %v", want, got)
	}
}

// TestRocketAsyncQueueCfg 验证 RocketMQ 异步队列/溢出池配置解析：未配置回落默认值。
func TestRocketAsyncQueueCfg(t *testing.T) {
	cfg := mqCfg(func(c *config.MQConfig) {
		c.RocketMQ.QueueSize = 0
		c.RocketMQ.MaxOverflowWorkers = 0
	})
	if got := rocketQueueSizeOf(cfg); got != defaultRocketQueueSize {
		t.Fatalf("queue size default: want %d, got %d", defaultRocketQueueSize, got)
	}
	if got := rocketOverflowWorkersOf(cfg); got != defaultRocketOverflowWorkers {
		t.Fatalf("overflow workers default: want %d, got %d", defaultRocketOverflowWorkers, got)
	}
	cfg2 := mqCfg(func(c *config.MQConfig) {
		c.RocketMQ.QueueSize = 10000
		c.RocketMQ.MaxOverflowWorkers = 512
	})
	if got := rocketQueueSizeOf(cfg2); got != 10000 {
		t.Fatalf("queue size configured: want 10000, got %d", got)
	}
	if got := rocketOverflowWorkersOf(cfg2); got != 512 {
		t.Fatalf("overflow workers configured: want 512, got %d", got)
	}
}

// TestRabbitAsyncQueueCfg 验证 RabbitMQ 异步队列/溢出池配置解析：未配置回落默认值（对齐 RocketMQ）。
func TestRabbitAsyncQueueCfg(t *testing.T) {
	cfg := mqCfg(func(c *config.MQConfig) {
		c.RabbitMQ.QueueSize = 0
		c.RabbitMQ.MaxOverflowWorkers = 0
	})
	if got := rabbitQueueSizeOf(cfg); got != defaultRabbitQueueSize {
		t.Fatalf("queue size default: want %d, got %d", defaultRabbitQueueSize, got)
	}
	if got := rabbitOverflowWorkersOf(cfg); got != defaultRabbitOverflowWorkers {
		t.Fatalf("overflow workers default: want %d, got %d", defaultRabbitOverflowWorkers, got)
	}
	cfg2 := mqCfg(func(c *config.MQConfig) {
		c.RabbitMQ.QueueSize = 10000
		c.RabbitMQ.MaxOverflowWorkers = 512
	})
	if got := rabbitQueueSizeOf(cfg2); got != 10000 {
		t.Fatalf("queue size configured: want 10000, got %d", got)
	}
	if got := rabbitOverflowWorkersOf(cfg2); got != 512 {
		t.Fatalf("overflow workers configured: want 512, got %d", got)
	}
}
