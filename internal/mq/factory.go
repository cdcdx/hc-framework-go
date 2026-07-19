package mq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// NewProducer 根据配置创建消息生产者。
//   - type=kafka 且配置了 brokers：返回 Kafka 生产者（事件真正发出）。
//   - type=rabbitmq 且配置了 url：返回 RabbitMQ 生产者。
//   - type=rocketmq 且配置了 endpoints：返回 RocketMQ 生产者。
//   - type=memory：返回进程内生产者（本地开发 / 测试）。
//   - 其它（含 none / 未配置 / 配置了但缺必填项）：返回 no-op 生产者（非 nil，Send 为空操作），
//     使「不启用事件总线」的语义在接口层面强制成立：调用方无需各自判空也不会 panic，
//     且下游按「无总线」分支正确运行（事件仅落库、缓存写降级回退本地 L2、积分由 Outbox relay 兜底）。
//
// log 为带级别的生产者内部日志（如熔断状态变更、DLQ 落盘/重放），可为 nil（静默）。
func NewProducer(ctx context.Context, cfg *config.MQConfig, log Logger) (Producer, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "kafka":
		if len(cfg.Kafka.Brokers) == 0 {
			return nopProducer{}, nil
		}
		return NewKafkaProducer(&cfg.Kafka, log)
	case "rabbitmq":
		if cfg.RabbitMQ.URL == "" {
			return nopProducer{}, nil
		}
		return NewRabbitMQProducer(cfg, consumerGroup(cfg), log)
	case "rocketmq":
		if len(cfg.RocketMQ.Endpoints) == 0 {
			return nopProducer{}, nil
		}
		return NewRocketMQProducer(cfg, consumerGroup(cfg), log)
	case "memory":
		return NewMemoryProducer(cfg, log)
	default:
		return nopProducer{}, nil
	}
}

// NewConsumer 根据配置创建消息消费者。
//   - type=kafka 且配置了 brokers：返回 Kafka 消费者（事件驱动消费）。
//   - type=rabbitmq 且配置了 url：返回 RabbitMQ 消费者。
//   - type=rocketmq 且配置了 endpoints：返回 RocketMQ 消费者。
//   - type=memory：返回进程内消费者（本地开发 / 测试）。
//   - 其它（含 none / 未配置 / 配置了但缺必填项）：返回 no-op 消费者（非 nil，Subscribe 为空操作），
//     使「不启用事件总线」的语义在接口层面强制成立：调用方无需各自判空也不会 panic。
//     实际是否启用订阅由 mq.IsEnabled 判定（main.go 据此决定是否启动消费循环）。
//
// log 为带级别的消费侧日志（如再均衡、重试、DLQ），可为 nil（静默）。
func NewConsumer(ctx context.Context, cfg *config.MQConfig, log Logger) (Consumer, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "kafka":
		if len(cfg.Kafka.Brokers) == 0 {
			return nopConsumer{}, nil
		}
		return NewKafkaConsumer(&cfg.Kafka, log)
	case "rabbitmq":
		if cfg.RabbitMQ.URL == "" {
			return nopConsumer{}, nil
		}
		return NewRabbitMQConsumer(cfg, consumerGroup(cfg), log)
	case "rocketmq":
		if len(cfg.RocketMQ.Endpoints) == 0 {
			return nopConsumer{}, nil
		}
		return NewRocketMQConsumer(cfg, consumerGroup(cfg), log)
	case "memory":
		return NewMemoryConsumer(cfg, consumerGroup(cfg), log)
	default:
		return nopConsumer{}, nil
	}
}

// consumerGroup 返回消费组名（所有队列类型共用）：
//  1. 优先取顶层 mq.consumer_group（统一配置，切类型无需改 group）；
//  2. 其次取 mq.kafka.consumer.group_id（向后兼容早期写在 kafka 段的用法）；
//  3. 均未配置则回退到统一默认值。
func consumerGroup(cfg *config.MQConfig) string {
	if cfg.ConsumerGroup != "" {
		return cfg.ConsumerGroup
	}
	if cfg.Kafka.Consumer.GroupID != "" {
		return cfg.Kafka.Consumer.GroupID
	}
	return "hc-framework-consumer"
}

// consumerRetryOf 返回当前 MQ 类型下的消费侧「本地重试」参数（RabbitMQ / RocketMQ / 内存适配器复用；
// Kafka 消费者自身已内联同样的语义，不走此处）。
//   - kafka：使用 mq.kafka.consumer.*（kafka 消费者 process 内部直接用，本函数仅供其它类型复用同一口径）；
//   - rocketmq/rabbitmq/memory：优先使用顶层 mq.consumer.*；若该段完全未配置（全零），
//     回落到 mq.kafka.consumer.*（向后兼容早期在 kafka 段统一设置重试的写法）；
//   - 最终仍为零时套用默认值 (5, 1s, 16s)，保证切到非 kafka 队列不会因缺省而“零重试”。
//
// 说明：三队列消费侧都先「本地指数退避重试」，再各自降级（Kafka 本地 DLQ / RabbitMQ broker DLX /
// RocketMQ broker %RETRY%），本函数即提供统一的「本地重试次数 + 退避上下界」。
func consumerRetryOf(cfg *config.MQConfig) (max int, base, maxDelay time.Duration) {
	if strings.EqualFold(strings.TrimSpace(cfg.Type), "kafka") {
		return cfg.Kafka.Consumer.RetryMax, cfg.Kafka.Consumer.RetryBaseDelay, cfg.Kafka.Consumer.RetryMaxDelay
	}
	c := cfg.Consumer
	if c.RetryMax == 0 && c.RetryBaseDelay == 0 && c.RetryMaxDelay == 0 {
		c = config.MQConsumerConfig{
			RetryMax:       cfg.Kafka.Consumer.RetryMax,
			RetryBaseDelay: cfg.Kafka.Consumer.RetryBaseDelay,
			RetryMaxDelay:  cfg.Kafka.Consumer.RetryMaxDelay,
		}
	}
	if c.RetryMax == 0 {
		c.RetryMax = 5
	}
	if c.RetryBaseDelay == 0 {
		c.RetryBaseDelay = time.Second
	}
	if c.RetryMaxDelay == 0 {
		c.RetryMaxDelay = 16 * time.Second
	}
	return c.RetryMax, c.RetryBaseDelay, c.RetryMaxDelay
}

// IsEnabled 判断当前 mq.type 是否为可启用的事件总线且连接配置齐全。
// kafka/rabbitmq/rocketmq 需各自连接项，memory 始终可用；none/空/未知/连接缺失均返回 false。
func IsEnabled(cfg *config.MQConfig) bool {
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "kafka":
		return len(cfg.Kafka.Brokers) > 0
	case "rabbitmq":
		return cfg.RabbitMQ.URL != ""
	case "rocketmq":
		return len(cfg.RocketMQ.Endpoints) > 0
	case "memory":
		return true
	default:
		return false
	}
}

// DescribeConfig 返回当前 MQ 配置的启用状态文案，便于启动时清晰告知事件总线是否启用及原因。
func DescribeConfig(cfg *config.MQConfig) string {
	t := strings.ToLower(strings.TrimSpace(cfg.Type))
	switch t {
	case "kafka":
		if len(cfg.Kafka.Brokers) == 0 {
			return "disabled: mq.type=kafka but mq.kafka.brokers empty"
		}
		return "enabled (kafka)"
	case "rabbitmq":
		if cfg.RabbitMQ.URL == "" {
			return "disabled: mq.type=rabbitmq but mq.rabbitmq.url empty"
		}
		return "enabled (rabbitmq)"
	case "rocketmq":
		if len(cfg.RocketMQ.Endpoints) == 0 {
			return "disabled: mq.type=rocketmq but mq.rocketmq.endpoints empty"
		}
		return "enabled (rocketmq)"
	case "memory":
		return "enabled (memory, in-process)"
	case "none", "":
		return "disabled (mq.type=none)"
	default:
		return fmt.Sprintf("disabled: unknown mq.type=%q", t)
	}
}
