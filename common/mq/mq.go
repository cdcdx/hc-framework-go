// Package mq 消息队列抽象层。
// 支持 memory（默认，进程内队列）/ kafka / rabbitmq / rocketmq。
// 对标 gin 版 common/mq 模块，四种后端均已实现对应适配器。
//
// 使用方式:
//
//	producer := mq.NewProducer(mq.Config{Type: "kafka", Brokers: []string{"..."}})
//	producer.Send(ctx, topic, key, value)
//
//	consumer := mq.NewConsumer(mq.Config{Type: "kafka", Brokers: []string{"..."}})
//	consumer.Subscribe(ctx, topic, handler)
package mq

import (
	"context"
	"errors"
	"time"
)

// 适配器公共错误。
var (
	// ErrProducerNotStarted 生产者未成功启动（如 NameServer 不可达）。
	ErrProducerNotStarted = errors.New("mq: producer not started")
	// ErrConsumerNotStarted 消费者未成功启动。
	ErrConsumerNotStarted = errors.New("mq: consumer not started")
	// ErrTopicEmpty 未提供 topic 且配置中也没有默认 topic。
	ErrTopicEmpty = errors.New("mq: topic is empty")
)

// Message 消息体
type Message struct {
	Key       string
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
}

// Producer 消息生产者接口
type Producer interface {
	// Send 发送消息。同步等待确认。
	Send(ctx context.Context, topic string, key string, value []byte) error
	// SendAsync 异步发送（fire-and-forget）
	SendAsync(ctx context.Context, topic string, key string, value []byte) error
	// Close 关闭生产者
	Close() error
}

// Handler 消息处理函数
type Handler func(ctx context.Context, msg *Message) error

// Consumer 消息消费者接口
type Consumer interface {
	// Subscribe 订阅 topic，消息交给 handler 处理。
	Subscribe(ctx context.Context, topic string, handler Handler) error
	// Close 关闭消费者
	Close() error
}

// KafkaConfig Kafka 专用连接参数（完全独立，不与其他后端互通）。
type KafkaConfig struct {
	// Brokers broker 地址列表。
	Brokers []string
	// ConsumerGroup 消费者组 ID。
	ConsumerGroup string
	// Topic 默认主题（未显式指定 topic 时使用）。
	Topic string
	// RequiredAcks：0 不等待 / 1 等待 leader / -1 等待全部副本（默认 1）。
	RequiredAcks int
	// BatchSize 批量攒批上限（条）。
	BatchSize int
	// BatchBytes 批量字节上限。
	BatchBytes int
}

// RocketMQConfig RocketMQ 专用连接参数（完全独立，不与其他后端互通）。
type RocketMQConfig struct {
	// NameServer 地址列表（如 ["127.0.0.1:9876"]）。
	NameServer []string
	// Group 消费/生产组名。
	Group string
	// Retry 发送重试次数。
	Retry int
	// Topic 默认主题。
	Topic string
	// AccessKey / SecretKey 用于开启 ACL 的 NameServer 集群。
	AccessKey string
	SecretKey string
	// Namespace 命名空间（如阿里云 ONS 实例 ID）。
	Namespace string
}

// RabbitMQConfig RabbitMQ 专用连接参数（完全独立，不与其他后端互通）。
type RabbitMQConfig struct {
	// URL 完整的 amqp 连接串（如 amqp://user:pass@host:5672/vhost）。
	// 必填，不再从顶层 Brokers 拼接。
	URL string
	// Exchange 交换机名称（发布/订阅时声明）。
	Exchange string
	// ExchangeType 交换机类型：direct / topic / fanout（默认 topic）。
	ExchangeType string
	// Queue 默认队列名（消费时声明并绑定；空则按 topic 派生）。
	Queue string
	// RoutingKey 路由键（默认与 topic 一致）。
	RoutingKey string
	// Topic 未显式指定 topic 时使用的默认 routing key。
	Topic string
	// Prefetch 消费者预取条数（QoS），默认 1（公平分发）。
	Prefetch int
}

// MemoryConfig 内存队列专用参数（完全独立）。
type MemoryConfig struct {
	// BufferSize 通道缓冲大小，默认 1024。
	BufferSize int
	// MaxRetries 单条消息消费失败后的最大重试次数（默认 3，0 表示不重试）。
	MaxRetries int
	// RetryDelay 首次重试延迟（默认 50ms，之后指数退避，上限 500ms）。
	RetryDelay time.Duration
}

// Config 消息队列配置。
//
// Type 取值: "kafka" | "rocketmq" | "rabbitmq" | "memory"。
// 各后端配置完全独立、互不互通：切换 Type 后只读取对应子段，其余子段保留但无效。
type Config struct {
	Enabled bool   // 是否启用
	Type    string // kafka / rocketmq / rabbitmq / memory

	// 各后端专用配置：仅在使用对应 Type 时生效，配置完全隔离。
	Kafka    KafkaConfig
	RocketMQ RocketMQConfig
	RabbitMQ RabbitMQConfig
	Memory   MemoryConfig
}

// NewProducer 根据配置创建消息生产者。
func NewProducer(cfg Config) Producer {
	if !cfg.Enabled {
		return &noopProducer{}
	}
	switch cfg.Type {
	case "kafka":
		return newKafkaProducer(cfg)
	case "rocketmq":
		return newRocketMQProducer(cfg)
	case "rabbitmq":
		return newRabbitMQProducer(cfg)
	case "memory", "":
		return newMemoryProducer(cfg)
	default:
		return &noopProducer{}
	}
}

// NewConsumer 根据配置创建消息消费者。
func NewConsumer(cfg Config) Consumer {
	if !cfg.Enabled {
		return &noopConsumer{}
	}
	switch cfg.Type {
	case "kafka":
		return newKafkaConsumer(cfg)
	case "rocketmq":
		return newRocketMQConsumer(cfg)
	case "rabbitmq":
		return newRabbitMQConsumer(cfg)
	case "memory", "":
		return newMemoryConsumer(cfg)
	default:
		return &noopConsumer{}
	}
}

// ---- noop: 禁用时的空实现 ----
type noopProducer struct{}

func (n *noopProducer) Send(ctx context.Context, topic, key string, value []byte) error {
	return nil
}
func (n *noopProducer) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	return nil
}
func (n *noopProducer) Close() error { return nil }

type noopConsumer struct{}

func (n *noopConsumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	return nil
}
func (n *noopConsumer) Close() error { return nil }
