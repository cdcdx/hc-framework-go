// Package mq 消息队列抽象层。
// 支持 memory（默认，进程内队列）/ kafka / rabbitmq / rocketmq。
// 对标 gin 版 common/mq 模块，当前 memory 和 kafka 可用，
// rabbitmq / rocketmq 为占位适配器。
//
// 使用方式:
//
//	producer := mq.NewProducer(mq.Config{Type: "kafka", Brokers: []string{"..."}})
//	producer.Send(ctx, topic, key, value)
//
//	consumer := mq.NewConsumer(mq.Config{Type: "kafka", Brokers: []string{"..."}})
//	consumer.Subscribe(ctx, topic, handler)
package mq

import "context"

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

// Config 消息队列配置
type Config struct {
	Enabled bool   // 是否启用
	Type    string // memory / kafka / rabbitmq / rocketmq / none
	// Kafka 专用
	Brokers       []string
	ConsumerGroup string
	// 通用
	DLQEnabled bool
	DLQTopic   string
	// 内存队列专用
	BufferSize int
	Workers    int
}

// NewProducer 根据配置创建消息生产者
func NewProducer(cfg Config) Producer {
	if !cfg.Enabled {
		return &noopProducer{}
	}
	switch cfg.Type {
	case "kafka":
		return newKafkaProducer(cfg)
	case "memory", "":
		return newMemoryProducer(cfg)
	default:
		return &noopProducer{}
	}
}

// NewConsumer 根据配置创建消息消费者
func NewConsumer(cfg Config) Consumer {
	if !cfg.Enabled {
		return &noopConsumer{}
	}
	switch cfg.Type {
	case "kafka":
		return newKafkaConsumer(cfg)
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
