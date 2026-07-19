// Package mq 消息队列抽象层，封装 Kafka/RabbitMQ/RocketMQ/Memory 的生产与消费。
package mq

import (
	"context"

	"github.com/cdcdx/hc-framework-go/internal/event"
)

// Producer 消息生产者接口
type Producer interface {
	// Send 异步发送单条消息：入队即返回，不阻塞调用方；失败降级本地 DLQ（不返回 error）。
	Send(ctx context.Context, msg *event.Message) error
	// SendSync 同步发送单条消息：阻塞等待 broker 确认后返回 error。失败不降级 DLQ，
	// 直接把投递结果返回给调用方，便于关键链路（如支付成功事件）发送失败时立即决策（回滚/重试）。
	SendSync(ctx context.Context, msg *event.Message) error
	// SendBatch 批量发送
	SendBatch(ctx context.Context, msgs []*event.Message) error
	// Close 关闭生产者
	Close() error
}

// 编译期断言：所有生产者实现均满足 Producer 接口（含 SendSync），漏实现会在编译期直接报错。
var (
	_ Producer = (*KafkaProducer)(nil)
	_ Producer = (*RocketMQProducer)(nil)
	_ Producer = (*RabbitMQProducer)(nil)
	_ Producer = (*memoryProducer)(nil)
	_ Producer = (*nopProducer)(nil)
)

// Consumer 消息消费者接口
type Consumer interface {
	// Subscribe 订阅主题
	Subscribe(ctx context.Context, topics []string, handler MessageHandler) error
	// Close 关闭消费者
	Close() error
}

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, msg *event.Message) error

// MessageQueue 消息队列统一接口
type MessageQueue interface {
	// NewProducer 创建生产者
	NewProducer(ctx context.Context) (Producer, error)
	// NewConsumer 创建消费者
	NewConsumer(ctx context.Context, groupID string) (Consumer, error)
	// Name 返回队列类型
	Name() string
	// Close 关闭连接
	Close() error
}
