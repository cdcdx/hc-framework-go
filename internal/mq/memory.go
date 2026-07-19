package mq

import (
	"context"
	"sync"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// memoryBroker 进程内消息 broker（用于本地开发 / 测试 / type=memory）。
// 按 topic 维护有界 channel；无订阅者时消息丢弃（at-most-once，符合内存总线不持久化的语义）。
type memoryBroker struct {
	mu     sync.RWMutex
	topics map[string]chan *event.Message
}

func newMemoryBroker() *memoryBroker {
	return &memoryBroker{topics: make(map[string]chan *event.Message)}
}

func (b *memoryBroker) ensureTopic(topic string) chan *event.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.topics[topic]; ok {
		return ch
	}
	ch := make(chan *event.Message, 1024)
	b.topics[topic] = ch
	return ch
}

func (b *memoryBroker) publish(topic string, msg *event.Message) {
	b.mu.RLock()
	ch, ok := b.topics[topic]
	b.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case ch <- msg:
	default:
		// 缓冲满则丢弃（进程内总线，不持久化、at-most-once）。计入「真正丢弃」指标，
		// 仅在 broker 类总线中可通过 DLQ/replay 兜底，memory 无法补发。
		recordDropped(typeMemory, topic)
	}
}

// memoryBrokerInstance 全局单例：同一进程内的 producer/consumer 共享同一总线。
var memoryBrokerInstance = newMemoryBroker()

// memoryWarnOnce 分别保证 producer / consumer 的 dev/test 提示各打印一次。
// 此前两处共用单个 once，导致先创建的 producer 一旦打印，consumer 的告警被永久吞掉；
// 拆成两个 once 后两者各自只打印一次、互不抑制（见 13 §3.38）。
var (
	memoryProducerWarnOnce sync.Once
	memoryConsumerWarnOnce sync.Once
)

// memoryWarn 打印一次 dev/test 提示。once 由调用方传入：生产代码传各自的包级 once
// （memoryProducerWarnOnce / memoryConsumerWarnOnce），保证 producer/consumer 各自只打印一次、
// 互不吞没；测试可传独立 local once 隔离验证，避免包级全局 once 已被消耗导致断言 flaky。
func memoryWarn(once *sync.Once, log Logger, format string, args ...interface{}) {
	if log == nil {
		log = nopLogger{}
	}
	once.Do(func() { log.Warnf(format, args...) })
}

type memoryProducer struct {
	broker *memoryBroker
	topics map[string]string
}

// NewMemoryProducer 创建进程内生产者。
func NewMemoryProducer(cfg *config.MQConfig, log Logger) (Producer, error) {
	memoryWarn(&memoryProducerWarnOnce, log, "memory MQ is for local dev/test ONLY: producer drops on full buffer (at-most-once, no DLQ), do NOT use in production")
	return &memoryProducer{broker: memoryBrokerInstance, topics: topicMapOf(cfg)}, nil
}

func (p *memoryProducer) Send(_ context.Context, msg *event.Message) error {
	topic := resolveTopic(p.topics, msg.EventType)
	p.broker.publish(topic, msg)
	// 进程内总线从生产者视角总是「接收成功」（无订阅者时消息静默丢弃，属 at-most-once 语义）。
	recordSend(typeMemory, topic, nil)
	return nil
}

func (p *memoryProducer) SendBatch(_ context.Context, msgs []*event.Message) error {
	for _, m := range msgs {
		topic := resolveTopic(p.topics, m.EventType)
		p.broker.publish(topic, m)
		recordSend(typeMemory, topic, nil)
	}
	return nil
}

// SendSync 进程内总线同步发送：与 Send 等价（发布到本地 broker 即返回，无网络往返）。
func (p *memoryProducer) SendSync(_ context.Context, msg *event.Message) error {
	topic := resolveTopic(p.topics, msg.EventType)
	p.broker.publish(topic, msg)
	recordSend(typeMemory, topic, nil)
	return nil
}

func (p *memoryProducer) Close() error { return nil }

type memoryConsumer struct {
	broker *memoryBroker
	cfg    *config.MQConfig
	group  string
	log    Logger
}

// NewMemoryConsumer 创建进程内消费者。
func NewMemoryConsumer(cfg *config.MQConfig, group string, log Logger) (Consumer, error) {
	if log == nil {
		log = nopLogger{}
	}
	memoryWarn(&memoryConsumerWarnOnce, log, "memory MQ is for local dev/test ONLY: consumer is in-process (no broker persistence/redelivery), do NOT use in production")
	return &memoryConsumer{broker: memoryBrokerInstance, cfg: cfg, group: group, log: log}, nil
}

// consumerConcurrencyOf 返回 memory 消费者每个 topic 的处理 goroutine 数。<=0 用默认 1（串行）。
func consumerConcurrencyOf(cfg *config.MQConfig) int {
	return maxInt(cfg.Consumer.Concurrency, 1)
}

// Subscribe 为每个 topic 启动 concurrency 个消费者 goroutine 并发处理；阻塞直到 ctx 取消
// （与 Kafka 消费者语义一致）。concurrency 见 mq.consumer.concurrency（默认 1，内存总线仅 dev/test）。
func (c *memoryConsumer) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	for _, topic := range topics {
		ch := c.broker.ensureTopic(topic)
		n := consumerConcurrencyOf(c.cfg)
		for i := 0; i < n; i++ {
			go func(ch chan *event.Message) {
				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-ch:
						rmax, rbase, rmaxDelay := consumerRetryOf(c.cfg)
						if err := retryProcess(ctx, msg, handler, rmax, rbase, rmaxDelay); err != nil {
							c.log.Errorf("memory consumer handler failed (topic=%s): %v", topic, err)
							recordConsumed(typeMemory, topic, resultDLQ) // 内存总线 at-most-once：失败即丢弃，计入 dlq 口径
						} else {
							recordConsumed(typeMemory, topic, resultProcessed)
						}
					}
				}
			}(ch)
		}
	}
	<-ctx.Done()
	return nil
}

func (c *memoryConsumer) Close() error { return nil }
