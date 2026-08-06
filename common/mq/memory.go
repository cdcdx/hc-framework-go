package mq

import (
	"context"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/common/metrics"
	"github.com/zeromicro/go-zero/core/logx"
)

// memoryProducer 进程内队列生产者（默认实现，无需外部依赖）。
type memoryProducer struct {
	broker *memoryBroker
}

// memoryConsumer 进程内队列消费者。
type memoryConsumer struct {
	broker     *memoryBroker
	done       chan struct{}
	handler    Handler       // 当前订阅的处理函数
	retries    int           // 单条消息最大重试次数（默认 3）
	retryDelay time.Duration // 重试间隔（默认 50ms，指数退避上限 500ms）
	dlq        Handler       // 死信处理器：重试耗尽后投递；为 nil 时仅记日志丢弃
}

// memoryBroker 进程内消息代理（单例，同进程共享）
type memoryBroker struct {
	mu      sync.RWMutex
	topics  map[string][]chan *Message
	bufSize int
}

var (
	brokerOnce   sync.Once
	globalBroker *memoryBroker
)

func getBroker(bufSize int) *memoryBroker {
	brokerOnce.Do(func() {
		if bufSize <= 0 {
			bufSize = 1024
		}
		globalBroker = &memoryBroker{
			topics:  make(map[string][]chan *Message),
			bufSize: bufSize,
		}
	})
	return globalBroker
}

func newMemoryProducer(cfg Config) *memoryProducer {
	return &memoryProducer{broker: getBroker(cfg.Memory.BufferSize)}
}

func (p *memoryProducer) Send(ctx context.Context, topic, key string, value []byte) error {
	msg := &Message{Key: key, Value: value}
	p.broker.mu.RLock()
	chs := p.broker.topics[topic]
	p.broker.mu.RUnlock()
	sent := false
	for _, ch := range chs {
		select {
		case ch <- msg:
			sent = true
		default:
			logx.WithContext(ctx).Errorf("[mq-memory] topic=%s buffer full, dropping message", topic)
		}
	}
	if sent {
		metrics.MQProduceTotal.WithLabelValues(topic, "ok").Inc()
	} else {
		metrics.MQProduceTotal.WithLabelValues(topic, "drop").Inc()
	}
	return nil
}

func (p *memoryProducer) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	go func() { _ = p.Send(ctx, topic, key, value) }()
	return nil
}

func (p *memoryProducer) Close() error { return nil }

func newMemoryConsumer(cfg Config) *memoryConsumer {
	b := getBroker(cfg.Memory.BufferSize)
	// 默认重试 3 次、退避 50ms（指数上限 500ms）；可由配置覆盖。
	retries := cfg.Memory.MaxRetries
	if retries <= 0 {
		retries = 3
	}
	delay := cfg.Memory.RetryDelay
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}
	return &memoryConsumer{broker: b, done: make(chan struct{}), retries: retries, retryDelay: delay}
}

// SetDLQ 设置死信处理器：消息在重试耗尽后仍失败时投递至此。
// 不调用则退化为仅记日志丢弃。
func (c *memoryConsumer) SetDLQ(h Handler) {
	c.dlq = h
}

// dispatch 执行 handler 并处理重试与死信：返回 nil 表示最终成功（或已入死信）。
func (c *memoryConsumer) dispatch(ctx context.Context, topic string, msg *Message) {
	start := time.Now()
	var lastErr error
	delay := c.retryDelay
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			// 指数退避，上限 500ms，避免重试风暴。
			backoff := delay
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
			time.Sleep(backoff)
			delay *= 2
		}
		err := c.handleOnce(ctx, msg)
		if err == nil {
			metrics.MQConsumeDurationSeconds.WithLabelValues(topic).Observe(time.Since(start).Seconds())
			metrics.MQConsumeTotal.WithLabelValues(topic, "ok").Inc()
			return
		}
		lastErr = err
		metrics.MQConsumeTotal.WithLabelValues(topic, "retry").Inc()
		logx.WithContext(ctx).Errorf("[mq-memory] handler error topic=%s attempt=%d: %v", topic, attempt+1, err)
	}
	// 重试耗尽 → 死信。
	metrics.MQConsumeTotal.WithLabelValues(topic, "dlq").Inc()
	metrics.MQDeadLetterTotal.WithLabelValues(topic).Inc()
	if c.dlq != nil {
		if derr := c.dlq(ctx, msg); derr != nil {
			logx.WithContext(ctx).Errorf("[mq-memory] DLQ handler error topic=%s: %v", topic, derr)
		}
		return
	}
	logx.WithContext(ctx).Errorf("[mq-memory] message dead-lettered (no DLQ handler) topic=%s key=%s: %v", topic, msg.Key, lastErr)
}

// handleOnce 包装单条处理；handler 返回 ErrSkip 时视为需忽略（不重试）。
func (c *memoryConsumer) handleOnce(ctx context.Context, msg *Message) error {
	return c.handler(ctx, msg)
}

func (c *memoryConsumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	ch := make(chan *Message, c.broker.bufSize)
	c.broker.mu.Lock()
	c.broker.topics[topic] = append(c.broker.topics[topic], ch)
	c.broker.mu.Unlock()

	// 保存 handler，供 dispatch 调用（保持不可变引用）。
	c.handler = handler

	go func() {
		defer func() {
			c.broker.mu.Lock()
			chs := c.broker.topics[topic]
			for i, ec := range chs {
				if ec == ch {
					c.broker.topics[topic] = append(chs[:i], chs[i+1:]...)
					break
				}
			}
			c.broker.mu.Unlock()
			close(ch)
		}()
		for {
			select {
			case <-c.done:
				return
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				c.dispatch(ctx, topic, msg)
			}
		}
	}()
	return nil
}

func (c *memoryConsumer) Close() error {
	close(c.done)
	return nil
}
