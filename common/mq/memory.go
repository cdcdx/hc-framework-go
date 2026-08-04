package mq

import (
	"context"
	"sync"

	"github.com/zeromicro/go-zero/core/logx"
)

// memoryProducer 进程内队列生产者（默认实现，无需外部依赖）。
type memoryProducer struct {
	broker *memoryBroker
}

// memoryConsumer 进程内队列消费者。
type memoryConsumer struct {
	broker *memoryBroker
	done   chan struct{}
}

// memoryBroker 进程内消息代理（单例，同进程共享）
type memoryBroker struct {
	mu       sync.RWMutex
	topics   map[string][]chan *Message
	bufSize  int
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
	return &memoryProducer{broker: getBroker(cfg.BufferSize)}
}

func (p *memoryProducer) Send(ctx context.Context, topic, key string, value []byte) error {
	msg := &Message{Key: key, Value: value}
	p.broker.mu.RLock()
	chs := p.broker.topics[topic]
	p.broker.mu.RUnlock()
	for _, ch := range chs {
		select {
		case ch <- msg:
		default:
			logx.WithContext(ctx).Errorf("[mq-memory] topic=%s buffer full, dropping message", topic)
		}
	}
	return nil
}

func (p *memoryProducer) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	go func() { _ = p.Send(ctx, topic, key, value) }()
	return nil
}

func (p *memoryProducer) Close() error { return nil }

func newMemoryConsumer(cfg Config) *memoryConsumer {
	b := getBroker(cfg.BufferSize)
	return &memoryConsumer{broker: b, done: make(chan struct{})}
}

func (c *memoryConsumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	ch := make(chan *Message, c.broker.bufSize)
	c.broker.mu.Lock()
	c.broker.topics[topic] = append(c.broker.topics[topic], ch)
	c.broker.mu.Unlock()

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
				if err := handler(ctx, msg); err != nil {
					logx.WithContext(ctx).Errorf("[mq-memory] handler error topic=%s: %v", topic, err)
				}
			}
		}
	}()
	return nil
}

func (c *memoryConsumer) Close() error {
	close(c.done)
	return nil
}
