package mq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// fakeProgressStore 测试用内存进度/去重存储：记录 Commit 调用，去重基于内存 set。
// 用于验证 RabbitMQ / RocketMQ 消费者注入 store 后的幂等去重行为。
type fakeProgressStore struct {
	mu      sync.Mutex
	seen    map[string]bool
	commits int
}

func newFakeProgressStore() *fakeProgressStore {
	return &fakeProgressStore{seen: map[string]bool{}}
}

func (f *fakeProgressStore) Get(_ context.Context, _ string, _ int) (int64, bool, error) {
	return 0, false, nil
}
func (f *fakeProgressStore) Commit(_ context.Context, _ string, _ int, _ int64, eventID string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	if eventID != "" {
		f.seen[eventID] = true
	}
	return nil
}
func (f *fakeProgressStore) Seen(_ context.Context, eventID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen[eventID], nil
}
func (f *fakeProgressStore) FlushInterval() time.Duration { return 0 }
func (f *fakeProgressStore) Flush() error                 { return nil }
func (f *fakeProgressStore) Close() error                 { return nil }

// fakeAck 是 amqp.Acknowledger 的内存实现，记录 Ack/Nack 调用，避免零值 Delivery 调用 Ack 时 panic。
type fakeAck struct {
	acks  int
	nacks int
}

func (f *fakeAck) Ack(_ uint64, _ bool) error          { f.acks++; return nil }
func (f *fakeAck) Nack(_ uint64, _ bool, _ bool) error { f.nacks++; return nil }
func (f *fakeAck) Reject(_ uint64, _ bool) error       { return nil }

// TestRabbitMQConsumer_DedupSkipsDuplicate 验证 RabbitMQ 消费者注入 store 后：
// 首次投递 handler 被调用并标记去重；同 event_id 重投直接跳过 handler（仍 Ack）。
func TestRabbitMQConsumer_DedupSkipsDuplicate(t *testing.T) {
	store := newFakeProgressStore()
	c := &RabbitMQConsumer{cfg: &config.MQConfig{}, log: nopLogger{}, progressStore: store}
	first := &event.Message{EventID: "evt-1", EventType: "x"}

	called := false
	d1 := amqp.Delivery{Acknowledger: &fakeAck{}, Body: mustJSON(t, first), RoutingKey: "x"}
	c.handleDelivery(context.Background(), d1, func(ctx context.Context, m *event.Message) error {
		called = true
		return nil
	})
	if !called {
		t.Fatal("handler should be called on first delivery")
	}
	if store.commits != 1 {
		t.Fatalf("expected 1 commit, got %d", store.commits)
	}

	// 同 event_id 重投：跳过 handler，commit 计数不变。
	called = false
	d2 := amqp.Delivery{Acknowledger: &fakeAck{}, Body: mustJSON(t, first), RoutingKey: "x"}
	c.handleDelivery(context.Background(), d2, func(ctx context.Context, m *event.Message) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("handler should be skipped on duplicate delivery")
	}
	if store.commits != 1 {
		t.Fatalf("commit count should stay 1 for duplicate, got %d", store.commits)
	}
}

// TestRocketMQConsumer_DedupSkipsDuplicate 验证 RocketMQ 消费者注入 store 后去重行为（与 Kafka 对齐）。
func TestRocketMQConsumer_DedupSkipsDuplicate(t *testing.T) {
	store := newFakeProgressStore()
	r := &RocketMQConsumer{cfg: &config.MQConfig{}, log: nopLogger{}, progressStore: store}
	body := mustJSON(t, &event.Message{EventID: "evt-1", EventType: "x"})
	me := &primitive.MessageExt{Message: primitive.Message{Body: body}}

	called := false
	handler := func(ctx context.Context, m *event.Message) error { called = true; return nil }

	res, err := r.handleMessages(context.Background(), "x", []*primitive.MessageExt{me}, handler)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res != consumer.ConsumeSuccess {
		t.Fatalf("expected ConsumeSuccess, got %v", res)
	}
	if !called {
		t.Fatal("handler should be called on first")
	}
	if store.commits != 1 {
		t.Fatalf("expected 1 commit, got %d", store.commits)
	}

	// 重复：handler 不调用，返回 ConsumeSuccess（被跳过），commit 计数不变。
	called = false
	res, err = r.handleMessages(context.Background(), "x", []*primitive.MessageExt{me}, handler)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res != consumer.ConsumeSuccess {
		t.Fatalf("expected ConsumeSuccess for dup, got %v", res)
	}
	if called {
		t.Fatal("handler should be skipped on duplicate")
	}
	if store.commits != 1 {
		t.Fatalf("commit should stay 1, got %d", store.commits)
	}
}

// TestRocketMQConsumer_PoisonReturnsRetryLater 验证毒消息修复：unmarshal 失败返回 ConsumeRetryLater
// （交由 broker 重试并最终进 %DLQ%），而非旧实现的 continue 后整批 ConsumeSuccess 静默丢弃。
func TestRocketMQConsumer_PoisonReturnsRetryLater(t *testing.T) {
	r := &RocketMQConsumer{cfg: &config.MQConfig{}, log: nopLogger{}}
	me := &primitive.MessageExt{Message: primitive.Message{Body: []byte("not-json{")}}
	res, err := r.handleMessages(context.Background(), "x", []*primitive.MessageExt{me}, func(ctx context.Context, m *event.Message) error {
		return nil
	})
	if res != consumer.ConsumeRetryLater {
		t.Fatalf("expected ConsumeRetryLater for poison, got %v", res)
	}
	if err == nil {
		t.Fatal("expected error for poison message")
	}
}
