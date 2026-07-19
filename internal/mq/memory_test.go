package mq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

func testMQConfig() *config.MQConfig {
	return &config.MQConfig{
		Kafka: config.KafkaConfig{
			Topics: config.KafkaTopicsConfig{
				IdleEvents: "idle-events",
				UserEvents: "user-events",
				TaskEvents: "task-events",
				ShopEvents: "shop-events",
				UserPoints: "user-points",
			},
		},
	}
}

func TestMemoryMQ_Roundtrip(t *testing.T) {
	cfg := testMQConfig()
	prod, err := NewMemoryProducer(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := NewMemoryConsumer(cfg, "g1", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan *event.Message, 1)
	go func() {
		_ = cons.Subscribe(ctx, []string{topicMapOf(cfg)[event.EventShopRedeemed]}, func(ctx context.Context, m *event.Message) error {
			got <- m
			return nil
		})
	}()

	// 等待订阅者注册 topic channel
	time.Sleep(30 * time.Millisecond)

	msg := &event.Message{
		EventType: event.EventShopRedeemed,
		Key:       "u1",
		EventID:   "shop:1",
		Payload:   event.ShopRedeemedPayload{UserID: "u1", OrderID: 1},
	}
	if err := prod.Send(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if m.EventType != event.EventShopRedeemed {
			t.Fatalf("event_type = %q, want shop.redeemed", m.EventType)
		}
		if m.Key != "u1" {
			t.Fatalf("key = %q, want u1", m.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message received within timeout")
	}
}

func TestMemoryMQ_UnsubscribedDropped(t *testing.T) {
	cfg := testMQConfig()
	prod, err := NewMemoryProducer(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 发布到无人订阅的 topic：应被丢弃（不阻塞、不 panic）
	if err := prod.Send(context.Background(), &event.Message{EventType: event.EventTaskCompleted}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryMQ_SendBatch(t *testing.T) {
	cfg := testMQConfig()
	prod, err := NewMemoryProducer(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	msgs := []*event.Message{
		{EventType: event.EventShopRedeemed, Key: "a"},
		{EventType: event.EventShopRedeemed, Key: "b"},
	}
	if err := prod.SendBatch(context.Background(), msgs); err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
}

// TestMemoryMQ_SendSync 验证 SendSync 与 Send 等价：发布到本地 broker 即返回且能被消费。
func TestMemoryMQ_SendSync(t *testing.T) {
	cfg := testMQConfig()
	prod, err := NewMemoryProducer(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := NewMemoryConsumer(cfg, "g1", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan *event.Message, 1)
	go func() {
		_ = cons.Subscribe(ctx, []string{topicMapOf(cfg)[event.EventShopRedeemed]}, func(ctx context.Context, m *event.Message) error {
			got <- m
			return nil
		})
	}()
	time.Sleep(30 * time.Millisecond)
	msg := &event.Message{EventType: event.EventShopRedeemed, Key: "u1", EventID: "shop:1", Payload: event.ShopRedeemedPayload{UserID: "u1", OrderID: 1}}
	if err := prod.SendSync(context.Background(), msg); err != nil {
		t.Fatalf("SendSync: %v", err)
	}
	// 内存 broker 在测试间复用同一 topic，可能先投递到此前 SendBatch 残留的消息；
	// 这里持续消费直到收到本次 SendSync 发出的 key=u1，避免跨测试污染导致的误判。
	for i := 0; i < 10; i++ {
		select {
		case m := <-got:
			if m.Key == "u1" {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("SendSync message (key=u1) not received within timeout")
			return
		}
	}
	t.Fatal("SendSync message (key=u1) not found among deliveries")
}

// warnSpy 记录 Warnf 调用的测试 Logger 实现，用于验证 dev/test 告警确实被打印。
type warnSpy struct {
	mu  sync.Mutex
	got []string
}

func (s *warnSpy) Infof(string, ...interface{})  {}
func (s *warnSpy) Errorf(string, ...interface{}) {}
func (s *warnSpy) Warnf(format string, args ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, fmt.Sprintf(format, args...))
}

// TestMemoryWarn_ProducerConsumerIndependent 验证 producer 与 consumer 的 dev/test 告警
// 各自只打印一次且互不抑制。此前两者共用单个包级 sync.Once，导致先创建的 producer
// 打印后 consumer 告警被永久吞掉（真实启动仅见 producer 告警）；拆成两个 once 后修复。
// 用独立 local once 传入，隔离包级全局 once（避免被其它测试用例提前消耗导致断言 flaky）。
func TestMemoryWarn_ProducerConsumerIndependent(t *testing.T) {
	spy := &warnSpy{}
	var pOnce, cOnce sync.Once

	memoryWarn(&pOnce, spy, "PRODUCER-WARN")
	memoryWarn(&cOnce, spy, "CONSUMER-WARN")
	memoryWarn(&pOnce, spy, "PRODUCER-WARN") // 第二次应为 no-op
	memoryWarn(&cOnce, spy, "CONSUMER-WARN") // 第二次应为 no-op

	spy.mu.Lock()
	got := append([]string(nil), spy.got...)
	spy.mu.Unlock()

	if len(got) != 2 {
		t.Fatalf("expected exactly 2 warnings (producer + consumer once each), got %d: %v", len(got), got)
	}
	if got[0] != "PRODUCER-WARN" || got[1] != "CONSUMER-WARN" {
		t.Errorf("unexpected warning order/content: %v", got)
	}
}

// TestMemoryMQ_PanicHandlerDoesNotCrash 验证 memory 消费者 handler 内 panic 不会拖垮进程：
// panic 被 callHandlerSafe 转为 error 仅记录，订阅 goroutine 继续消费后续消息（见 13 §3.39）。
func TestMemoryMQ_PanicHandlerDoesNotCrash(t *testing.T) {
	cfg := testMQConfig()
	prod, err := NewMemoryProducer(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := NewMemoryConsumer(cfg, "g1", nil)
	if err != nil {
		t.Fatal(err)
	}
	topic := topicMapOf(cfg)[event.EventShopRedeemed]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var normal atomic.Int64
	go func() {
		_ = cons.Subscribe(ctx, []string{topic}, func(_ context.Context, m *event.Message) error {
			if m.Key == "panic" {
				panic("boom")
			}
			if m.Key == "ok" {
				normal.Add(1)
			}
			return nil
		})
	}()
	time.Sleep(30 * time.Millisecond)

	// 先发一条会 panic 的消息：不应崩溃进程、不应阻塞后续消费
	if err := prod.Send(context.Background(), &event.Message{EventType: event.EventShopRedeemed, Key: "panic"}); err != nil {
		t.Fatal(err)
	}
	// 再发正常消息：若兜底生效，consumer 仍存活并在短时间内处理它
	if err := prod.Send(context.Background(), &event.Message{EventType: event.EventShopRedeemed, Key: "ok"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	for normal.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("consumer died after handler panic: 'ok' message not processed")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
