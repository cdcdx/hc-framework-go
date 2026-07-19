package mq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// fakeRocketProducer 记录 SendSync 调用到的主题，用于验证 probe 路径向每个消费主题发消息。
type fakeRocketProducer struct {
	sentTopics []string
	err        error
}

func (f *fakeRocketProducer) Start() error    { return nil }
func (f *fakeRocketProducer) Shutdown() error { return nil }
func (f *fakeRocketProducer) SendSync(_ context.Context, m ...*primitive.Message) (*primitive.SendResult, error) {
	for _, msg := range m {
		f.sentTopics = append(f.sentTopics, msg.Topic)
	}
	return &primitive.SendResult{Status: primitive.SendOK}, f.err
}

// TestEnsureRocketMQTopics_ProbePath 验证未配置 broker_addr 时，向每个消费主题发送探针消息以触发
// broker 自动建主题（不依赖 admin API，无需真实 broker）。
func TestEnsureRocketMQTopics_ProbePath(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq"} // BrokerAddr 空 → 走 probe
	fake := &fakeRocketProducer{}
	ensureRocketMQTopics(context.Background(), cfg, "hc-consumer-group", fake, nopLogger{})

	want := ConsumerTopics(cfg)
	if len(fake.sentTopics) != len(want) {
		t.Fatalf("probe sent %d topics, want %d (%v)", len(fake.sentTopics), len(want), want)
	}
	got := map[string]bool{}
	for _, tp := range fake.sentTopics {
		got[tp] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("probe missing topic %q; sent=%v", w, fake.sentTopics)
		}
	}
}

// TestEnsureRocketMQTopics_NoProducerNoBroker logs a warning path without panicking.
func TestEnsureRocketMQTopics_NoProducerNoBroker(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq"} // 无 broker_addr 且无 producer
	// 仅验证不 panic（warn 路径）
	ensureRocketMQTopics(context.Background(), cfg, "g", nil, nopLogger{})
}

// TestConsumerTopics_ContainsExpected 确保 ConsumerTopics 覆盖 4 个事件主题且映射正确（与生产者一致）。
func TestConsumerTopics_ContainsExpected(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq"}
	topics := ConsumerTopics(cfg)
	seen := map[string]bool{}
	for _, tp := range topics {
		seen[tp] = true
	}
	for _, ev := range []string{event.EventIdleSettled, event.EventShopRedeemed, event.EventTaskCompleted, event.EventUserPointsAdjust} {
		if !seen[ResolveTopic(topicMapOf(cfg), ev)] {
			t.Fatalf("ConsumerTopics missing event %q", ev)
		}
	}
}

// TestRocketMQConsumer_LocalRetryThenSuccess 验证消费侧本地重试对齐 Kafka / RabbitMQ：
// handler 前几次失败后自愈，本地重试覆盖即在内部成功，无需升级到 broker %RETRY%（返回 ConsumeSuccess，无 broker 重试）。
func TestRocketMQConsumer_LocalRetryThenSuccess(t *testing.T) {
	r := &RocketMQConsumer{
		cfg: &config.MQConfig{Type: "rocketmq", Consumer: config.MQConsumerConfig{
			RetryMax:       2,
			RetryBaseDelay: time.Millisecond,
			RetryMaxDelay:  5 * time.Millisecond,
		}},
		log: nopLogger{},
	}
	body := mustJSON(t, &event.Message{EventID: "e1", EventType: "x"})
	me := &primitive.MessageExt{Message: primitive.Message{Body: body}}

	var calls int64
	res, err := r.handleMessages(context.Background(), "x", []*primitive.MessageExt{me}, func(ctx context.Context, m *event.Message) error {
		n := atomic.AddInt64(&calls, 1)
		if n < 2 { // 第 1 次失败，本地重试第 2 次成功
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res != consumer.ConsumeSuccess {
		t.Fatalf("expected ConsumeSuccess after local retry, got %v", res)
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("handler should be called exactly 2 times (local retry), got %d", got)
	}
}

// TestRocketMQProducer_RelayCountsAlignedWithKafkaRabbit 验证 RocketMQ relay 的计数口径对齐
// Kafka / RabbitMQ：补发成功计 1 次 success；补发失败不计发送失败（历史失败消息的重试，非新发送尝试，
// 且该失败已由先前的 recordDLQ 计数），避免 broker 持续不可达时每轮 replay 全量计 failure 虚高失败率。
func TestRocketMQProducer_RelayCountsAlignedWithKafkaRabbit(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq", RocketMQ: config.RocketMQConfig{SendTimeout: time.Second}}
	msg := &event.Message{EventType: event.EventIdleSettled, EventID: "r1"}

	// 成功补发：记 1 次 success。
	capOK := &captureMetrics{}
	SetMetrics(capOK)
	t.Cleanup(func() { SetMetrics(nil) })
	pok := &RocketMQProducer{
		cfg: cfg,
		p:   &fakeRocketProducer{},
		producerBase: producerBase{
			topics:  topicMapOf(cfg),
			log:     nopLogger{},
			breaker: newProducerBreaker("t", nopLogger{}),
		},
	}
	if err := pok.relay(context.Background(), msg); err != nil {
		t.Fatalf("relay success err: %v", err)
	}
	if got := capOK.snapshot(); len(got) != 1 || got[0][2] != resultSuccess {
		t.Fatalf("relay success want 1 success record, got %v", got)
	}

	// 失败补发：不计发送失败。
	capFail := &captureMetrics{}
	SetMetrics(capFail)
	pfail := &RocketMQProducer{
		cfg: cfg,
		p:   &fakeRocketProducer{err: errors.New("broker down")},
		producerBase: producerBase{
			topics:  topicMapOf(cfg),
			log:     nopLogger{},
			breaker: newProducerBreaker("t", nopLogger{}),
		},
	}
	if err := pfail.relay(context.Background(), msg); err == nil {
		t.Fatal("relay failure should return err")
	}
	if got := capFail.snapshot(); len(got) != 0 {
		t.Fatalf("relay failure want 0 send records (not counted), got %v", got)
	}
}
