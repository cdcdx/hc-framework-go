package mq

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// TestNoopProducer_SendNoop 验证 no-op 生产者 Send/SendBatch/Close 均为空操作且不报错。
// 即使调用方未判空直接调用也不会 panic，保证 type=none 安全。
func TestNoopProducer_SendNoop(t *testing.T) {
	p := nopProducer{}
	if err := p.Send(context.Background(), &event.Message{EventType: event.EventIdleSettled}); err != nil {
		t.Fatalf("nopProducer.Send: %v", err)
	}
	if err := p.SendBatch(context.Background(), []*event.Message{{EventType: event.EventShopRedeemed}}); err != nil {
		t.Fatalf("nopProducer.SendBatch: %v", err)
	}
	if err := p.SendSync(context.Background(), &event.Message{EventType: event.EventShopRedeemed}); err != nil {
		t.Fatalf("nopProducer.SendSync: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("nopProducer.Close: %v", err)
	}
}

// TestNoopConsumer_SubscribeRespectsCtx 验证 no-op 消费者 Subscribe 阻塞直到 ctx 取消后返回，
// 不会泄漏 goroutine 或订阅任何主题。
func TestNoopConsumer_SubscribeRespectsCtx(t *testing.T) {
	c := nopConsumer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Subscribe(ctx, []string{"x"}, func(context.Context, *event.Message) error { return nil })
	}()
	select {
	case <-done:
		t.Fatal("noopConsumer.Subscribe returned before ctx cancel")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("noopConsumer.Subscribe: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("noopConsumer.Subscribe did not return after ctx cancel")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("nopConsumer.Close: %v", err)
	}
}

// TestNewProducer_DisabledReturnsNoop 覆盖全部「禁用」分支（none/空/未知 + 各类型连接缺失），
// 验证工厂对其返回非 nil 的 no-op 生产者，使 no-op 语义在接口层强制成立（调用方无需各自判空）。
func TestNewProducer_DisabledReturnsNoop(t *testing.T) {
	cases := []struct {
		name string
		tune func(c *config.MQConfig)
	}{
		{"none", func(c *config.MQConfig) { c.Type = "none" }},
		{"empty", func(c *config.MQConfig) { c.Type = "" }},
		{"bogus", func(c *config.MQConfig) { c.Type = "bogus" }},
		{"kafka-no-brokers", func(c *config.MQConfig) { c.Type = "kafka" }},
		{"rabbitmq-no-url", func(c *config.MQConfig) { c.Type = "rabbitmq" }},
		{"rocketmq-no-endpoints", func(c *config.MQConfig) { c.Type = "rocketmq" }},
	}
	for _, tc := range cases {
		p, err := NewProducer(context.Background(), mqCfg(tc.tune), nil)
		if err != nil {
			t.Fatalf("%s: NewProducer: %v", tc.name, err)
		}
		if p == nil {
			t.Fatalf("%s: want non-nil no-op producer", tc.name)
		}
		if _, ok := p.(nopProducer); !ok {
			t.Fatalf("%s: want nopProducer, got %T", tc.name, p)
		}
	}
}

// TestNewConsumer_DisabledReturnsNoop 同上，覆盖消费者禁用分支。
func TestNewConsumer_DisabledReturnsNoop(t *testing.T) {
	cases := []struct {
		name string
		tune func(c *config.MQConfig)
	}{
		{"none", func(c *config.MQConfig) { c.Type = "none" }},
		{"empty", func(c *config.MQConfig) { c.Type = "" }},
		{"bogus", func(c *config.MQConfig) { c.Type = "bogus" }},
		{"kafka-no-brokers", func(c *config.MQConfig) { c.Type = "kafka" }},
		{"rabbitmq-no-url", func(c *config.MQConfig) { c.Type = "rabbitmq" }},
		{"rocketmq-no-endpoints", func(c *config.MQConfig) { c.Type = "rocketmq" }},
	}
	for _, tc := range cases {
		c, err := NewConsumer(context.Background(), mqCfg(tc.tune), nil)
		if err != nil {
			t.Fatalf("%s: NewConsumer: %v", tc.name, err)
		}
		if c == nil {
			t.Fatalf("%s: want non-nil no-op consumer", tc.name)
		}
		if _, ok := c.(nopConsumer); !ok {
			t.Fatalf("%s: want nopConsumer, got %T", tc.name, c)
		}
	}
}
