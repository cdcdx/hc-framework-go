package mq

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/sony/gobreaker/v2"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// TestIsBreakerOpen 锁死三队列共享的「熔断打开」判定：
// 仅 errBreakerOpen 哨兵与 gobreaker.ErrOpenState/ErrTooManyRequests 视为熔断打开；
// nil、普通错误均不算。
func TestIsBreakerOpen(t *testing.T) {
	if isBreakerOpen(nil) {
		t.Fatal("nil should not be breaker open")
	}
	if isBreakerOpen(errors.New("boom")) {
		t.Fatal("plain error should not be breaker open")
	}
	if !isBreakerOpen(errBreakerOpen) {
		t.Fatal("errBreakerOpen sentinel must be breaker open")
	}
	if !isBreakerOpen(gobreaker.ErrOpenState) {
		t.Fatal("gobreaker.ErrOpenState must be breaker open")
	}
	if !isBreakerOpen(gobreaker.ErrTooManyRequests) {
		t.Fatal("gobreaker.ErrTooManyRequests must be breaker open")
	}
}

// newOpenBreaker 构造一个已「打开」的熔断器（连续失败达阈值），用于模拟 broker 不可达。
func newOpenBreaker() *gobreaker.CircuitBreaker[struct{}] {
	b := newProducerBreaker("test-producer", nopLogger{})
	for i := 0; i < 5; i++ {
		_, _ = b.Execute(func() (struct{}, error) { return struct{}{}, errors.New("boom") })
	}
	return b
}

// countDLQFiles 统计 dlq 目录下 .dlq.jsonl 文件数（避免依赖 filepath 在该环境的怪异行为）。
func countDLQFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %q: %v", dir, err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".dlq.jsonl") {
			n++
		}
	}
	return n
}

// TestRocketMQProducer_SendOneBreakerOpenNoDLQ 验证熔断打开时 sendOne 返回熔断错误且不降级 DLQ：
// 与 Kafka 对齐——broker 短暂抖动期间消息留在内存队列等恢复（requeue），而非无谓落盘依赖 30s replay。
func TestRocketMQProducer_SendOneBreakerOpenNoDLQ(t *testing.T) {
	dlqPath := t.TempDir()
	p := &RocketMQProducer{
		cfg:   &config.MQConfig{Type: "rocketmq", RocketMQ: config.RocketMQConfig{DLQEnabled: true, DLQLocalPath: dlqPath}},
		group: "g",
		p:     &fakeRocketProducer{}, // 熔断打开时 breaker 短路，不会真正调用 SendSync
		producerBase: producerBase{
			topics:  topicMapOf(&config.MQConfig{Type: "rocketmq"}),
			log:     nopLogger{},
			queue:   make(chan *event.Message, 1),
			sendSem: make(chan struct{}, 1),
			dlq:     newDLQStore(typeRocketMQ, true, dlqPath, nopLogger{}),
			breaker: newOpenBreaker(),
		},
	}
	msg := &event.Message{EventType: event.EventIdleSettled, EventID: "e1"}
	if err := p.sendOne(context.Background(), msg); !isBreakerOpen(err) {
		t.Fatalf("expected breaker-open error, got %v", err)
	}
	// 不应落 DLQ：broker 恢复后由 requeue 路径直接补发。
	if n := countDLQFiles(t, dlqPath); n != 0 {
		t.Fatalf("breaker-open must not write DLQ, got %d file(s)", n)
	}
}

// TestRocketMQProducer_SendOneRealFailureDLQ 验证真实发送失败（熔断仍关闭）时 sendOne 降级 DLQ 并返回 nil。
func TestRocketMQProducer_SendOneRealFailureDLQ(t *testing.T) {
	dlqPath := t.TempDir()
	p := &RocketMQProducer{
		cfg:   &config.MQConfig{Type: "rocketmq", RocketMQ: config.RocketMQConfig{DLQEnabled: true, DLQLocalPath: dlqPath}},
		group: "g",
		p:     &fakeRocketProducer{err: errors.New("broker down")},
		producerBase: producerBase{
			topics:  topicMapOf(&config.MQConfig{Type: "rocketmq"}),
			log:     nopLogger{},
			queue:   make(chan *event.Message, 1),
			sendSem: make(chan struct{}, 1),
			dlq:     newDLQStore(typeRocketMQ, true, dlqPath, nopLogger{}),
			breaker: newProducerBreaker("test-producer", nopLogger{}), // 全新：首次失败仍关闭
		},
	}
	msg := &event.Message{EventType: event.EventIdleSettled, EventID: "e2"}
	if err := p.sendOne(context.Background(), msg); err != nil {
		t.Fatalf("real failure should be DLQ'd (nil err), got %v", err)
	}
	if n := countDLQFiles(t, dlqPath); n != 1 {
		t.Fatalf("real failure must write DLQ, got %d file(s)", n)
	}
}
