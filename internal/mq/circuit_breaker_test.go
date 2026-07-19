package mq

import (
	"errors"
	"testing"

	"github.com/sony/gobreaker/v2"
)

// TestNewProducerBreaker_TripsOnConsecutiveFailures 验证三队列共享的生产者熔断构造：
// 连续失败达阈值（5）后熔断打开，快速失败避免雪崩——这正是 Kafka/RabbitMQ/RocketMQ 生产者对齐的能力。
func TestNewProducerBreaker_TripsOnConsecutiveFailures(t *testing.T) {
	b := newProducerBreaker("test-producer", nopLogger{})
	for i := 0; i < 5; i++ {
		if _, err := b.Execute(func() (struct{}, error) { return struct{}{}, errors.New("boom") }); err == nil {
			t.Fatalf("attempt %d: expected error", i)
		}
	}
	if b.State() != gobreaker.StateOpen {
		t.Fatalf("expected breaker open, got %v", b.State())
	}
	// 打开态 Execute 立即返回 ErrOpenState（快速失败，不真正发起发送）。
	if _, err := b.Execute(func() (struct{}, error) { return struct{}{}, nil }); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("expected ErrOpenState when open, got %v", err)
	}
}
