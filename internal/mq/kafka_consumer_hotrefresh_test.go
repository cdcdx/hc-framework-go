package mq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// TestSubscribe_PartitionHotRefresh 锁死「分区扩容热感知」（13 §6.5 #4 / §3.57）：
// partition_refresh_interval>0 时，运行中分区数增长应被动态感知并新建 reader（日志 "new partition detected"）。
// 通过注入 partitionFn 模拟分区从 [0,1] 增至 [0,1,2]，无需真实 Kafka。
func TestSubscribe_PartitionHotRefresh(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	c, cl := newTestKafkaConsumer(t, config.KafkaConsumerConfig{
		PartitionRefreshInterval: 20 * time.Millisecond,
		RetryBaseDelay:           10 * time.Millisecond,
	})

	var mu sync.Mutex
	parts := []int{0, 1}
	c.partitionFn = func(ctx context.Context, topic string) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), parts...), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Subscribe(ctx, []string{"t"}, func(ctx context.Context, m *event.Message) error { return nil })
	}()

	// 让初始订阅建立 2 个 reader 后，模拟 broker 侧扩容到 3 个分区
	time.Sleep(60 * time.Millisecond)
	mu.Lock()
	parts = []int{0, 1, 2}
	mu.Unlock()

	detected := false
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if cl.containsInfo("new partition detected") {
			detected = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return after cancel")
	}
	if !detected {
		t.Fatalf("expected 'new partition detected' from hot refresh")
	}
}

// TestSubscribe_ConnectionWarnThreshold 锁死「多连接开销告警」（13 §6.5 #8）：
// reader 数（=broker 连接数）超过 connection_warn_threshold 时启动告警 "high connection count"。
func TestSubscribe_ConnectionWarnThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	c, cl := newTestKafkaConsumer(t, config.KafkaConsumerConfig{
		ConnectionWarnThreshold: 3,
		RetryBaseDelay:          10 * time.Millisecond,
	})
	// 模拟 5 个分区（超过阈值 3）
	c.partitionFn = func(ctx context.Context, topic string) ([]int, error) {
		return []int{0, 1, 2, 3, 4}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Subscribe(ctx, []string{"t"}, func(ctx context.Context, m *event.Message) error { return nil })
	}()

	warned := false
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if cl.containsWarn("high connection count") {
			warned = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return")
	}
	if !warned {
		t.Fatalf("expected connection warn (high connection count)")
	}
}

// TestSubscribe_ConnectionWarnDisabled 阈值<=0 时不告警（默认行为，不扰民）。
func TestSubscribe_ConnectionWarnDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	c, cl := newTestKafkaConsumer(t, config.KafkaConsumerConfig{
		ConnectionWarnThreshold: 0,
		RetryBaseDelay:          10 * time.Millisecond,
	})
	c.partitionFn = func(ctx context.Context, topic string) ([]int, error) {
		return []int{0, 1, 2, 3, 4, 5, 6, 7}, nil // 8 个分区，远超任何合理阈值
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Subscribe(ctx, []string{"t"}, func(ctx context.Context, m *event.Message) error { return nil })
	}()
	time.Sleep(100 * time.Millisecond)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return")
	}
	if cl.containsWarn("high connection count") {
		t.Fatalf("connection warn should be disabled when threshold<=0")
	}
}
