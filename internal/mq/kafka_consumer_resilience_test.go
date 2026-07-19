package mq

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// resilienceTestConsumer 构造仅用于白盒测试的消费者，指向不可达 broker（分区查询必然失败）。
func resilienceTestConsumer(strict bool) *KafkaConsumer {
	c := &KafkaConsumer{
		cfg: &config.KafkaConfig{
			// 127.0.0.1:1 为保留端口，几乎不可能被监听 → DialContext 快速失败。
			Brokers: []string{"127.0.0.1:1"},
			Consumer: config.KafkaConsumerConfig{
				PartitionFetchStrict: strict,
			},
		},
		log:     nopLogger{},
		closeCh: make(chan struct{}),
	}
	c.partitionFn = c.partitions // 与 NewKafkaConsumer 默认一致（见 13 §6.5 #4 / §3.57）
	return c
}

func noopHandler(context.Context, *event.Message) error { return nil }

// TestSubscribe_StrictFailFast 严格模式下任一 topic 查询失败即整体启动失败（旧行为）。
func TestSubscribe_StrictFailFast(t *testing.T) {
	c := resilienceTestConsumer(true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.Subscribe(ctx, []string{"topic-a", "topic-b"}, noopHandler)
	if err == nil {
		t.Fatal("strict mode should return error when partition fetch fails")
	}
	if !strings.Contains(err.Error(), "read partitions for topic") {
		t.Fatalf("strict mode error should mention topic partition fetch, got: %v", err)
	}
}

// TestSubscribe_ResilientAllFail 韧性模式下【全部】 topic 失败时仍应报错（避免静默空转）。
func TestSubscribe_ResilientAllFail(t *testing.T) {
	c := resilienceTestConsumer(false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := c.Subscribe(ctx, []string{"topic-a", "topic-b"}, noopHandler)
	if err == nil {
		t.Fatal("resilient mode should still error when ALL topics fail (nothing to consume)")
	}
	if !strings.Contains(err.Error(), "all") || !strings.Contains(err.Error(), "nothing to consume") {
		t.Fatalf("resilient all-fail error should mention nothing to consume, got: %v", err)
	}
}
