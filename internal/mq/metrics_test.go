package mq

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// captureMetrics 是测试用的 Metrics 实现，记录每次上报的 (type, topic, result)。
type captureMetrics struct {
	mu      sync.Mutex
	recs    [][3]string
	dlq     [][2]string
	dropped [][2]string
	backlog [][3]string
}

func (m *captureMetrics) IncProducerSend(mqType, topic, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, [3]string{mqType, topic, result})
}

func (m *captureMetrics) IncProducerDLQ(mqType, topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlq = append(m.dlq, [2]string{mqType, topic})
}

func (m *captureMetrics) IncProducerDropped(mqType, topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropped = append(m.dropped, [2]string{mqType, topic})
}

func (m *captureMetrics) SetDLQBacklog(mqType, topic string, backlog int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backlog = append(m.backlog, [3]string{mqType, topic, strconv.FormatInt(backlog, 10)})
}

func (m *captureMetrics) snapshot() [][3]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][3]string, len(m.recs))
	copy(out, m.recs)
	return out
}

// TestRecordSend_SuccessFailure 验证 recordSend 依 err 是否为 nil 上报 success/failure。
func TestRecordSend_SuccessFailure(t *testing.T) {
	cap := &captureMetrics{}
	SetMetrics(cap)
	t.Cleanup(func() { SetMetrics(nil) }) // 恢复 no-op，避免影响其它用例

	recordSend(typeKafka, "idle-events", nil)
	recordSend(typeRabbitMQ, "shop-events", context.Canceled)

	got := cap.snapshot()
	if len(got) != 2 {
		t.Fatalf("want 2 records, got %d: %v", len(got), got)
	}
	if got[0] != [3]string{"kafka", "idle-events", "success"} {
		t.Fatalf("record0 = %v, want kafka/idle-events/success", got[0])
	}
	if got[1] != [3]string{"rabbitmq", "shop-events", "failure"} {
		t.Fatalf("record1 = %v, want rabbitmq/shop-events/failure", got[1])
	}
}

// TestRecordDLQAndDropped 验证降级到 DLQ 与真正丢弃分别走独立指标口径。
func TestRecordDLQAndDropped(t *testing.T) {
	cap := &captureMetrics{}
	SetMetrics(cap)
	t.Cleanup(func() { SetMetrics(nil) })

	recordDLQ(typeKafka, "idle-events")
	recordDropped(typeMemory, "user-events")
	recordDropped(typeNone, "events")

	if len(cap.dlq) != 1 || cap.dlq[0] != [2]string{"kafka", "idle-events"} {
		t.Fatalf("dlq records = %v, want [[kafka idle-events]]", cap.dlq)
	}
	if len(cap.dropped) != 2 {
		t.Fatalf("dropped records = %v, want 2", cap.dropped)
	}
}

// TestSetMetrics_NilRestoresNoop 验证传 nil 恢复为 no-op（不 panic、不记录）。
func TestSetMetrics_NilRestoresNoop(t *testing.T) {
	SetMetrics(nil)
	// 不应 panic
	recordSend(typeMemory, "user-events", nil)
}

// TestMemoryProducer_RecordsSendMetrics 验证内存生产者 Send/SendBatch 会上报发送成功指标。
func TestMemoryProducer_RecordsSendMetrics(t *testing.T) {
	cap := &captureMetrics{}
	SetMetrics(cap)
	t.Cleanup(func() { SetMetrics(nil) })

	cfg := testMQConfig()
	prod, err := NewMemoryProducer(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := prod.Send(context.Background(), &event.Message{EventType: event.EventShopRedeemed, Key: "u1"}); err != nil {
		t.Fatal(err)
	}
	if err := prod.SendBatch(context.Background(), []*event.Message{
		{EventType: event.EventShopRedeemed, Key: "a"},
		{EventType: event.EventShopRedeemed, Key: "b"},
	}); err != nil {
		t.Fatal(err)
	}

	got := cap.snapshot()
	if len(got) != 3 {
		t.Fatalf("want 3 send records (1 Send + 2 SendBatch), got %d: %v", len(got), got)
	}
	wantTopic := topicMapOf(cfg)[event.EventShopRedeemed]
	for i, r := range got {
		if r[0] != typeMemory || r[1] != wantTopic || r[2] != resultSuccess {
			t.Fatalf("record%d = %v, want memory/%s/success", i, r, wantTopic)
		}
	}
}

// TestRocketSendDefaults 验证 RocketMQ 发送超时/重试的缺省与显式配置。
func TestRocketSendDefaults(t *testing.T) {
	// 未配置：用默认值
	cfg := &config.MQConfig{}
	if to := rocketSendTimeoutOf(cfg); to != defaultRocketSendTimeout {
		t.Fatalf("default send timeout = %v, want %v", to, defaultRocketSendTimeout)
	}
	if rr := rocketSendRetriesOf(cfg); rr != defaultRocketSendRetries {
		t.Fatalf("default send retries = %d, want %d", rr, defaultRocketSendRetries)
	}
	// 显式配置：以配置为准
	cfg.RocketMQ.SendTimeout = 7 * 1e9 // 7s
	cfg.RocketMQ.SendRetries = 5
	if to := rocketSendTimeoutOf(cfg); to != 7*1e9 {
		t.Fatalf("configured send timeout = %v, want 7s", to)
	}
	if rr := rocketSendRetriesOf(cfg); rr != 5 {
		t.Fatalf("configured send retries = %d, want 5", rr)
	}
}
