package mq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// batchFakeProducer 记录每次 SendSync 调用的批大小与主题，用于验证批量发送把同主题多条消息
// 合并为「一次 SendSync」，跨主题拆成多批（不依赖真实 broker）。
type batchFakeProducer struct {
	calls       int
	batchSizes  []int
	batchTopics []string
	err         error
}

func (f *batchFakeProducer) Start() error    { return nil }
func (f *batchFakeProducer) Shutdown() error { return nil }
func (f *batchFakeProducer) SendSync(_ context.Context, m ...*primitive.Message) (*primitive.SendResult, error) {
	f.calls++
	f.batchSizes = append(f.batchSizes, len(m))
	if len(m) > 0 {
		f.batchTopics = append(f.batchTopics, m[0].Topic)
	}
	return &primitive.SendResult{Status: primitive.SendOK}, f.err
}

// newBatchTestProducer 构造用于批量路径单测的 RocketMQProducer（不走 NewRocketMQProducer，免 broker）：
// 预置内存队列 / 主题 / 熔断 / 关闭 DLQ（disabled，append 安全跳过、不落盘）。
func newBatchTestProducer(cfg *config.MQConfig, p rocketProducer) *RocketMQProducer {
	return &RocketMQProducer{
		cfg: cfg,
		p:   p,
		producerBase: producerBase{
			queue:   make(chan *event.Message, 64),
			topics:  topicMapOf(cfg),
			log:     nopLogger{},
			breaker: newProducerBreaker("t", nopLogger{}),
			dlq:     newDLQStore(typeRocketMQ, false, "", nopLogger{}),
		},
	}
}

// drainQueue 把 n 条消息（按 eventType 生成）压入队列，模拟 loop 之前已入队的状态。
func enqueueN(r *RocketMQProducer, n int, evType string) {
	for i := 0; i < n; i++ {
		r.queue <- &event.Message{
			EventType: evType,
			EventID:   fmt.Sprintf("%s-%d", evType, i),
			Key:       fmt.Sprintf("k-%d", i),
		}
	}
}

// TestRocketMQProducer_BatchSend_SameTopicSingleCall 验证：同主题多条消息经一次 SendSync 批量发送
// （即批量发送的核心目标——把「每条一次 RTT」降为「每批一次 RTT」），且每条消息各计一次发送成功。
func TestRocketMQProducer_BatchSend_SameTopicSingleCall(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq", RocketMQ: config.RocketMQConfig{SendTimeout: time.Second}}
	cap := &captureMetrics{}
	SetMetrics(cap)
	t.Cleanup(func() { SetMetrics(nil) })

	r := newBatchTestProducer(cfg, &batchFakeProducer{})
	batchSize := 5
	enqueueN(r, batchSize, event.EventIdleSettled)

	first := <-r.queue
	r.sendBatch(context.Background(), r.collectBatch(first, batchSize))

	mock := r.p.(*batchFakeProducer)
	if mock.calls != 1 {
		t.Fatalf("want exactly 1 SendSync call (batched), got %d", mock.calls)
	}
	if len(mock.batchSizes) != 1 || mock.batchSizes[0] != batchSize {
		t.Fatalf("want one batch of size %d, got sizes=%v", batchSize, mock.batchSizes)
	}
	wantTopic := r.topicFor(&event.Message{EventType: event.EventIdleSettled})
	if mock.batchTopics[0] != wantTopic {
		t.Fatalf("batch topic = %q, want %q", mock.batchTopics[0], wantTopic)
	}

	got := cap.snapshot()
	if len(got) != batchSize {
		t.Fatalf("want %d send records (message-level), got %d: %v", batchSize, len(got), got)
	}
	for _, rec := range got {
		if rec[2] != resultSuccess {
			t.Fatalf("want success, got %v", rec)
		}
		if rec[1] != wantTopic {
			t.Fatalf("record topic = %q, want %q", rec[1], wantTopic)
		}
	}
}

// TestRocketMQProducer_BatchSend_SplitsByTopic 验证：跨主题的一批消息按主题分组，逐主题各调用一次
// SendSync（RocketMQ 批量发送要求同批同主题），且总消息数不丢、各主题数量正确。
func TestRocketMQProducer_BatchSend_SplitsByTopic(t *testing.T) {
	// 显式配置事件→主题映射，确保两个事件映射到不同主题（空 Kafka.Topics 时会都回落 "events" 被合并）。
	cfg := &config.MQConfig{
		Type: "rocketmq",
		Kafka: config.KafkaConfig{Topics: config.KafkaTopicsConfig{
			IdleEvents: "idle-events",
			ShopEvents: "shop-events",
		}},
		RocketMQ: config.RocketMQConfig{SendTimeout: time.Second},
	}
	cap := &captureMetrics{}
	SetMetrics(cap)
	t.Cleanup(func() { SetMetrics(nil) })

	r := newBatchTestProducer(cfg, &batchFakeProducer{})
	const idleN, shopN = 3, 2
	enqueueN(r, idleN, event.EventIdleSettled)
	enqueueN(r, shopN, event.EventShopRedeemed)

	batchSize := 32 // 足够大，把 5 条一次性取全
	first := <-r.queue
	r.sendBatch(context.Background(), r.collectBatch(first, batchSize))

	mock := r.p.(*batchFakeProducer)
	if mock.calls != 2 {
		t.Fatalf("want 2 SendSync calls (one per topic), got %d", mock.calls)
	}
	total := 0
	for _, s := range mock.batchSizes {
		total += s
	}
	if total != idleN+shopN {
		t.Fatalf("total messages across batches = %d, want %d (lost messages?)", total, idleN+shopN)
	}
	wantIdle := r.topicFor(&event.Message{EventType: event.EventIdleSettled})
	wantShop := r.topicFor(&event.Message{EventType: event.EventShopRedeemed})
	seen := map[string]int{}
	for i, tp := range mock.batchTopics {
		seen[tp] = mock.batchSizes[i]
	}
	if seen[wantIdle] != idleN {
		t.Fatalf("idle topic batch size = %d, want %d", seen[wantIdle], idleN)
	}
	if seen[wantShop] != shopN {
		t.Fatalf("shop topic batch size = %d, want %d", seen[wantShop], shopN)
	}
	if len(cap.snapshot()) != idleN+shopN {
		t.Fatalf("want %d success records, got %d", idleN+shopN, len(cap.snapshot()))
	}
}

// TestRocketMQProducer_BatchSend_FailureDowngradesDLQ 验证：整批真实发送失败时，整组降级本地 DLQ
// （每条计一次发送失败 + 一次 DLQ），不丢消息、不误计成功；与 sendOne 单条失败语义对齐。
func TestRocketMQProducer_BatchSend_FailureDowngradesDLQ(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq", RocketMQ: config.RocketMQConfig{SendTimeout: time.Second}}
	cap := &captureMetrics{}
	SetMetrics(cap)
	t.Cleanup(func() { SetMetrics(nil) })

	const n = 4
	r := newBatchTestProducer(cfg, &batchFakeProducer{err: errors.New("broker down")})
	enqueueN(r, n, event.EventIdleSettled)

	first := <-r.queue
	r.sendBatch(context.Background(), r.collectBatch(first, n))

	mock := r.p.(*batchFakeProducer)
	if mock.calls != 1 {
		t.Fatalf("want 1 SendSync attempt, got %d", mock.calls)
	}
	// 每条消息计 1 次发送失败
	failRecs := 0
	for _, rec := range cap.snapshot() {
		if rec[2] == resultFailure {
			failRecs++
		}
		if rec[2] == resultSuccess {
			t.Fatalf("batch failure must not record success: %v", rec)
		}
	}
	if failRecs != n {
		t.Fatalf("want %d failure send records, got %d", n, failRecs)
	}
	// 每条消息计 1 次 DLQ 降级
	if len(cap.dlq) != n {
		t.Fatalf("want %d DLQ records (whole group downgraded), got %d", n, len(cap.dlq))
	}
}

// TestRocketMQProducer_CollectBatch_NonBlocking 验证 collectBatch 非阻塞：
// 队列不足 batchSize 时立即返回当前所有消息（不引入额外等待延迟，低吞吐退化为单条发送）。
func TestRocketMQProducer_CollectBatch_NonBlocking(t *testing.T) {
	cfg := &config.MQConfig{Type: "rocketmq"}
	r := newBatchTestProducer(cfg, &batchFakeProducer{})

	// 空队列：仅 first 一条，立即返回（不阻塞）。
	first := &event.Message{EventType: event.EventIdleSettled, EventID: "e0"}
	single := r.collectBatch(first, 32)
	if len(single) != 1 {
		t.Fatalf("empty queue: want batch len 1 (just first), got %d", len(single))
	}

	// 预填 3 条：first 取 1 条后 collectBatch 再取剩余 2 条，共 3 条，未到 batchSize=32，立即返回（不阻塞）。
	enqueueN(r, 3, event.EventIdleSettled)
	first2 := <-r.queue
	partial := r.collectBatch(first2, 32)
	if len(partial) != 3 {
		t.Fatalf("partial queue: want batch len 3, got %d (blocking wait would deadlock)", len(partial))
	}
}
