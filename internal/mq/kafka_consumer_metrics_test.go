package mq

import (
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// captureConsumerMetrics 是测试用的 ConsumerMetrics 实现，记录每次上报的标签与值。
type captureConsumerMetrics struct {
	mu       sync.Mutex
	consumed [][3]string // type, topic, result
	fetchErr [][2]string // type, topic
	lag      []cmRecord
	offset   []cmRecord
	dlq      []cmRecord // type, topic, backlog（partition 未用）
}

type cmRecord struct {
	mqType    string
	topic     string
	partition int
	value     int64
}

func (m *captureConsumerMetrics) IncConsumed(mqType, topic, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumed = append(m.consumed, [3]string{mqType, topic, result})
}

func (m *captureConsumerMetrics) IncFetchError(mqType, topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchErr = append(m.fetchErr, [2]string{mqType, topic})
}

func (m *captureConsumerMetrics) SetLag(mqType, topic string, partition int, lag int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lag = append(m.lag, cmRecord{mqType, topic, partition, lag})
}

func (m *captureConsumerMetrics) SetOffset(mqType, topic string, partition int, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.offset = append(m.offset, cmRecord{mqType, topic, partition, offset})
}

func (m *captureConsumerMetrics) SetDLQBacklog(mqType, topic string, backlog int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlq = append(m.dlq, cmRecord{mqType, topic, 0, backlog})
}

func (m *captureConsumerMetrics) findLag(topic string, partition int) (cmRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.lag {
		if r.topic == topic && r.partition == partition {
			return r, true
		}
	}
	return cmRecord{}, false
}

// TestConsumerMetrics_SetLagOffset 验证 SetLag/SetOffset 经钩子落到正确标签。
func TestConsumerMetrics_SetLagOffset(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	setConsumerLag(typeKafka, "idle-events", 3, 99)
	setConsumerOffset(typeKafka, "idle-events", 3, 42)

	lag, ok := cap.findLag("idle-events", 3)
	if !ok {
		t.Fatalf("lag record for idle-events/3 missing: %+v", cap.lag)
	}
	if lag.value != 99 || lag.mqType != typeKafka {
		t.Fatalf("lag = %+v, want mqType=kafka value=99", lag)
	}
	if len(cap.offset) != 1 || cap.offset[0] != (cmRecord{typeKafka, "idle-events", 3, 42}) {
		t.Fatalf("offset = %+v, want kafka/idle-events/3/42", cap.offset)
	}
}

// TestRecordConsumed_Results 验证三种消费结果（processed/dedup_skipped/dlq）分别计到对应 result。
func TestRecordConsumed_Results(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	recordConsumed(typeKafka, "shop-events", resultProcessed)
	recordConsumed(typeKafka, "shop-events", resultDedupSkipped)
	recordConsumed(typeKafka, "shop-events", resultDLQ)

	want := [][3]string{
		{typeKafka, "shop-events", resultProcessed},
		{typeKafka, "shop-events", resultDedupSkipped},
		{typeKafka, "shop-events", resultDLQ},
	}
	cap.mu.Lock()
	got := make([][3]string, len(cap.consumed))
	copy(got, cap.consumed)
	cap.mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("want 3 consumed records, got %d: %v", len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("consumed[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestRecordConsumeFetchError 验证 fetch 错误计数经钩子上报，且覆盖 kafka/rabbitmq/rocketmq 三后端
// （memory 为进程内总线无 broker 拉取，不报；该跨后端口径由 rabbitmq.go/rocketmq.go 的失败分支调用）。
func TestRecordConsumeFetchError(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	for _, tc := range []struct {
		mqType, topic string
	}{
		{typeKafka, "user-events"},
		{typeRabbitMQ, "shop-events"},
		{typeRocketMQ, "idle-events"},
	} {
		recordConsumeFetchError(tc.mqType, tc.topic)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	want := [][2]string{
		{typeKafka, "user-events"},
		{typeRabbitMQ, "shop-events"},
		{typeRocketMQ, "idle-events"},
	}
	if len(cap.fetchErr) != len(want) {
		t.Fatalf("fetchErr = %v, want %v", cap.fetchErr, want)
	}
	for i := range want {
		if cap.fetchErr[i] != want[i] {
			t.Fatalf("fetchErr[%d] = %v, want %v", i, cap.fetchErr[i], want[i])
		}
	}
}

// TestSetConsumerMetrics_NilRestoresNoop 验证传 nil 恢复为 no-op（不 panic）。
func TestSetConsumerMetrics_NilRestoresNoop(t *testing.T) {
	SetConsumerMetrics(nil)
	// 不应 panic
	recordConsumed(typeKafka, "user-events", resultProcessed)
	setConsumerLag(typeKafka, "user-events", 0, 1)
	setConsumerOffset(typeKafka, "user-events", 0, 1)
	recordConsumeFetchError(typeKafka, "user-events")
}

// TestClampLag 验证瞬时负 lag 被钳到 0，正常非负值原样返回。
func TestClampLag(t *testing.T) {
	cases := []struct {
		in, want int64
	}{
		{5, 5}, {1, 1}, {0, 0}, {-1, 0}, {-100, 0},
	}
	for _, c := range cases {
		if got := clampLag(c.in); got != c.want {
			t.Fatalf("clampLag(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestRecordConsumed_AllMQTypes 验证 kafka/rabbitmq/rocketmq/memory 四种总线的消费计数都经
// 同一 ConsumerMetrics 钩子上报（补全 §3.62 前非 Kafka 后端缺失消费指标的问题）。
func TestRecordConsumed_AllMQTypes(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	types := []string{typeKafka, typeRabbitMQ, typeRocketMQ, typeMemory}
	results := []string{resultProcessed, resultDedupSkipped, resultDLQ}
	for _, mt := range types {
		for _, res := range results {
			recordConsumed(mt, "topic-x", res)
		}
	}

	cap.mu.Lock()
	got := make([][3]string, len(cap.consumed))
	copy(got, cap.consumed)
	cap.mu.Unlock()

	want := len(types) * len(results)
	if len(got) != want {
		t.Fatalf("want %d consumed records, got %d", want, len(got))
	}
	seen := make(map[[3]string]bool, len(got))
	for _, g := range got {
		seen[g] = true
	}
	for _, mt := range types {
		for _, res := range results {
			if !seen[[3]string{mt, "topic-x", res}] {
				t.Fatalf("missing consumed record type=%s result=%s", mt, res)
			}
		}
	}
}

// TestScrapeConsumerLag_DrivesMetrics 验证抓取循环（scrapeConsumerLag）驱动 lag/offset 指标上报。
// kafka.Reader.Stats() 为无连接读取内部统计，无需真实 broker（未 fetch 时 Lag/Offset 为 0）。
func TestScrapeConsumerLag_DrivesMetrics(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"127.0.0.1:1"}, // 不可达即可，Stats 不建连
		Topic:     "idle-events",
		Partition: 2,
	})
	defer r.Close()

	scrapeConsumerLag([]readerMeta{{r: r, topic: "idle-events", partition: 2}})

	if _, ok := cap.findLag("idle-events", 2); !ok {
		t.Fatalf("expected lag record for idle-events/2, got lag=%+v offset=%+v", cap.lag, cap.offset)
	}
	if len(cap.offset) != 1 || cap.offset[0].partition != 2 || cap.offset[0].topic != "idle-events" {
		t.Fatalf("offset = %+v, want idle-events/2", cap.offset)
	}
}

// TestNewKafkaConsumer_GroupModeRejected 验证显式 mode=group 因 KRaft 不兼容启动报错（§3.60）。
func TestNewKafkaConsumer_GroupModeRejected(t *testing.T) {
	cfg := &config.KafkaConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		Consumer: config.KafkaConsumerConfig{Mode: "group"},
	}
	if _, err := NewKafkaConsumer(cfg, nil); err == nil {
		t.Fatalf("expected error for mode=group, got nil")
	}
}

// TestNewKafkaConsumer_DirectModeOK 验证 mode=direct / 空值均能正常构造（§3.60）。
func TestNewKafkaConsumer_DirectModeOK(t *testing.T) {
	for _, mode := range []string{"", "direct", "DIRECT"} {
		cfg := &config.KafkaConfig{
			Brokers:  []string{"127.0.0.1:9092"},
			Consumer: config.KafkaConsumerConfig{Mode: mode},
		}
		if _, err := NewKafkaConsumer(cfg, nil); err != nil {
			t.Fatalf("mode=%q should be accepted, got error: %v", mode, err)
		}
	}
}

// TestNewKafkaConsumer_UnknownModeWarnsButOK 验证未知 mode 回落 direct（不报错，仅告警）。
func TestNewKafkaConsumer_UnknownModeWarnsButOK(t *testing.T) {
	cl := &capturingLogger{}
	cfg := &config.KafkaConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		Consumer: config.KafkaConsumerConfig{Mode: "weird"},
	}
	k, err := NewKafkaConsumer(cfg, cl)
	if err != nil {
		t.Fatalf("unknown mode should fall back to direct, got error: %v", err)
	}
	if k == nil {
		t.Fatalf("consumer must not be nil")
	}
	if !cl.containsWarn("unknown consumer.mode") {
		t.Fatalf("expected warn about unknown consumer.mode, got warns: %v", cl.warns)
	}
}
