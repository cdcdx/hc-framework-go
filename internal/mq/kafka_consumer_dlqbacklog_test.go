package mq

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// findDLQ 返回某 topic 最后一次上报的 DLQ backlog 值。
func (m *captureConsumerMetrics) findDLQ(topic string) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var v int64
	var ok bool
	for _, r := range m.dlq { // 取最后一次（gauge 语义）
		if r.topic == topic {
			v, ok = r.value, true
		}
	}
	return v, ok
}

// TestAppendDLQ_BacklogGauge 验证 appendDLQ 落盘成功后按 topic 累加并上报存量 gauge。
// 不依赖 broker：直接构造 KafkaConsumer 并调用 appendDLQ。
func TestAppendDLQ_BacklogGauge(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	dir := t.TempDir()
	c := &KafkaConsumer{
		cfg: &config.KafkaConfig{Consumer: config.KafkaConsumerConfig{DLQLocalPath: dir}},
		log: nopLogger{},
	}

	// 两条 topic-a、一条 topic-b。
	c.appendDLQ(kafka.Message{Topic: "topic-a", Value: []byte(`{"x":1}`)}, errTest)
	c.appendDLQ(kafka.Message{Topic: "topic-a", Value: []byte(`{"x":2}`)}, errTest)
	c.appendDLQ(kafka.Message{Topic: "topic-b", Value: []byte(`{"y":1}`)}, errTest)

	if v, ok := cap.findDLQ("topic-a"); !ok || v != 2 {
		t.Fatalf("topic-a backlog = %d ok=%v, want 2", v, ok)
	}
	if v, ok := cap.findDLQ("topic-b"); !ok || v != 1 {
		t.Fatalf("topic-b backlog = %d ok=%v, want 1", v, ok)
	}

	// 文件确实落盘（默认 suffix ".dlq"）。
	if _, err := os.Stat(filepath.Join(dir, "topic-a.dlq.jsonl")); err != nil {
		t.Fatalf("topic-a dlq file missing: %v", err)
	}
}

// TestAppendDLQ_NoPathNoGauge 验证未配置 DLQLocalPath 时 appendDLQ 直接返回、不上报。
func TestAppendDLQ_NoPathNoGauge(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	c := &KafkaConsumer{
		cfg: &config.KafkaConfig{Consumer: config.KafkaConsumerConfig{DLQLocalPath: ""}},
		log: nopLogger{},
	}
	c.appendDLQ(kafka.Message{Topic: "topic-a", Value: []byte(`{"x":1}`)}, errTest)

	if _, ok := cap.findDLQ("topic-a"); ok {
		t.Fatalf("expected no backlog report when DLQLocalPath empty, got dlq=%+v", cap.dlq)
	}
}

// TestInitDLQBacklog_ScanExisting 验证启动扫描已有 DLQ 文件行数初始化存量（跨重启存量可见）。
func TestInitDLQBacklog_ScanExisting(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	dir := t.TempDir()
	// 预置：topic-a 3 行（含一空行应被忽略）、topic-b 1 行。
	if err := os.WriteFile(filepath.Join(dir, "topic-a.dlq.jsonl"),
		[]byte("{\"x\":1}\n{\"x\":2}\n\n{\"x\":3}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "topic-b.dlq.jsonl"),
		[]byte("{\"y\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 非 DLQ 文件不应被计入。
	if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("noise\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &KafkaConsumer{
		cfg: &config.KafkaConfig{Consumer: config.KafkaConsumerConfig{DLQLocalPath: dir}},
		log: nopLogger{},
	}
	c.initDLQBacklog(dir, "") // 空 suffix 回落 ".dlq"

	if v, ok := cap.findDLQ("topic-a"); !ok || v != 3 {
		t.Fatalf("topic-a scanned backlog = %d ok=%v, want 3", v, ok)
	}
	if v, ok := cap.findDLQ("topic-b"); !ok || v != 1 {
		t.Fatalf("topic-b scanned backlog = %d ok=%v, want 1", v, ok)
	}

	// 扫描后再 append 应在已有基数上继续累加（3 -> 4）。
	c.appendDLQ(kafka.Message{Topic: "topic-a", Value: []byte(`{"x":4}`)}, errTest)
	if v, ok := cap.findDLQ("topic-a"); !ok || v != 4 {
		t.Fatalf("topic-a backlog after append = %d ok=%v, want 4", v, ok)
	}
}

// TestInitDLQBacklog_CustomSuffix 验证自定义 suffix 下的文件名反解与统计。
func TestInitDLQBacklog_CustomSuffix(t *testing.T) {
	cap := &captureConsumerMetrics{}
	SetConsumerMetrics(cap)
	t.Cleanup(func() { SetConsumerMetrics(nil) })

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "orders.dead.jsonl"),
		[]byte("{\"a\":1}\n{\"a\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &KafkaConsumer{
		cfg: &config.KafkaConfig{Consumer: config.KafkaConsumerConfig{DLQLocalPath: dir, DLQSuffix: ".dead"}},
		log: nopLogger{},
	}
	c.initDLQBacklog(dir, ".dead")

	if v, ok := cap.findDLQ("orders"); !ok || v != 2 {
		t.Fatalf("orders scanned backlog = %d ok=%v, want 2", v, ok)
	}
}

var errTest = errTestErr{}

type errTestErr struct{}

func (errTestErr) Error() string { return "test error" }
