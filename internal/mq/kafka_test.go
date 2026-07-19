package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/sony/gobreaker/v2"
)

func newTestProducer(t *testing.T, tune ...func(*config.KafkaProducerConfig)) *KafkaProducer {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.KafkaConfig{
		Brokers: []string{"127.0.0.1:1"}, // 连接被拒绝，WriteMessages 立即失败（不阻塞、不依赖网络）
		Producer: config.KafkaProducerConfig{
			DLQEnabled:      true,
			DLQLocalPath:    dir,
			DeliveryTimeout: 500 * time.Millisecond,
		},
		Topics: config.KafkaTopicsConfig{
			UserEvents: "user-events",
			IdleEvents: "idle-events",
			TaskEvents: "task-events",
			ShopEvents: "shop-events",
		},
	}
	for _, fn := range tune {
		fn(&cfg.Producer)
	}
	p, err := NewKafkaProducer(cfg, nil)
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}
	return p
}

// TestTopicFor 验证事件类型 -> 主题的映射。
func TestTopicFor(t *testing.T) {
	p := newTestProducer(t)
	defer p.Close()
	cases := map[string]string{
		event.EventUserRegistered: "user-events",
		event.EventUserLoggedIn:   "user-events",
		event.EventIdleSettled:    "idle-events",
		event.EventTaskCompleted:  "task-events",
		event.EventShopRedeemed:   "shop-events",
		"unknown.event":           "events", // 未知类型回落
	}
	for et, want := range cases {
		if got := p.topicFor(&event.Message{EventType: et}); got != want {
			t.Fatalf("topicFor(%q) = %q, want %q", et, got, want)
		}
	}
}

// TestAppendDLQ_Producer 验证生产者 appendDLQ 落盘格式（含原始消息与错误）。
func TestAppendDLQ_Producer(t *testing.T) {
	p := newTestProducer(t)
	dir := p.cfg.Producer.DLQLocalPath
	msg := &event.Message{EventType: event.EventTaskCompleted, Key: "k1", EventID: "e1"}
	// 通过共享 dlq 存储落盘（topic 由事件映射得出，TaskCompleted -> task-events）。
	p.dlq.append("task-events", msg, errors.New("broker down"))

	fp := filepath.Join(dir, "task-events.dlq.jsonl")
	data, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("read dlq: %v", err)
	}
	var rec dlqRecord
	if err := json.Unmarshal(splitLines(data)[0], &rec); err != nil {
		t.Fatalf("unmarshal dlq: %v", err)
	}
	if rec.Message == nil || rec.Message.Key != "k1" || rec.Message.EventID != "e1" {
		t.Fatalf("dlq message mismatch: %+v", rec.Message)
	}
	if rec.Error != "broker down" {
		t.Fatalf("dlq error = %q, want 'broker down'", rec.Error)
	}
}

// TestSendOne_DLQFallback 验证 broker 不可用时 sendOne 降级落 DLQ（消息不丢）。
func TestSendOne_DLQFallback(t *testing.T) {
	p := newTestProducer(t)
	defer p.Close()
	dir := p.cfg.Producer.DLQLocalPath

	// 通过 Send 入队，后台 loop 调用 sendOne（连接被拒绝 -> 失败 -> DLQ）。
	if err := p.Send(context.Background(), &event.Message{
		EventType: event.EventIdleSettled,
		Key:       "uid-7",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	fp := filepath.Join(dir, "idle-events.dlq.jsonl")
	if !waitForFile(t, fp, time.Second) {
		t.Fatal("DLQ file not created within timeout (sendOne did not fall back to DLQ)")
	}
	// 等待文件内容真正落盘（appendDLQ 先建空文件再写入，waitForFile 仅探测存在性，
	// 直接读取可能读到空内容导致 splitLines 越界；此处轮询至非空，消除测试竞态）。
	data := waitForFileContent(t, fp, time.Second)
	var rec dlqRecord
	if err := json.Unmarshal(splitLines(data)[0], &rec); err != nil {
		t.Fatalf("unmarshal dlq: %v", err)
	}
	if rec.Message == nil || rec.Message.Key != "uid-7" {
		t.Fatalf("dlq message mismatch: %+v", rec.Message)
	}
}

// TestSendOne_BreakerOpenNoDLQ 锁死「消除 broker 恢复瞬间额外一跳」的核心不变量：
// 熔断器打开（broker 不可达判定）时，sendOne 不应把消息降级到 DLQ（否则 broker 恢复瞬间本可直接
// 发出的消息会多走「队列→loop→DLQ→replay」一跳），而应返回 errBreakerOpen 交由 loop 保留等待恢复。
func TestSendOne_BreakerOpenNoDLQ(t *testing.T) {
	p := newTestProducer(t)
	defer p.Close()

	// 连续真实失败 5 次（CLOSED 态、连接被拒绝立即失败），触发熔断器打开。
	msg := &event.Message{EventType: event.EventTaskCompleted, Key: "k"}
	for i := 0; i < 5; i++ {
		if err := p.sendOne(context.Background(), msg); err != nil {
			t.Fatalf("sendOne (closed) returned unexpected err: %v", err)
		}
	}
	if p.breaker.State() != gobreaker.StateOpen {
		t.Fatalf("breaker should be OPEN after 5 consecutive failures, got %v", p.breaker.State())
	}

	before := countDLQLines(t, p.cfg.Producer.DLQLocalPath)

	// 熔断打开后再次发送：应返回 errBreakerOpen 且不落 DLQ（交由 loop requeue 等待恢复）。
	recovered := &event.Message{EventType: event.EventTaskCompleted, Key: "recovered"}
	err := p.sendOne(context.Background(), recovered)
	if !errors.Is(err, errBreakerOpen) {
		t.Fatalf("sendOne under OPEN breaker = %v, want errBreakerOpen", err)
	}
	after := countDLQLines(t, p.cfg.Producer.DLQLocalPath)
	if after != before {
		t.Fatalf("OPEN-state sendOne must NOT fall back to DLQ (extra hop); before=%d after=%d", before, after)
	}
}

// countDLQLines 统计目录下所有 *.dlq.jsonl 的非空行总数（跨 topic 的 DLQ 落盘量）。
func countDLQLines(t *testing.T, dir string) int {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, "*.dlq.jsonl"))
	total := 0
	for _, fp := range files {
		data, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		total += len(splitLines(data))
	}
	return total
}

// TestSend_NonBlocking 验证队列满时 Send 立即返回（不阻塞业务关键路径）。
func TestSend_NonBlocking(t *testing.T) {
	p := newTestProducer(t, func(pc *config.KafkaProducerConfig) {
		pc.QueueSize = 1 // 极小队列，便于制造满队列场景
	})
	defer p.Close()

	done := make(chan struct{})
	go func() {
		// 连续投递超过队列容量，验证不阻塞。
		for i := 0; i < 50; i++ {
			_ = p.Send(context.Background(), &event.Message{
				EventType: event.EventTaskCompleted,
				Key:       "k",
			})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on full queue (should return immediately via background goroutine)")
	}
}

// TestSend_OverflowSaturatedToDLQ 验证队列满且溢出池打满（MaxOverflowWorkers=0）时，
// 多余消息直接降级到 DLQ（C3 兜底分支）：Send 立即返回、不无限起 goroutine、消息不丢。
func TestSend_OverflowSaturatedToDLQ(t *testing.T) {
	p := newTestProducer(t, func(pc *config.KafkaProducerConfig) {
		pc.QueueSize = 1
		pc.MaxOverflowWorkers = 0 // 溢出池容量 0 -> 队列满时一律落 DLQ（确定性触发 C3 兜底分支）
	})
	defer p.Close()
	dir := p.cfg.Producer.DLQLocalPath

	before := runtime.NumGoroutine()
	// 第一条入队，其余 99 条超过队列容量 -> 溢出池满 -> 落 DLQ（不阻塞、不丢）。
	for i := 0; i < 100; i++ {
		if err := p.Send(context.Background(), &event.Message{
			EventType: event.EventTaskCompleted,
			Key:       "k",
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	after := runtime.NumGoroutine()
	// 溢出路径不起 goroutine（容量为 0），goroutine 数量不应随消息量无界增长（C3 核心保证）。
	if after-before > 20 {
		t.Fatalf("goroutine count grew by %d under full queue; C3 bound violated", after-before)
	}

	// 先等待后台 loop 把队列里缓冲的消息全部处理完（sendOne 失败落 DLQ），
	// 否则 -race 下可能读到队列尚未排空时的文件内容，导致行数不足（时序敏感断言）。
	waitQueueDrain(t, p, 2*time.Second)

	fp := filepath.Join(dir, "task-events.dlq.jsonl")
	if !waitForFile(t, fp, 2*time.Second) {
		t.Fatal("overflow messages not fallen back to DLQ")
	}
	// 轮询直到 DLQ 行数达到预期（队列排空 + 落盘完成），再次消除读取时刻的时序竞争。
	data := waitForDLQCount(t, fp, 90, 2*time.Second)
	if n := len(splitLines(data)); n < 90 {
		t.Fatalf("expected >=90 DLQ lines after drain, got %d", n)
	}
}

// TestAcksOf / TestCompressionOf 验证配置字符串到枚举的映射。
func TestAcksOf(t *testing.T) {
	if a := acksOf("all"); a != acksOf("-1") {
		t.Fatal("acksOf(all) should equal acksOf(-1)")
	}
	if a := acksOf("1"); a == acksOf("all") {
		t.Fatal("acksOf(1) should differ from acksOf(all)")
	}
	if a := acksOf("bogus"); a != acksOf("all") {
		t.Fatal("acksOf(bogus) should fall back to RequireAll")
	}
}

func TestCompressionOf(t *testing.T) {
	if compressionOf("bogus") != 0 {
		t.Fatal("compressionOf(bogus) should be zero value (no compression)")
	}
	// 各已知算法映射不应为零值（gzip/snappy/lz4/zstd 均为正枚举）。
	for _, algo := range []string{"gzip", "snappy", "lz4", "zstd"} {
		if compressionOf(algo) == 0 {
			t.Fatalf("compressionOf(%q) should be non-zero", algo)
		}
	}
}

// waitQueueDrain 轮询等待生产者的内存发送队列排空（len(queue)==0），即后台 loop 已取出所有缓冲消息。
// 仅用于测试：断言 DLQ 落盘结果前先确保异步路径处理完毕，消除 -race 下的时序竞争。
func waitQueueDrain(t *testing.T, p *KafkaProducer, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(p.queue) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("kafka queue not drained within %s (len=%d)", timeout, len(p.queue))
}

// waitForDLQCount 轮询等待 DLQ 文件非空行数达到 min 行（队列排空 + 落盘完成的综合信号），最多等待 timeout。
// 超时则返回已读到内容（可能不足），交由调用方断言。配合 waitQueueDrain 使用，彻底消除读取时刻的时序竞争。
func waitForDLQCount(t *testing.T, path string, min int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil {
			if n := len(splitLines(data)); n >= min {
				return data
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, _ := os.ReadFile(path)
	return data
}

// waitForFile 轮询等待文件出现，最多等待 timeout。
func waitForFile(t *testing.T, path string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// waitForFileContent 轮询等待文件存在且内容非空（DLQ 先创建空文件再写入，需待内容落盘）。
// 返回完整文件内容；超时仍为空则返回已读到（可能为空）内容，交由调用方断言。
func waitForFileContent(t *testing.T, path string, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil && len(bytes.TrimSpace(data)) > 0 {
			return data
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, _ := os.ReadFile(path)
	return data
}

// TestOffsetStore_SetMonotonic 验证 offset 提交「只增不减」：同一分区消息乱序完成时，
// 较小 offset 后提交不会把进度回退，避免重启后该区间消息被重复消费。
func TestOffsetStore_SetMonotonic(t *testing.T) {
	s := newOffsetStore(t.TempDir()+"/offsets.json", 0)
	s.set("t", 0, 5)
	s.set("t", 0, 3) // 乱序较小 offset，不应回退
	if v, _ := s.get("t", 0); v != 5 {
		t.Fatalf("offset should stay 5 (monotonic), got %d", v)
	}
	s.set("t", 0, 7)
	if v, _ := s.get("t", 0); v != 7 {
		t.Fatalf("offset should advance to 7, got %d", v)
	}
}
