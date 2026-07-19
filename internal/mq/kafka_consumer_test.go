package mq

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// newTestConsumer 构造一个仅用于白盒测试的消费者：不连接 Broker，DLQ 落临时目录。
func newTestConsumer(t *testing.T, tune ...func(*config.KafkaConsumerConfig)) *KafkaConsumer {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.KafkaConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		Producer: config.KafkaProducerConfig{},
		Consumer: config.KafkaConsumerConfig{
			GroupID:        "test-group",
			RetryMax:       2,
			RetryBaseDelay: 2 * time.Millisecond,
			RetryMaxDelay:  20 * time.Millisecond,
			DLQLocalPath:   dir,
		},
	}
	for _, fn := range tune {
		fn(&cfg.Consumer)
	}
	c, err := NewKafkaConsumer(cfg, nil)
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	return c
}

// readDLQLines 读取消费者 DLQ 文件并逐行解析（忽略空行）。
func readConsumerDLQ(t *testing.T, dir, topic string) []consumerDLQRecord {
	t.Helper()
	fp := filepath.Join(dir, topic+".dlq.jsonl")
	data, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("read dlq %s: %v", fp, err)
	}
	var recs []consumerDLQRecord
	for _, line := range splitLines(data) {
		if len(line) == 0 {
			continue
		}
		var r consumerDLQRecord
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("unmarshal dlq line: %v", err)
		}
		recs = append(recs, r)
	}
	return recs
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

// TestProcess_SuccessNoRetry 验证 handler 直接成功时不重试、不落 DLQ。
func TestProcess_SuccessNoRetry(t *testing.T) {
	c := newTestConsumer(t)
	var calls int32
	handler := func(ctx context.Context, m *event.Message) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}
	msg := kafka.Message{Topic: "ok-topic", Value: mustJSON(t, &event.Message{EventType: "x"})}
	parsed := &event.Message{EventType: "x"}
	c.process(context.Background(), msg, parsed, handler)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("handler called %d times, want 1", got)
	}
	// 成功不应产生 DLQ 文件
	if _, err := os.Stat(filepath.Join(c.cfg.Consumer.DLQLocalPath, "ok-topic.dlq.jsonl")); !os.IsNotExist(err) {
		t.Fatal("DLQ file should not exist on success")
	}
}

// TestProcess_RetryExhaustedThenDLQ 验证 handler 持续失败时按 RetryMax 重试，
// 耗尽后落 DLQ，且 DLQ 记录包含原始消息与错误。
func TestProcess_RetryExhaustedThenDLQ(t *testing.T) {
	c := newTestConsumer(t, func(cc *config.KafkaConsumerConfig) {
		cc.RetryMax = 2 // 期望 handler 被调用 RetryMax+1 = 3 次
	})
	var calls int32
	wantErr := errors.New("boom")
	handler := func(ctx context.Context, m *event.Message) error {
		atomic.AddInt32(&calls, 1)
		return wantErr
	}
	dir := c.cfg.Consumer.DLQLocalPath
	msg := kafka.Message{Topic: "fail-topic", Key: []byte("kkey"), Value: mustJSON(t, &event.Message{EventType: "task.completed", Key: "k1"})}
	parsed := &event.Message{EventType: "task.completed", Key: "k1"}
	c.process(context.Background(), msg, parsed, handler)

	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("handler called %d times, want 3 (RetryMax+1)", got)
	}
	recs := readConsumerDLQ(t, dir, "fail-topic")
	if len(recs) != 1 {
		t.Fatalf("dlq records = %d, want 1", len(recs))
	}
	if recs[0].Error != wantErr.Error() {
		t.Fatalf("dlq error = %q, want %q", recs[0].Error, wantErr.Error())
	}
	// DLQ 记录的 Key 取 kafka 消息级 Key（与 event 内部 Key 区分）
	if recs[0].Key != "kkey" || recs[0].Topic != "fail-topic" {
		t.Fatalf("dlq record mismatch: %+v", recs[0])
	}
}

// TestProcess_CtxCancelGoesToDLQ 验证处理中途 ctx 取消时立即转 DLQ（不阻塞等待退避定时器）。
func TestProcess_CtxCancelGoesToDLQ(t *testing.T) {
	c := newTestConsumer(t, func(cc *config.KafkaConsumerConfig) {
		cc.RetryBaseDelay = time.Hour // 退避极长，确保走 ctx.Done 分支而非定时器
	})
	var calls int32
	handler := func(ctx context.Context, m *event.Message) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("fail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 首试失败后，第二次进入退避即因 ctx 取消而转 DLQ

	dir := c.cfg.Consumer.DLQLocalPath
	msg := kafka.Message{Topic: "cancel-topic", Value: mustJSON(t, &event.Message{EventType: "x"})}
	parsed := &event.Message{EventType: "x"}
	c.process(ctx, msg, parsed, handler)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("handler called %d times, want 1 (must not retry after ctx cancel)", got)
	}
	if recs := readConsumerDLQ(t, dir, "cancel-topic"); len(recs) != 1 {
		t.Fatalf("dlq records = %d, want 1", len(recs))
	}
}

// TestProcess_PoisonMessage 验证无法解析的毒消息直接落 DLQ（不进入重试）。
func TestProcess_PoisonMessage(t *testing.T) {
	c := newTestConsumer(t)
	var calls int32
	handler := func(ctx context.Context, m *event.Message) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}
	dir := c.cfg.Consumer.DLQLocalPath
	msg := kafka.Message{Topic: "poison-topic", Value: []byte("not-json{")}
	// 毒消息经 handle 反序列化失败直接落 DLQ（与线上路径一致；process 已不再负责反序列化）。
	store := newFileProgressStore(filepath.Join(t.TempDir(), "offsets.json"), 0)
	c.handle(context.Background(), store, msg, handler)

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("handler called %d times on poison message, want 0", got)
	}
	recs := readConsumerDLQ(t, dir, "poison-topic")
	if len(recs) != 1 {
		t.Fatalf("dlq records = %d, want 1", len(recs))
	}
	if recs[0].Error == "" {
		t.Fatal("poison dlq record should carry unmarshal error")
	}
}

// TestAppendDLQ_Consumer 直接验证 appendDLQ 落盘格式正确。
func TestAppendDLQ_Consumer(t *testing.T) {
	c := newTestConsumer(t)
	msg := kafka.Message{Topic: "direct", Partition: 1, Offset: 42, Key: []byte("key"), Value: []byte("v")}
	c.appendDLQ(msg, errors.New("cause"))
	recs := readConsumerDLQ(t, c.cfg.Consumer.DLQLocalPath, "direct")
	if len(recs) != 1 {
		t.Fatalf("dlq records = %d, want 1", len(recs))
	}
	r := recs[0]
	if r.Partition != 1 || r.Offset != 42 || r.Key != "key" || r.Value != "v" {
		t.Fatalf("dlq record fields mismatch: %+v", r)
	}
	if r.Error != "cause" {
		t.Fatalf("dlq error = %q, want cause", r.Error)
	}
}

// TestSubscribe_CloseDoesNotHang 验证订阅不可达 broker 时调用 Close() 不会让 Subscribe
// 挂死：closeCh->cancel 解除 FetchMessage 阻塞，reader.Close 经超时兜底返回（C2）。
func TestSubscribe_CloseDoesNotHang(t *testing.T) {
	c := newTestConsumer(t, func(cc *config.KafkaConsumerConfig) {
		cc.CloseTimeout = 200 * time.Millisecond // 极短超时，验证兜底生效
	})
	handler := func(ctx context.Context, m *event.Message) error { return nil }

	subErr := make(chan error, 1)
	go func() {
		// broker 127.0.0.1:1 不可达，FetchMessage 持续报错并退避重试；
		// Close() 触发 cancel 后应解除阻塞并在 reader.Close 超时内返回。
		subErr <- c.Subscribe(context.Background(), []string{"x"}, handler)
	}()

	time.Sleep(50 * time.Millisecond) // 让 Subscribe 进入 FetchMessage 循环
	c.Close()

	select {
	case <-subErr:
		// 正常退出，未挂死
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not return after Close (close timeout not effective)")
	}
}

func mustJSON(t *testing.T, m *event.Message) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestProcess_PanicGoesToDLQ 验证 Kafka handler 内 panic 不会拖垮进程，
// 而是被 recover 为 error 并直接落 DLQ（见 13 §3.39）。
func TestProcess_PanicGoesToDLQ(t *testing.T) {
	c := newTestConsumer(t)
	msg := kafka.Message{Topic: "panic-topic", Value: mustJSON(t, &event.Message{EventType: "x"})}

	// 在子函数内捕获：若 process 未兜底 panic，recover 不会触发，子函数退出后 panic 外泄致测试崩溃。
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("kafka process must recover handler panic, leaked: %v", r)
			}
		}()
		c.process(context.Background(), msg, &event.Message{EventType: "x"}, func(_ context.Context, _ *event.Message) error {
			panic("boom")
		})
	}()

	recs := readConsumerDLQ(t, c.cfg.Consumer.DLQLocalPath, "panic-topic")
	if len(recs) != 1 {
		t.Fatalf("dlq records = %d, want 1 (panicked message must be captured)", len(recs))
	}
	if !strings.Contains(recs[0].Error, "handler panic") {
		t.Fatalf("dlq error = %q, want to contain 'handler panic'", recs[0].Error)
	}
}
