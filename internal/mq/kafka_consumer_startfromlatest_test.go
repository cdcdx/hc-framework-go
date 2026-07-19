package mq

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// capturingLogger 收集 Warnf / Infof 输出，便于断言各类启动期告警（13 §6.5 #6/#4/#8）。
// 线程安全：消费侧多个 goroutine 并发写、测试轮询读，必须用锁保护切片。
type capturingLogger struct {
	mu    sync.Mutex
	warns []string
	infos []string
}

func (l *capturingLogger) Infof(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, format)
}
func (l *capturingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, format)
}
func (l *capturingLogger) Errorf(string, ...interface{}) {}

// containsWarn / containsInfo：加锁读取，供并发场景下的断言轮询使用。
func (l *capturingLogger) containsWarn(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, w := range l.warns {
		if containsSub(w, sub) {
			return true
		}
	}
	return false
}
func (l *capturingLogger) containsInfo(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, i := range l.infos {
		if containsSub(i, sub) {
			return true
		}
	}
	return false
}

func newTestKafkaConsumer(t *testing.T, cc config.KafkaConsumerConfig) (*KafkaConsumer, *capturingLogger) {
	t.Helper()
	cl := &capturingLogger{}
	cfg := &config.KafkaConfig{Brokers: []string{"127.0.0.1:9092"}, Consumer: cc}
	c, err := NewKafkaConsumer(cfg, cl)
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	return c, cl
}

func TestWarnStartFromLatest_OffsetFileExists(t *testing.T) {
	dir := t.TempDir()
	offsetPath := filepath.Join(dir, "kafka-offsets.json")
	if err := os.WriteFile(offsetPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	// start_from_latest=true 且 offset 文件已存在 → 应告警
	c, cl := newTestKafkaConsumer(t, config.KafkaConsumerConfig{StartFromLatest: true})
	c.warnStartFromLatestIfOffsetExists(offsetPath)
	if len(cl.warns) != 1 {
		t.Fatalf("expected 1 warn when start_from_latest=true and offset file exists, got %d: %v", len(cl.warns), cl.warns)
	}
	if !containsSub(cl.warns[0], "has NO effect") {
		t.Fatalf("warn message unexpected: %q", cl.warns[0])
	}
}

func TestWarnStartFromLatest_OffsetFileAbsent(t *testing.T) {
	// start_from_latest=true 但 offset 文件不存在（首次启动）→ 不告警（此时确会跳过积压）
	c, cl := newTestKafkaConsumer(t, config.KafkaConsumerConfig{StartFromLatest: true})
	c.warnStartFromLatestIfOffsetExists(filepath.Join(t.TempDir(), "kafka-offsets.json"))
	if len(cl.warns) != 0 {
		t.Fatalf("expected no warn when offset file absent, got %v", cl.warns)
	}
}

func TestWarnStartFromLatest_FalseNoWarn(t *testing.T) {
	// start_from_latest=false 即使 offset 文件存在也不告警（语义本就从头续读）
	dir := t.TempDir()
	offsetPath := filepath.Join(dir, "kafka-offsets.json")
	_ = os.WriteFile(offsetPath, []byte("{}"), 0o644)
	c, cl := newTestKafkaConsumer(t, config.KafkaConsumerConfig{StartFromLatest: false})
	c.warnStartFromLatestIfOffsetExists(offsetPath)
	if len(cl.warns) != 0 {
		t.Fatalf("expected no warn when start_from_latest=false, got %v", cl.warns)
	}
}

func TestWarnStartFromLatest_PerInstancePath(t *testing.T) {
	// 多实例下应检查「本实例」的 offset 文件（扩展名前加 .<id>），而非共享文件
	dir := t.TempDir()
	base := filepath.Join(dir, "kafka-offsets.json")
	// 仅本实例（id=2）的文件存在，共享 base 文件不存在
	instPath := filepath.Join(dir, "kafka-offsets.2.json")
	if err := os.WriteFile(instPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, cl := newTestKafkaConsumer(t, config.KafkaConsumerConfig{StartFromLatest: true, InstanceCount: 3, InstanceID: 2})
	got := c.instanceOffsetPath(base)
	if got != instPath {
		t.Fatalf("instanceOffsetPath want %q got %q", instPath, got)
	}
	c.warnStartFromLatestIfOffsetExists(got)
	if len(cl.warns) != 1 {
		t.Fatalf("expected 1 warn for this instance's offset file, got %d: %v", len(cl.warns), cl.warns)
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
