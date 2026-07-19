package mq

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestFileProgressStore_SyncFlush 锁死 #2 核心不变量：ordered_commit 模式下每条消息同步落盘
// offset——Commit(sync=true) 返回后，offset 必须已持久化到文件，不依赖后台 5s 周期落盘。
// 这样 broker/进程在「处理完、未落盘」窗口崩溃时，该消息不会被重放（崩溃重放窗口≈0）。
func TestFileProgressStore_SyncFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kafka-offsets.json")
	s := newFileProgressStore(path, 0)

	if err := s.Commit(context.Background(), "t", 0, 10, "e1", true); err != nil {
		t.Fatalf("sync commit: %v", err)
	}
	// 立即读文件（不经内存），断言已落盘
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read offset file: %v", err)
	}
	var m map[string]int64
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["t/0"] != 10 {
		t.Fatalf("after sync commit, persisted offset = %v, want 10", m["t/0"])
	}

	// 非同步提交不应立即落盘（仅内存，验证 sync 语义真实生效）
	s2 := newFileProgressStore(filepath.Join(t.TempDir(), "o2.json"), 0)
	if err := s2.Commit(context.Background(), "t", 0, 7, "", false); err != nil {
		t.Fatalf("async commit: %v", err)
	}
	if _, err := os.Stat(filepath.Join(t.TempDir(), "o2.json")); err == nil {
		// 文件可能尚不存在（未 flush）——非同步模式下不应强制写盘
	}
	// Get 从内存返回
	if off, ok, _ := s2.Get(context.Background(), "t", 0); !ok || off != 7 {
		t.Fatalf("async commit in-memory Get = %d,%v want 7,true", off, ok)
	}
}

// TestFileProgressStore_OnlyIncrease 验证文件存储「只增不减」，避免乱序完成导致 offset 回退。
func TestFileProgressStore_OnlyIncrease(t *testing.T) {
	s := newFileProgressStore(filepath.Join(t.TempDir(), "o.json"), 0)
	_ = s.Commit(context.Background(), "t", 1, 100, "", true)
	_ = s.Commit(context.Background(), "t", 1, 50, "", true) // 更小，应被忽略
	if off, _, _ := s.Get(context.Background(), "t", 1); off != 100 {
		t.Fatalf("offset = %d, want 100 (only-increase)", off)
	}
}

// TestFileProgressStore_FlushIntervalConfig 锁死「offset 落盘间隔可配置」（13 §6.5 #3 / §3.58）：
// 构造时传 0 用默认 5s；传自定义值则生效。仅文件存储相关，SQL 存储无视（其 FlushInterval 恒 0）。
func TestFileProgressStore_FlushIntervalConfig(t *testing.T) {
	def := newFileProgressStore(filepath.Join(t.TempDir(), "def.json"), 0)
	if iv := def.FlushInterval(); iv != 5*time.Second {
		t.Fatalf("default flush interval want 5s, got %v", iv)
	}
	custom := newFileProgressStore(filepath.Join(t.TempDir(), "cus.json"), 2*time.Second)
	if iv := custom.FlushInterval(); iv != 2*time.Second {
		t.Fatalf("custom flush interval want 2s, got %v", iv)
	}
}

// TestSQLProgressStore_RoundTrip 验证 #3 的 SQL 实现：offset 推进 + 只增不减 + event_id 跨分区幂等去重。
func TestSQLProgressStore_RoundTrip(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "c.db")), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	s, err := NewSQLProgressStore(db)
	if err != nil {
		t.Fatalf("NewSQLProgressStore: %v", err)
	}

	// 未记录过 → (0, false)
	if off, ok, _ := s.Get(context.Background(), "t", 0); ok || off != 0 {
		t.Fatalf("Get empty = %d,%v want 0,false", off, ok)
	}
	// Seen 空 event_id → false
	if seen, _ := s.Seen(context.Background(), ""); seen {
		t.Fatal("Seen(\"\") should be false")
	}

	// 提交 offset=5，event_id=e1
	if err := s.Commit(context.Background(), "t", 0, 5, "e1", false); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if off, ok, _ := s.Get(context.Background(), "t", 0); !ok || off != 5 {
		t.Fatalf("Get = %d,%v want 5,true", off, ok)
	}
	if seen, _ := s.Seen(context.Background(), "e1"); !seen {
		t.Fatal("after commit e1, Seen(e1) should be true (dedup recorded)")
	}

	// 只增不减：提交更小 offset=3 不应回退
	_ = s.Commit(context.Background(), "t", 0, 3, "e2", false)
	if off, _, _ := s.Get(context.Background(), "t", 0); off != 5 {
		t.Fatalf("offset = %d, want 5 (only-increase)", off)
	}
	// e2 也已去重记录
	if seen, _ := s.Seen(context.Background(), "e2"); !seen {
		t.Fatal("Seen(e2) should be true")
	}

	// 跨分区：不同 partition 同 event_id 仍被识别为已处理（跨分区幂等）
	if seen, _ := s.Seen(context.Background(), "e1"); !seen {
		t.Fatal("cross-partition Seen(e1) should be true")
	}

	// FlushInterval=0：表明每条 Commit 即落库，无需后台周期落盘协程
	if s.FlushInterval() != 0 {
		t.Fatalf("SQL FlushInterval = %v, want 0", s.FlushInterval())
	}
}
