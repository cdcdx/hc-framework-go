package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/event"
)

// countLines 统计文件非空行数（DLQ 为 JSONL）。文件不存在返回 0。
func countLines(t *testing.T, fp string) int {
	t.Helper()
	data, err := os.ReadFile(fp)
	if err != nil {
		return 0
	}
	n := 0
	for _, l := range splitLines(data) {
		if len(l) > 0 {
			n++
		}
	}
	return n
}

// TestDLQ_MaxBytesDropsMessage 验证单文件字节上限：超限后 append 丢弃新消息（不再无限增长）。
func TestDLQ_MaxBytesDropsMessage(t *testing.T) {
	dir := t.TempDir()
	d := newDLQStore(typeKafka, true, dir, nil)
	d.maxBytes = 300 // 极小上限，几条即触顶

	topic := "task-events"
	for i := 0; i < 100; i++ {
		d.append(topic, &event.Message{EventType: event.EventTaskCompleted, Key: fmt.Sprintf("k%d", i)}, fmt.Errorf("broker down"))
	}
	fi, err := os.Stat(d.pathFor(topic))
	if err != nil {
		t.Fatalf("stat dlq: %v", err)
	}
	if fi.Size() > d.maxBytes {
		t.Fatalf("dlq file size %d exceeds max %d (size cap not enforced)", fi.Size(), d.maxBytes)
	}
	if n := countLines(t, d.pathFor(topic)); n == 0 || n >= 100 {
		t.Fatalf("expected some (but not all) messages persisted under size cap, got %d lines", n)
	}
}

// TestDLQ_ReplayBatchLimit 验证单次 replay 批量上限：一轮只补发 replayBatchMax 条，其余保留。
func TestDLQ_ReplayBatchLimit(t *testing.T) {
	dir := t.TempDir()
	d := newDLQStore(typeKafka, true, dir, nil)
	d.replayBatchMax = 5

	topic := "user-events"
	for i := 0; i < 20; i++ {
		d.append(topic, &event.Message{EventType: event.EventUserRegistered, Key: fmt.Sprintf("u%d", i)}, fmt.Errorf("down"))
	}

	var relayed int64
	relay := func(_ context.Context, _ *event.Message) error {
		atomic.AddInt64(&relayed, 1)
		return nil
	}
	d.replayAll(relay)
	if got := atomic.LoadInt64(&relayed); got != 5 {
		t.Fatalf("first replay round: want 5 relayed (batch cap), got %d", got)
	}
	if n := countLines(t, d.pathFor(topic)); n != 15 {
		t.Fatalf("after 1st round: want 15 remaining, got %d", n)
	}
	// 再跑三轮补完剩余 15 条（5+5+5）。
	for i := 0; i < 3; i++ {
		d.replayAll(relay)
	}
	if got := atomic.LoadInt64(&relayed); got != 20 {
		t.Fatalf("after 4 rounds: want 20 relayed total, got %d", got)
	}
	if n := countLines(t, d.pathFor(topic)); n != 0 {
		t.Fatalf("after all rounds: want 0 remaining, got %d", n)
	}
	if _, err := os.Stat(d.pathFor(topic)); !os.IsNotExist(err) {
		t.Fatalf("empty dlq file should be removed, stat err=%v", err)
	}
}

// TestDLQ_ReplayFailureKeepsRemaining 验证 relay 失败时剩余消息保留（不丢），下轮可继续。
func TestDLQ_ReplayFailureKeepsRemaining(t *testing.T) {
	dir := t.TempDir()
	d := newDLQStore(typeKafka, true, dir, nil)
	topic := "idle-events"
	for i := 0; i < 10; i++ {
		d.append(topic, &event.Message{EventType: event.EventIdleSettled, Key: fmt.Sprintf("i%d", i)}, fmt.Errorf("down"))
	}
	// relay 始终失败：首条即 stop，全部保留。
	d.replayAll(func(_ context.Context, _ *event.Message) error { return fmt.Errorf("still down") })
	if n := countLines(t, d.pathFor(topic)); n != 10 {
		t.Fatalf("all messages should be preserved on relay failure, got %d", n)
	}
	// broker 恢复：全部补发成功，文件清空。
	d.replayAll(func(_ context.Context, _ *event.Message) error { return nil })
	if n := countLines(t, d.pathFor(topic)); n != 0 {
		t.Fatalf("all messages should be replayed after recovery, got %d", n)
	}
}

// TestDLQ_ConcurrentAppendReplayNoLoss 验证并发 append 与 replay 不丢消息（修 read-modify-write 竞争）。
// 一批消息在 replay 进行中持续 append；relay 收集补发的 key，最终「补发 + 残留」应等于全部 append 的消息，无丢失。
func TestDLQ_ConcurrentAppendReplayNoLoss(t *testing.T) {
	dir := t.TempDir()
	d := newDLQStore(typeKafka, true, dir, nil)
	topic := "shop-events"
	const total = 500

	var relayedMu sync.Mutex
	relayed := make(map[string]struct{})
	relay := func(_ context.Context, m *event.Message) error {
		relayedMu.Lock()
		relayed[m.Key] = struct{}{}
		relayedMu.Unlock()
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 持续 append
		defer wg.Done()
		for i := 0; i < total; i++ {
			d.append(topic, &event.Message{EventType: event.EventShopRedeemed, Key: fmt.Sprintf("s%d", i)}, fmt.Errorf("down"))
		}
	}()
	wg.Add(1)
	go func() { // 并发多轮 replay
		defer wg.Done()
		for i := 0; i < 50; i++ {
			d.replayAll(relay)
		}
	}()
	wg.Wait()
	// 收尾：把剩余全部补发。
	d.replayAll(relay)

	remaining := countLines(t, d.pathFor(topic))
	relayedMu.Lock()
	relayedCount := len(relayed)
	relayedMu.Unlock()
	if relayedCount+remaining < total {
		t.Fatalf("message loss detected: relayed=%d + remaining=%d < total=%d", relayedCount, remaining, total)
	}
	if remaining != 0 {
		t.Fatalf("final replay should drain all, got %d remaining", remaining)
	}
	if relayedCount != total {
		t.Fatalf("want all %d unique keys relayed, got %d", total, relayedCount)
	}
}

// TestDLQ_RecoverLeftover 验证进程崩溃遗留的 .replaying 快照会被并回主文件并重放。
func TestDLQ_RecoverLeftover(t *testing.T) {
	dir := t.TempDir()
	d := newDLQStore(typeKafka, true, dir, nil)
	topic := "task-events"

	// 手造遗留快照（模拟上次 rename 后进程崩溃、未删除）。
	rec := dlqRecord{Message: &event.Message{EventType: event.EventTaskCompleted, Key: "leftover"}, Error: "down"}
	raw, _ := json.Marshal(rec)
	leftover := d.pathFor(topic) + dlqReplayingSuffix
	if err := os.WriteFile(leftover, append(raw, '\n'), 0o644); err != nil {
		t.Fatalf("write leftover: %v", err)
	}

	var relayed int64
	d.replayAll(func(_ context.Context, m *event.Message) error {
		if m.Key == "leftover" {
			atomic.AddInt64(&relayed, 1)
		}
		return nil
	})
	if atomic.LoadInt64(&relayed) != 1 {
		t.Fatalf("leftover snapshot not recovered/replayed, relayed=%d", relayed)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("leftover snapshot should be removed after recovery, stat err=%v", err)
	}
}

// TestDLQ_DisabledNoop 验证未启用时 append / replay 均为空操作（不建文件、不 panic）。
func TestDLQ_DisabledNoop(t *testing.T) {
	dir := t.TempDir()
	d := newDLQStore(typeKafka, false, dir, nil)
	d.append("t", &event.Message{EventType: event.EventTaskCompleted}, fmt.Errorf("x"))
	d.replayAll(func(_ context.Context, _ *event.Message) error { return nil })
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(files) != 0 {
		t.Fatalf("disabled dlq should not create files, got %v", files)
	}
}
