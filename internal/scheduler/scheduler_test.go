package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestScheduler_IntervalJobRuns 验证按固定间隔重复执行。
func TestScheduler_IntervalJobRuns(t *testing.T) {
	s := New()
	var count int32
	s.AddIntervalJob("interval", 10*time.Millisecond, func(context.Context) {
		atomic.AddInt32(&count, 1)
	})
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(120 * time.Millisecond)
	cancel()
	s.Stop()
	if c := atomic.LoadInt32(&count); c < 2 {
		t.Fatalf("interval job ran %d times, want >= 2", c)
	}
}

// TestScheduler_PeriodJobRunsOnChange 验证周期标识变化时才执行（边界触发，启动不立即执行）。
func TestScheduler_PeriodJobRunsOnChange(t *testing.T) {
	s := New()
	var periodCounter int32
	var ran int32
	s.AddPeriodJob("period", 10*time.Millisecond, func() string {
		return fmt.Sprintf("%d", atomic.AddInt32(&periodCounter, 1))
	}, func(context.Context) {
		atomic.StoreInt32(&ran, 1)
	})
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()
	s.Stop()
	if atomic.LoadInt32(&ran) != 1 {
		t.Fatal("period job should run once on period change")
	}
}

// TestScheduler_StopIdempotent 验证重复 Stop 不 panic。
func TestScheduler_StopIdempotent(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	cancel()
	s.Stop()
	s.Stop()
}

// TestScheduler_ExecRecoversPanic 验证任务 panic 被 recover，不影响调度器。
func TestScheduler_ExecRecoversPanic(t *testing.T) {
	s := New()
	s.exec(context.Background(), &scheduledJob{name: "panic", fn: func(context.Context) {
		panic("boom")
	}})
}

// panicMetricsHook 是测试用 Metrics 钩子，记录被上报 panic 的任务名。
type panicMetricsHook struct {
	got []string
}

func (h *panicMetricsHook) RecordPanic(name string) { h.got = append(h.got, name) }

// TestScheduler_ExecPanicRecordsMetric 守护「调度任务 panic 经钩子上报」：
// 无钩子时静默、有钩子时 RecordPanic 被调用（与消费侧 panic→计数规范一致）。
func TestScheduler_ExecPanicRecordsMetric(t *testing.T) {
	t.Run("hook invoked", func(t *testing.T) {
		s := New()
		h := &panicMetricsHook{}
		s.SetMetricsHook(h)
		s.exec(context.Background(), &scheduledJob{name: "boom-job", fn: func(context.Context) {
			panic("boom")
		}})
		if len(h.got) != 1 || h.got[0] != "boom-job" {
			t.Fatalf("RecordPanic got %v, want [boom-job]", h.got)
		}
	})

	t.Run("nil hook is no-op", func(t *testing.T) {
		s := New()
		s.SetMetricsHook(nil)
		// 不应 panic
		s.exec(context.Background(), &scheduledJob{name: "x", fn: func(context.Context) {
			panic("boom")
		}})
	})
}

// TestParseCronMinute 表驱动验证最简 cron 分钟字段解析。
func TestParseCronMinute(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"*", time.Minute, true},
		{"*/5", 5 * time.Minute, true},
		{"*/15", 15 * time.Minute, true},
		{"30", 60 * time.Minute, true},
		{"", 0, false},
		{"*/0", 0, false},
		{"abc", 0, false},
	}
	for _, tc := range cases {
		d, ok := ParseCronMinute(tc.in)
		if ok != tc.ok {
			t.Fatalf("ParseCronMinute(%q) ok = %v, want %v", tc.in, ok, tc.ok)
		}
		if d != tc.want {
			t.Fatalf("ParseCronMinute(%q) = %v, want %v", tc.in, d, tc.want)
		}
	}
}
