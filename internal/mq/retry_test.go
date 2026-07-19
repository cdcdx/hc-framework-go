package mq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/event"
)

var errTestRetry = errors.New("test retry")

// TestRetryProcess_BackoffNotSkipped 回归：验证消费重试的指数退避不会被跳过。
// 复用一个 time.Timer 时，若首次 handler 执行耗时超过 base 延迟，定时器会在第一次尝试期间提前触发，
// 残留值被后续 Reset+select 立即读到，导致「第二次重试不等待直接打下游」——下游故障/高负载时反而被猛打。
// 修复（sleepWithContext）后，相邻两次重试之间必须真正等待 base 退避。
func TestRetryProcess_BackoffNotSkipped(t *testing.T) {
	base := 50 * time.Millisecond
	maxDelay := 2 * time.Second

	// 首次调用故意耗时超过 base（触发旧实现的 Timer 提前触发路径），之后每次快速失败。
	const firstSlow = 120 * time.Millisecond
	var firstEnd, secondStart int64
	var calls int64

	handler := func(_ context.Context, _ *event.Message) error {
		n := atomic.AddInt64(&calls, 1)
		now := time.Now()
		if n == 1 {
			firstEnd = now.UnixNano() + int64(firstSlow) // 记录「首次返回时刻 + 故意耗时」
			time.Sleep(firstSlow)
		}
		if n == 2 {
			secondStart = time.Now().UnixNano()
		}
		return errTestRetry
	}

	_ = retryProcess(context.Background(), &event.Message{}, handler, 3, base, maxDelay)

	if atomic.LoadInt64(&calls) < 2 {
		t.Fatalf("expected handler called at least twice, got %d", calls)
	}
	gap := time.Duration(secondStart - firstEnd)
	// 修复后 gap ≈ base（50ms）；旧 bug 下 gap ≈ 0（< base/2）。用 base/2 作为下界，留足 CI 调度余量。
	if gap < base/2 {
		t.Fatalf("backoff between retries skipped: gap=%v, want >= %v (base=%v)", gap, base/2, base)
	}
}

// TestRetryProcess_ExhaustsAndReturnsErr 验证重试耗尽后返回最后一次错误（非 nil）。
func TestRetryProcess_ExhaustsAndReturnsErr(t *testing.T) {
	var calls int64
	handler := func(_ context.Context, _ *event.Message) error {
		atomic.AddInt64(&calls, 1)
		return errTestRetry
	}
	err := retryProcess(context.Background(), &event.Message{}, handler, 2, time.Millisecond, 10*time.Millisecond)
	if err != errTestRetry {
		t.Fatalf("retryProcess err = %v, want errTestRetry", err)
	}
	if got := atomic.LoadInt64(&calls); got != 3 { // retryMax=2 → 尝试 0,1,2 共 3 次
		t.Fatalf("handler calls = %d, want 3", got)
	}
}

// TestRetryProcess_CtxCancelReturnsErr 验证 ctx 取消时立即返回 ctx.Err()。
func TestRetryProcess_CtxCancelReturnsErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 直接取消
	var calls int64
	handler := func(_ context.Context, _ *event.Message) error {
		atomic.AddInt64(&calls, 1)
		return errTestRetry
	}
	err := retryProcess(ctx, &event.Message{}, handler, 5, 10*time.Millisecond, time.Second)
	if err != context.Canceled {
		t.Fatalf("retryProcess err = %v, want context.Canceled", err)
	}
}

// TestRetryProcess_PanicRecovered 验证 handler 内 panic 被 recover 为 error 返回，
// 且不会因重试而重复执行（panic 视为不可重试的确定性失败），更不会拖垮调用 goroutine。
func TestRetryProcess_PanicRecovered(t *testing.T) {
	var calls int64
	handler := func(_ context.Context, _ *event.Message) error {
		atomic.AddInt64(&calls, 1)
		panic("handler boomed")
	}
	err := retryProcess(context.Background(), &event.Message{}, handler, 3, time.Millisecond, 10*time.Millisecond)
	if err == nil {
		t.Fatal("retryProcess returned nil err, want panic-as-error")
	}
	if !errors.Is(err, errHandlerPanic) {
		t.Fatalf("retryProcess err = %v, want wrapped errHandlerPanic", err)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("handler calls = %d, want 1 (panic must not be retried)", got)
	}
}
