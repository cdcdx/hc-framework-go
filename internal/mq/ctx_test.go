package mq

import (
	"context"
	"testing"
	"time"
)

// TestCtxWithTimeoutIfUnset_WithDeadline 已带截止时间的 ctx 应原样返回且 cancel 为空操作。
func TestCtxWithTimeoutIfUnset_WithDeadline(t *testing.T) {
	base, baseCancel := context.WithTimeout(context.Background(), time.Second)
	defer baseCancel()
	got, cancel := ctxWithTimeoutIfUnset(base, 5*time.Second)
	if got != base {
		t.Fatalf("expected same ctx when deadline present")
	}
	cancel() // 必须是空操作，不能取消 base
	if base.Err() != nil {
		t.Fatalf("cancel must be no-op for a ctx that already had a deadline, got err=%v", base.Err())
	}
}

// TestCtxWithTimeoutIfUnset_WithoutDeadline 无截止时间的 ctx 应派生出带超时的子 ctx，且 cancel 生效。
func TestCtxWithTimeoutIfUnset_WithoutDeadline(t *testing.T) {
	got, cancel := ctxWithTimeoutIfUnset(context.Background(), 50*time.Millisecond)
	defer cancel()
	d, ok := got.Deadline()
	if !ok {
		t.Fatalf("expected derived ctx to carry a deadline")
	}
	if time.Until(d) > 50*time.Millisecond {
		t.Fatalf("derived deadline too far in the future: %v", time.Until(d))
	}
}
