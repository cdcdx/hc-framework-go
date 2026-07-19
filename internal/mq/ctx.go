package mq

import (
	"context"
	"time"
)

// ctxWithTimeoutIfUnset 若 ctx 已带截止时间则原样返回；否则以 d 派生带超时的子 ctx（调用方负责 defer cancel）。
//
// 用途：发送路径上若调用方传 context.Background()（如 outbox relay、部分同步发送），
// 仍能保证「发送/确认」有上界，避免 broker 阻塞时 Send 无限期挂起、拖住关键路径，
// 或（RabbitMQ）持证（mu）永久阻塞、冻结整个生产者。
func ctxWithTimeoutIfUnset(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// sleepWithContext 在 d 到期后返回，或 ctx 取消时立即返回。每次调用创建独立定时器并 defer Stop，
// 既不会泄漏（区别于 time.After：其定时器需等触发才被 GC），也避免复用单个 time.Timer 时
// 因「首次回调耗时超过 d 导致定时器提前触发、残留值被后续 Reset+select 立即读到」而跳过退避
// （消费重试 / 拉取退避的真实隐患，见 retryProcess 与 kafka consumer 的 process）。
func sleepWithContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
