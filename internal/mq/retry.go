package mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/event"
)

// errHandlerPanic 标记 handler 内部发生的 panic 已被 recover 转换为 error，
// 调用方可据此跳过重试（panic 通常是确定性 bug，重试无意义、甚至可能重复执行带副作用的逻辑）。
var errHandlerPanic = errors.New("mq handler panic")

// callHandlerSafe 调用 handler 并 recover panic，将 panic 转换为 error 返回，
// 避免单个消息的 handler 异常直接拖垮整个消费 goroutine / 进程（见 13 §3.39）。
func callHandlerSafe(ctx context.Context, msg *event.Message, handler MessageHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errHandlerPanic, r)
		}
	}()
	return handler(ctx, msg)
}

// retryProcess 通用消费重试：失败按指数退避重试 retryMax 次（retryMax>=0 表示至少尝试 1 次）。
// ctx 取消时立即返回 ctx.Err()；全部失败后返回最后一次错误（由调用方决定 DLQ/丢弃）。
// Kafka 消费者有自己更完整的重试+DLQ 实现，此处供 RabbitMQ/RocketMQ/内存适配器复用同一语义。
//
// 健壮性：handler 内部 panic 由 callHandlerSafe 转为 error，并视为不可重试的失败直接返回，
// 防止任何一条消息的 handler 异常导致进程整体崩溃。
func retryProcess(ctx context.Context, msg *event.Message, handler MessageHandler, retryMax int, base, max time.Duration) error {
	delay := base

	var lastErr error
	for attempt := 0; attempt <= maxInt(retryMax, 0); attempt++ {
		if attempt > 0 {
			sleepWithContext(ctx, delay)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			delay *= 2
			if delay > max {
				delay = max
			}
		}
		err := callHandlerSafe(ctx, msg, handler)
		if err != nil {
			// handler panic 已转 error 且不可重试，直接当失败返回，
			// 交由上层（DLQ / Nack / 日志）处理，避免重复执行带副作用的 handler。
			if errors.Is(err, errHandlerPanic) {
				return err
			}
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}
