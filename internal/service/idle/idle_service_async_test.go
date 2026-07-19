package idle

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// ctxCapturingProducer 捕获调用 Send 时传入的 ctx，用于守护 emitIdleSettledAsync 不会在派生的
// 5s 超时 ctx 上「提前取消」（见 §3.64）。其 Send 同步把 ctx.Err() 写入 done 通道。
type ctxCapturingProducer struct {
	done chan error
}

func (p *ctxCapturingProducer) Send(ctx context.Context, msg *event.Message) error {
	select {
	case p.done <- ctx.Err():
	default:
	}
	return nil
}
func (p *ctxCapturingProducer) SendSync(context.Context, *event.Message) error    { return nil }
func (p *ctxCapturingProducer) SendBatch(context.Context, []*event.Message) error { return nil }
func (p *ctxCapturingProducer) Close() error                                      { return nil }

// TestEmitIdleSettledAsync_ContextNotPrematurelyCancelled 守护：emitIdleSettledAsync 派生的 5s
// 超时 ctx 不得在 goroutine 启动前被 cancel。旧实现把 defer cancel() 放在外部函数，go 语句启动
// goroutine 后函数立即返回、cancel() 随即执行，导致 ctx 在 emitIdleSettled 运行前即被取消——
// RabbitMQ 的确认等待会立即命中 Done() 而丢 idle.settled 事件（设计意图「用带超时 ctx 避免被请求
// ctx 取消」彻底落空）。修复后 cancel 移入 goroutine，Send 调用时 ctx 仍有效。
func TestEmitIdleSettledAsync_ContextNotPrematurelyCancelled(t *testing.T) {
	prod := &ctxCapturingProducer{done: make(chan error, 1)}
	svc := &IdleService{publisher: prod}
	rec := &model.IdleRecord{ID: 1, UserID: "u1", DurationSeconds: 10, PointsEarned: 5}

	svc.emitIdleSettledAsync(rec)

	select {
	case err := <-prod.done:
		if err != nil {
			t.Fatalf("Send called with cancelled ctx (err=%v): emitIdleSettledAsync prematurely cancelled the 5s timeout ctx", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publisher.Send was not called within 2s (async emit did not run)")
	}
}
