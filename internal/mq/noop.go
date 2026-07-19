package mq

import (
	"context"

	"github.com/cdcdx/hc-framework-go/internal/event"
)

// nopProducer 是事件生产者的空实现（no-op）。当 mq.type=none / 未配置 / 连接项缺失时，
// 工厂返回它而非 nil。这样「不启用事件总线」的语义在接口层面被强制为安全空操作，
// 调用方无需各自判空——否则一旦遗漏 nil 判空，对接口方法调用会直接 panic。
//
// 下游流程在「无总线」下仍按正确分支运行：
//   - idle 结算 / 用户事件：仅落库，不发布事件；
//   - 缓存写降级：main.go 不会注入该 sender，退化为本地异步写 L2；
//   - 积分余额：由进程内 Outbox 同步快路径 + 后台 relay 保证最终一致，事件仅作冗余触发。
type nopProducer struct{}

// Send 是 no-op：总线未启用（mq.type=none / 未配置 / 连接缺失）。事件仅落库 / 走 Outbox relay，
// 不会真正发出，故计入「真正丢弃」指标，便于量化「本应发出但未发出」的事件量（events_disabled）。
func (nopProducer) Send(_ context.Context, _ *event.Message) error {
	recordDropped(typeNone, "events")
	return nil
}
func (nopProducer) SendBatch(_ context.Context, _ []*event.Message) error { return nil }

// SendSync 是 no-op：总线未启用，事件仅落库 / 走 Outbox relay，计入「真正丢弃」指标。
func (nopProducer) SendSync(_ context.Context, _ *event.Message) error {
	recordDropped(typeNone, "events")
	return nil
}
func (nopProducer) Close() error { return nil }

// nopConsumer 是事件消费者的空实现（no-op）。当 mq.type=none / 未配置 / 连接项缺失时，
// 工厂返回它而非 nil。Subscribe 仅阻塞直到 ctx 取消后返回，不订阅任何主题、不投递任何消息；
// Close 为空操作。同样保证「不启用事件总线」时不会因调用方忘记判空而 panic。
type nopConsumer struct{}

func (nopConsumer) Subscribe(ctx context.Context, _ []string, _ MessageHandler) error {
	<-ctx.Done()
	return nil
}
func (nopConsumer) Close() error { return nil }

// NewNopProducer 返回一个 no-op 生产者（所有发送操作为空操作，Close 安全）。
// 用于 NewProducer 失败时作为安全回退，避免传入 nil 接口导致调用方 panic。
func NewNopProducer() Producer { return nopProducer{} }

// NewNopConsumer 返回一个 no-op 消费者（Subscribe 阻塞至 ctx 取消，Close 安全）。
func NewNopConsumer() Consumer { return nopConsumer{} }
