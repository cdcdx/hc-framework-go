package mq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/cdcdx/hc-framework-go/internal/event"
)

// producerBase 封装 Kafka / RabbitMQ / RocketMQ 三个真实生产者的「跨切面」发送逻辑：
// 异步队列 + 溢出池 + 熔断 + 本地 DLQ 兜底 + replay 重放 + 指标上报 + 生命周期关闭。
//
// 三者此前的实现中这些逻辑几乎逐字相同（kafka.go / rabbitmq.go / rocketmq.go 各约 200 行重复），
// 仅协议相关的「真正发送一条」(publish) 与「DLQ 补发一条」(relayFn) 不同。抽到此处统一维护，
// 协议适配器只需实现 publish / relayFn 并在构造时注入，避免三份近重复的同步演进与潜在不一致。
//
// 设计约定：producerBase 以匿名嵌入方式进入各具体生产者，故其字段（queue / dlq / breaker / topics
// 等）与方法（Send / SendBatch / requeue / fallbackToDLQ / topicFor / replayLoop / Close 经
// closeProducer）对具体生产者可见（方法提升），具体生产者继续持有自身协议字段（writer / conn / p
// 等）与协议方法（sendOne / relay / loop）。
type producerBase struct {
	queue     chan *event.Message
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	sendSem   chan struct{} // 溢出池并发上限（限流，避免队列满时无限起 goroutine）

	dlq          *dlqStore                           // 本地文件 DLQ（broker 不可达时降级落盘，replay 补偿）
	breaker      *gobreaker.CircuitBreaker[struct{}] // 熔断保护：broker 连续不可达时快速失败，避免雪崩
	topics       map[string]string                   // eventType -> topic（统一来源于 eventTopicsMap / topicMapOf）
	log          Logger
	typeName     string // 指标标签：kafka|rabbitmq|rocketmq
	maxCloseWait time.Duration

	// 由具体生产者注入的协议相关钩子：
	publish func(ctx context.Context, msg *event.Message) error // = 协议 sendOne（熔断保护已在其中）
	relayFn relayFunc                                           // = 协议 relay（供 replayLoop 调用）
}

// initBase 初始化共享字段（队列 / 溢出池 / DLQ / 熔断 / 主题 / 类型 / 关闭超时）。
// queueSize / overflowWorkers 的「0 是否禁用」等口径由各构造器按其历史语义预先算好再传入，
// 此处不做二次默认值覆盖，避免改变既有行为。
func (b *producerBase) initBase(typeName string, queueSize, overflowWorkers int, dlq *dlqStore, breaker *gobreaker.CircuitBreaker[struct{}], topics map[string]string, log Logger) {
	if log == nil {
		log = nopLogger{}
	}
	b.typeName = typeName
	b.queue = make(chan *event.Message, queueSize)
	b.closeCh = make(chan struct{})
	b.sendSem = make(chan struct{}, overflowWorkers)
	b.dlq = dlq
	b.breaker = breaker
	b.topics = topics
	b.log = log
	b.maxCloseWait = 5 * time.Second
}

// Send 异步发送单条消息（入队即返回，不阻塞）。队列满时转入有界溢出池后台发送；
// 溢出池也打满则降级到本地 DLQ（由 replay 补偿）。两种方式都不阻塞业务关键路径，
// 且溢出 goroutine 受 sendSem 限流并被 wg 跟踪，避免无限增长（C3）。
// 三队列生产者（Kafka / RabbitMQ / RocketMQ）此逻辑完全一致。
func (b *producerBase) Send(ctx context.Context, msg *event.Message) error {
	select {
	case b.queue <- msg:
		return nil
	default:
	}
	// 队列满：仅当获取到一个溢出槽位时才起 goroutine 补发，把并发量控制在有限范围。
	// 若溢出池也已饱和（说明 Broker 持续不可达），直接降级到 DLQ，与发送失败等价，
	// 既不阻塞调用方、也不丢失消息。
	select {
	case b.sendSem <- struct{}{}:
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer func() { <-b.sendSem }()
			// 溢出池无「等待恢复」语义：仅熔断打开（broker 不可达）才降级 DLQ；
			// 其余（如 marshal 这类不可补发的极端错误）直接丢弃，避免落不可补发的
			// DLQ 占用 replay 轮询。真实发送失败已在 publish 内降级 DLQ 并返回 nil，此处不会收到。
			if err := b.publish(context.Background(), msg); err != nil {
				if isBreakerOpen(err) {
					b.fallbackToDLQ(msg, err)
				}
			}
		}()
		return nil
	default:
		b.log.Warnf("%s producer overflow saturated, fallback to DLQ (event=%s)", b.typeName, msg.EventType)
		recordSend(b.typeName, b.topicFor(msg), errProducerOverflow)
		recordDLQ(b.typeName, b.topicFor(msg))
		b.dlq.append(b.topicFor(msg), msg, errProducerOverflow)
		return nil
	}
}

// SendBatch 批量发送（逐条入队）。
func (b *producerBase) SendBatch(ctx context.Context, msgs []*event.Message) error {
	for _, m := range msgs {
		if err := b.Send(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// processMessage 后台发送循环主路径的单条处理：发送失败且为「熔断打开」时 requeue 等待恢复，
// 避免降级 DLQ 的额外一跳；真实失败已在 publish 内降级 DLQ、marshal 错误不可重试直接丢弃，
// 二者均不再 requeue（避免无效重试 / 无限 requeue 死循环）。三队列语义一致。
func (b *producerBase) processMessage(ctx context.Context, msg *event.Message) {
	if err := b.publish(ctx, msg); err != nil {
		if isBreakerOpen(err) {
			b.requeue(msg)
		}
	}
}

// drainMessage 关闭排空阶段的单条处理：仅「熔断打开」仍发不出去的消息降级 DLQ
// （requeue 已无意义）。三队列语义一致。
func (b *producerBase) drainMessage(ctx context.Context, msg *event.Message) {
	if err := b.publish(ctx, msg); err != nil {
		if isBreakerOpen(err) {
			b.fallbackToDLQ(msg, err)
		}
	}
}

// requeue 在熔断打开时把消息放回队列尾部等待恢复，避免不必要地降级 DLQ。
// 队列满（内存缓冲已达上限）时改为降级 DLQ 由 replay 补偿，防止消息在内存无限堆积。
func (b *producerBase) requeue(msg *event.Message) {
	// 熔断已打开（broker 持续不可达）：重新入队只会被立刻再次 publish 失败，制造「入队→Send
	// 失败→再入队」的无效风暴，最终打满队列/溢出池（见 2026-07-20 日志：queue full and
	// overflow workers saturated）。broker 恢复后 replayLoop 会从 DLQ 重放，故此处直接降级
	// DLQ，既消除风暴又不丢消息；仅在熔断器半开探测（StateHalfOpen）时才给一次重试机会。
	if b.breaker != nil && b.breaker.State() == gobreaker.StateOpen {
		b.fallbackToDLQ(msg, errBreakerOpen)
		return
	}
	// 短暂退避：避免 broker 持续不可达时忙等打满 CPU，也给 broker 恢复、熔断器进入
	// 半开探测留出时间；退避远小于 breaker Timeout，恢复后的感知延迟可忽略。
	sleepWithContext(context.Background(), requeueBackoff)
	select {
	case b.queue <- msg:
	default:
		// 队列满：内存缓冲已用尽，按既有降级语义落 DLQ 由 replay 补偿，不阻塞主路径。
		b.fallbackToDLQ(msg, errProducerOverflow)
	}
}

// fallbackToDLQ 把消息降级到本地 DLQ（统一出口：记录日志、指标与落盘）。
func (b *producerBase) fallbackToDLQ(msg *event.Message, sendErr error) {
	b.log.Warnf("%s send failed (event=%s): %v", b.typeName, msg.EventType, sendErr)
	recordDLQ(b.typeName, b.topicFor(msg))
	b.dlq.append(b.topicFor(msg), msg, sendErr)
}

// topicFor 事件类型 -> 主题。未知类型回落到 "events"。
func (b *producerBase) topicFor(msg *event.Message) string {
	return resolveTopic(b.topics, msg.EventType)
}

// replayLoop 后台重放协程：定期尝试将 DLQ 中消息补发到 Broker（恢复后自动清空）。
func (b *producerBase) replayLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.closeCh:
			return
		case <-ticker.C:
			b.dlq.replayAll(b.relayFn)
		}
	}
}

// closeProducer 关闭生产者：停止后台协程并尽力排空队列，最后调用 closeUnderlying 关闭底层资源。
// 关闭全过程带 maxCloseWait 超时兜底：Broker 不可达时底层关闭 / 队列排空可能长时间阻塞，
// 超时后放弃剩余 flush 直接返回，避免拖慢进程优雅退出（未发出的消息已落 DLQ）。
// 各具体生产者（Kafka / RabbitMQ / RocketMQ）此前此逻辑一致，仅 closeUnderlying 不同。
func (b *producerBase) closeProducer(closeUnderlying func() error) error {
	done := make(chan error, 1)
	b.closeOnce.Do(func() {
		close(b.closeCh)
		go func() {
			b.wg.Wait() // 等 worker / loop / replayLoop 排空队列并退出
			done <- closeUnderlying()
		}()
	})
	select {
	case err := <-done:
		return err
	case <-time.After(b.maxCloseWait):
		return fmt.Errorf("%s producer close timed out after %s (broker unreachable?)", b.typeName, b.maxCloseWait)
	}
}
