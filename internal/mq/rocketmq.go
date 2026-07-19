package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// rocketProducer/rocketConsumer 用于接收 rocketmq-client-go/v2 的（未导出的）具体类型，
// 因其未导出 Producer/PushConsumer 接口，这里用结构化接口承接，保持与 mq.Producer/Consumer 解耦。
type rocketProducer interface {
	Start() error
	SendSync(ctx context.Context, m ...*primitive.Message) (*primitive.SendResult, error)
	Shutdown() error
}

type rocketConsumer interface {
	Subscribe(topic string, selector consumer.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error
	Start() error
	Shutdown() error
}

// RocketMQProducer 基于 apache/rocketmq-client-go/v2 的生产者实现（DefaultProducer）。
//
// 异步发送模型（对齐 Kafka 生产者）：Send 仅入队即返回，不阻塞调用方（结算/下单/兑换等关键路径）；
// 后台 loop 协程经 SendSync 真正发送到 broker，队列满时转入有界溢出池后台发送，溢出池也打满则
// 降级到本地 DLQ 由 replay 协程补发（broker 恢复后自动补偿，不丢消息）。SendSync 仍带超时与
// 客户端重试（覆盖瞬时失败），故「异步」指「解耦调用方与 broker 往返」，而非「不保证送达」。
type RocketMQProducer struct {
	producerBase
	cfg   *config.MQConfig
	group string
	p     rocketProducer
}

// 默认发送超时/重试：留空配置时的兜底值，避免同步发送在 broker 阻塞时无限期挂起。
const (
	defaultRocketSendTimeout = 3 * time.Second
	defaultRocketSendRetries = 2

	// 异步队列 / 溢出池默认规模（对齐 Kafka 生产者语义）。
	defaultRocketQueueSize       = 8192
	defaultRocketOverflowWorkers = 256

	// 默认批量发送条数：后台 loop 非阻塞凑批上限，分组后逐主题一次 SendSync 显著减少 broker RTT。
	defaultRocketBatchSize = 32
)

// rocketSendTimeoutOf 返回 SendSync 超时，未配置（<=0）时用默认值。
func rocketSendTimeoutOf(cfg *config.MQConfig) time.Duration {
	return orDuration(cfg.RocketMQ.SendTimeout, defaultRocketSendTimeout)
}

// rocketSendRetriesOf 返回发送阶段重试次数，未配置（<=0）时用默认值。
func rocketSendRetriesOf(cfg *config.MQConfig) int {
	if cfg.RocketMQ.SendRetries > 0 {
		return cfg.RocketMQ.SendRetries
	}
	return defaultRocketSendRetries
}

// rocketQueueSizeOf 返回异步发送队列长度，未配置（<=0）时用默认 8192。
func rocketQueueSizeOf(cfg *config.MQConfig) int {
	return maxInt(cfg.RocketMQ.QueueSize, defaultRocketQueueSize)
}

// rocketOverflowWorkersOf 返回队列满时溢出发送的最大并发 goroutine 数，未配置（<=0）时用默认 256。
func rocketOverflowWorkersOf(cfg *config.MQConfig) int {
	return maxInt(cfg.RocketMQ.MaxOverflowWorkers, defaultRocketOverflowWorkers)
}

// rocketBatchSizeOf 返回后台 loop 批量发送每批聚合上限，未配置（<=0）时用默认 32。
func rocketBatchSizeOf(cfg *config.MQConfig) int {
	return maxInt(cfg.RocketMQ.BatchSize, defaultRocketBatchSize)
}

// NewRocketMQProducer 创建 RocketMQ 生产者并 Start。endpoints 为空返回 error（factory 在无配置时返回 nil，保持 no-op）。
func NewRocketMQProducer(cfg *config.MQConfig, group string, log Logger) (*RocketMQProducer, error) {
	if len(cfg.RocketMQ.Endpoints) == 0 {
		return nil, fmt.Errorf("rocketmq: empty endpoints")
	}
	if log == nil {
		log = nopLogger{}
	}
	opts := []producer.Option{
		producer.WithNameServer(primitive.NamesrvAddr(cfg.RocketMQ.Endpoints)),
		producer.WithGroupName(group),
		// 显式发送超时：避免 broker 阻塞时 SendSync 无限期挂起（拖住结算/下单关键路径或 outbox relay）。
		producer.WithSendMsgTimeout(rocketSendTimeoutOf(cfg)),
		// 发送阶段重试：覆盖 broker 短暂不可达等瞬时失败，与消费端服务端 %RETRY% 重试互补。
		producer.WithRetry(rocketSendRetriesOf(cfg)),
	}
	if cfg.RocketMQ.AccessKey != "" {
		opts = append(opts, producer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.RocketMQ.AccessKey,
			SecretKey: cfg.RocketMQ.SecretKey,
		}))
	}
	p, err := producer.NewDefaultProducer(opts...)
	if err != nil {
		return nil, fmt.Errorf("rocketmq new producer: %w", err)
	}
	if err := p.Start(); err != nil {
		return nil, fmt.Errorf("rocketmq start producer: %w", err)
	}
	prod := &RocketMQProducer{
		cfg:   cfg,
		group: group,
		p:     p,
	}
	// 共享发送脚手架（队列 / 溢出池 / DLQ / 熔断 / 主题 / 关闭）统一由 producerBase 持有，
	// 仅协议相关的「真正发送一条」(sendOne) 与「DLQ 补发一条」(relay) 在此注入。
	prod.initBase(typeRocketMQ, rocketQueueSizeOf(cfg), rocketOverflowWorkersOf(cfg),
		newDLQStore(typeRocketMQ, cfg.RocketMQ.DLQEnabled, cfg.RocketMQ.DLQLocalPath, log),
		newProducerBreaker("rocketmq-producer", log),
		topicMapOf(cfg),
		log,
	)
	prod.publish = prod.sendOne
	prod.relayFn = prod.relay
	// 预建主题：RocketMQ 不像 Kafka/RabbitMQ 在订阅时自动建主题，PushConsumer 订阅不存在的主题会在
	// Start() 直接报 “route info not found” 而失败。生产者先于消费者创建（cmd/server），此处先把主题建好，
	// 确保随后 Subscribe 成功。优先用 admin API（若配置 broker_addr），否则发探针消息触发 broker 自动建主题。
	// 注意：此处直接调用底层 rocketProducer.SendSync（同步、启动期一次性），与后台异步 loop 互不干扰。
	prod.ensureTopics(context.Background())

	// 启动异步发送 / 重放协程：Send 入队即返回，真正发送在后台进行，不阻塞调用方关键路径。
	prod.wg.Add(1)
	go prod.loop()
	if cfg.RocketMQ.DLQEnabled && cfg.RocketMQ.DLQLocalPath != "" {
		prod.wg.Add(1)
		go prod.replayLoop()
	}
	return prod, nil
}

// Send 异步发送单条消息（入队即返回，不阻塞）。队列满时转入有界溢出池后台发送；
// 溢出池也打满则降级到本地 DLQ（由 replay 补偿）。两种方式都不阻塞业务关键路径，
// 且溢出 goroutine 受 sendSem 限流并被 wg 跟踪，避免无限增长。
// SendSync 同步发送单条消息：阻塞等待 broker 确认（SendSync）后返回 error。失败不降级 DLQ，
// 直接把投递结果返回调用方，便于关键链路（如支付成功事件）发送失败时立即决策（回滚/重试）。
// 仍受 send_timeout 超时上界与客户端重试约束，broker 不可达时返回 error 而非永久阻塞。
// 发送经熔断器保护：broker 连续不可达时熔断打开，快速失败（对齐 Kafka / RabbitMQ 生产者）。
func (r *RocketMQProducer) SendSync(ctx context.Context, msg *event.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("rocketmq marshal: %w", err)
	}
	topic := r.topicFor(msg)
	m := &primitive.Message{Topic: topic, Body: body}
	if msg.Key != "" {
		m.WithKeys([]string{msg.Key})
	}
	if msg.EventID != "" {
		m.WithTag(msg.EventID)
	}
	sendCtx, cancel := ctxWithTimeoutIfUnset(ctx, rocketSendTimeoutOf(r.cfg))
	defer cancel()
	_, berr := r.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, r.sendSyncMsg(sendCtx, m, topic, msg.EventType)
	})
	return berr
}

// loop 后台发送循环：从队列取消息，非阻塞凑成一批（按主题分组后逐主题一次 SendSync 批量发送），
// 显著减少 broker RTT 次数、压低发送尾延迟。收到关闭信号时先批量排空再退出。
func (r *RocketMQProducer) loop() {
	defer r.wg.Done()
	batchSize := rocketBatchSizeOf(r.cfg)
	for {
		select {
		case <-r.closeCh:
			// 关闭排空：尽力批量发送剩余消息；队列空后退出（不阻塞等待）。
			r.drainRemaining(context.Background(), batchSize)
			return
		case first := <-r.queue:
			// 主路径：以 first 为首非阻塞凑批（高吞吐凑满、低吞吐退化为 1），再批量发送。
			r.sendBatch(context.Background(), r.collectBatch(first, batchSize))
		}
	}
}

// sendOne 经 SendSync 真正发送单条消息（后台 loop / 溢出池调用）。
// 发送经熔断器保护（对齐 Kafka / RabbitMQ 生产者）：
//   - 熔断打开 / 半开名额耗尽（broker 大概率仍不可达、可能正在恢复）：返回该错误但不降级 DLQ，
//     交由调用方决定——主 loop requeue 等待恢复（消除恢复瞬间的「队列→DLQ」额外一跳），
//     溢出池 / 关闭排空则降级 DLQ（无「等待」语义）。
//   - 真实发送失败：降级到本地 DLQ（replay 协程在恢复后补发），且不重复计数（计数由 sendSyncMsg 完成一次）。
// collectBatch 以 first 为首，尽量非阻塞地从队列再取 batchSize-1 条，组成一批。
// 非阻塞：高吞吐时直接凑满一批（最大吞吐），低吞吐时仅 first 一条（不引入额外等待延迟）。
func (r *RocketMQProducer) collectBatch(first *event.Message, batchSize int) []*event.Message {
	batch := make([]*event.Message, 0, batchSize)
	batch = append(batch, first)
	for len(batch) < batchSize {
		select {
		case m := <-r.queue:
			batch = append(batch, m)
		default:
			return batch
		}
	}
	return batch
}

// drainRemaining 关闭阶段排空队列剩余消息（批量发送，不阻塞等待）。
func (r *RocketMQProducer) drainRemaining(ctx context.Context, batchSize int) {
	for {
		select {
		case first := <-r.queue:
			r.sendBatch(ctx, r.collectBatch(first, batchSize))
		default:
			return
		}
	}
}

// sendBatch 将一批消息按主题分组，逐主题调用一次 SendSync（批量发送）。
// RocketMQ 批量发送约束：同批消息必须同主题，故先按主题分组再分别发送。失败处理对齐既有语义：
//   - 熔断打开：整组 requeue 等待 broker 恢复（消除恢复瞬间「队列→DLQ」额外一跳）；
//   - 真实发送失败：整组降级本地 DLQ（replay 补偿），每条计一次发送失败 + 一次 DLQ。
func (r *RocketMQProducer) sendBatch(ctx context.Context, batch []*event.Message) {
	if len(batch) == 0 {
		return
	}
	// 按主题分组（RocketMQ 批量发送要求同主题）
	groups := make(map[string][]*event.Message, 4)
	for _, m := range batch {
		t := r.topicFor(m)
		groups[t] = append(groups[t], m)
	}
	for topic, msgs := range groups {
		r.sendBatchTopic(ctx, topic, msgs)
	}
}

// sendBatchTopic 将同一主题的一批消息通过一次 SendSync 批量发送（减少 broker RTT 次数）。
func (r *RocketMQProducer) sendBatchTopic(ctx context.Context, topic string, msgs []*event.Message) {
	prim := make([]*primitive.Message, 0, len(msgs))
	for _, m := range msgs {
		body, err := json.Marshal(m)
		if err != nil {
			r.log.Errorf("rocketmq marshal failed (event=%s): %v", m.EventType, err)
			// marshal 失败不可补发，直接丢弃（不计 DLQ，避免污染 replay 轮询）
			continue
		}
		pm := &primitive.Message{Topic: topic, Body: body}
		if m.Key != "" {
			pm.WithKeys([]string{m.Key})
		}
		if m.EventID != "" {
			pm.WithTag(m.EventID)
		}
		prim = append(prim, pm)
	}
	if len(prim) == 0 {
		return
	}
	sendCtx, cancel := ctxWithTimeoutIfUnset(ctx, rocketSendTimeoutOf(r.cfg))
	defer cancel()
	_, berr := r.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, r.sendSyncRaw(sendCtx, prim...)
	})
	if berr == nil {
		// 成功：每条消息计一次发送成功（保持消息级吞吐口径，与 sendOne 单条发送一致）
		for range prim {
			recordSend(typeRocketMQ, topic, nil)
		}
		return
	}
	if isBreakerOpen(berr) {
		// 熔断打开：整组 requeue 等待恢复（主 loop 语义，整组仅退避一次避免放大延迟）
		r.requeueBatch(msgs)
		return
	}
	// 真实发送失败：整组降级 DLQ（replay 补偿），每条计一次发送失败 + 一次 DLQ。
	r.log.Warnf("rocketmq batch send failed (topic=%s, n=%d): %v", topic, len(prim), berr)
	for _, m := range msgs {
		recordSend(typeRocketMQ, topic, berr)
		recordDLQ(typeRocketMQ, topic)
		r.dlq.append(topic, m, berr)
	}
}

// requeueBatch 熔断打开时把整组消息放回队列尾部等待恢复（仅整体退避一次，避免逐条退避放大延迟）。
// 队列满（内存缓冲已达上限）时单条降级 DLQ 由 replay 补偿，防止消息在内存无限堆积。
func (r *RocketMQProducer) requeueBatch(msgs []*event.Message) {
	// 短暂退避：避免 broker 持续不可达时忙等打满 CPU，也给 broker 恢复、熔断器进入
	// 半开探测留出时间；退避远小于 breaker Timeout，恢复后的感知延迟可忽略。
	sleepWithContext(context.Background(), requeueBackoff)
	for _, m := range msgs {
		select {
		case r.queue <- m:
		default:
			// 队列满：内存缓冲已用尽，按既有降级语义落 DLQ 由 replay 补偿，不阻塞主路径。
			r.fallbackToDLQ(m, errProducerOverflow)
		}
	}
}

func (r *RocketMQProducer) sendOne(ctx context.Context, msg *event.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		r.log.Errorf("rocketmq marshal failed: %v", err)
		return nil
	}
	topic := r.topicFor(msg)
	m := &primitive.Message{
		Topic: topic,
		Body:  body,
	}
	if msg.Key != "" {
		m.WithKeys([]string{msg.Key})
	}
	if msg.EventID != "" {
		m.WithTag(msg.EventID)
	}
	// 落实 send_timeout：Send 多传 context.Background()（loop / 溢出池 / replay），
	// 以配置超时派生子 ctx，避免 broker 阻塞时 SendSync 无限期挂起、拖住关键路径。
	sendCtx, cancel := ctxWithTimeoutIfUnset(ctx, rocketSendTimeoutOf(r.cfg))
	defer cancel()
	_, berr := r.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, r.sendSyncMsg(sendCtx, m, topic, msg.EventType)
	})
	if berr == nil {
		return nil // 成功：发送计数由 sendSyncMsg 记一次，此处不重复
	}
	if isBreakerOpen(berr) {
		// 熔断打开：不降级 DLQ，返回错误交由调用方 requeue（主 loop）或 DLQ（溢出池/排空）。
		return berr
	}
	// 真实发送失败：降级 DLQ 由 replay 补偿（计数 sendSyncMsg 已记一次，避免双重计数）。
	r.log.Warnf("rocketmq send failed (event=%s topic=%s): %v", msg.EventType, topic, berr)
	recordDLQ(typeRocketMQ, topic)
	r.dlq.append(topic, msg, berr)
	return nil
}

// sendSyncRaw 真正同步发送单条消息（breaker 由调用方包裹），不记指标。
// 供 sendSyncMsg（正常发送路径，需计成功/失败）与 relay（DLQ 补发，仅成功计 success、失败不计，
// 对齐 Kafka / RabbitMQ 的 relay）共用底层发送，避免 relay 复用 sendSyncMsg 导致 replay 失败被
// 误计入「发送失败」指标（broker 持续不可达时每轮 replay 全量计 failure，虚高失败率）。
func (r *RocketMQProducer) sendSyncRaw(ctx context.Context, m ...*primitive.Message) error {
	_, err := r.p.SendSync(ctx, m...)
	return err
}

// sendSyncMsg 同步发送单条消息并记一次发送计数（成功/失败）。供 sendOne / SendSync 正常发送路径复用。
// eventType 当前仅作日志上下文保留，便于未来扩展（如失败日志按事件类型聚合）。
func (r *RocketMQProducer) sendSyncMsg(ctx context.Context, m *primitive.Message, topic, eventType string) error {
	err := r.sendSyncRaw(ctx, m)
	recordSend(typeRocketMQ, topic, err)
	return err
}

// relay 将一条 DLQ 消息补发到 broker（供 replay 协程调用）。成功返回 nil；失败返回 error（保留在 DLQ 中待下次重试）。
// 计数口径对齐 Kafka / RabbitMQ 的 relay：仅补发成功计入「发送成功」（消息最终送达），
// 补发失败不计「发送失败」——它不是一次新的发送尝试，而是历史失败消息的重试，且该失败已由
// 先前的 recordDLQ 计数；避免 broker 持续不可达时每轮 replay 把 DLQ 全量计为发送失败、虚高失败率。
func (r *RocketMQProducer) relay(_ context.Context, msg *event.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	topic := r.topicFor(msg)
	m := &primitive.Message{
		Topic: topic,
		Body:  body,
	}
	if msg.Key != "" {
		m.WithKeys([]string{msg.Key})
	}
	if msg.EventID != "" {
		m.WithTag(msg.EventID)
	}
	sendCtx, cancel := context.WithTimeout(context.Background(), rocketSendTimeoutOf(r.cfg))
	defer cancel()
	_, berr := r.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, r.sendSyncRaw(sendCtx, m)
	})
	if berr == nil {
		recordSend(typeRocketMQ, topic, nil) // DLQ 补发成功计入发送成功（对齐 Kafka / RabbitMQ）
	}
	return berr
}

// Close 关闭生产者：停止后台协程并尽力排空队列，最后关闭底层 producer。
// 关闭全过程带 5s 超时兜底：broker 不可达时 SendSync 的 flush 可能长时间阻塞，超时后放弃
// 剩余发送直接返回，不拖慢进程优雅退出（未发出的消息已落 DLQ，待 broker 恢复后 replay）。
// Close 关闭生产者：停止后台协程并尽力排空队列，最后关闭底层 producer。
func (r *RocketMQProducer) Close() error {
	return r.closeProducer(func() error { return r.p.Shutdown() })
}

// rocketmqProbeBody 探针消息体：反序列化为空事件，消费者 handler 按未知 EventType 返回 nil 安全忽略。
const rocketmqProbeBody = "{}"

// ensureTopics 预建 RocketMQ 消费主题（见 free 函数 ensureRocketMQTopics）。
func (r *RocketMQProducer) ensureTopics(ctx context.Context) {
	ensureRocketMQTopics(ctx, r.cfg, r.group, r.p, r.log)
}

// ensureRocketMQTopics 预建 RocketMQ 消费主题。
// RocketMQ 不像 Kafka/RabbitMQ 在订阅时自动建主题：PushConsumer 订阅不存在的主题会在 Start() 直接报
// “route info not found” 导致整个事件总线启动失败。因此在生产者（先于消费者）启动后预建主题：
//   - 配置 mq.rocketmq.broker_addr：用 admin API 显式建主题（即使 broker 关闭 autoCreateTopicEnable 也能工作）；
//   - 未配置 broker_addr 且 p 非 nil：向每个消费主题发送一条探针消息，借助 broker 的 autoCreateTopicEnable
//     在首次发布时自动建主题（探针会被消费者安全忽略）。
//
// 任一方式失败均仅告警，不阻断启动；若主题仍未就绪，随后 Subscribe 的 Start() 会因
// "route info not found" 直接失败（main.go 记录 "event consumer init failed" 并禁用事件驱动进度），
// 不会无限重试。故生产环境建议配置 mq.rocketmq.broker_addr 走 admin API 显式建主题，
// 或在 broker 侧开启 autoCreateTopicEnable，确保消费者 Start 前主题已存在。
func ensureRocketMQTopics(ctx context.Context, cfg *config.MQConfig, group string, p rocketProducer, log Logger) {
	topics := ConsumerTopics(cfg)
	if addr := cfg.RocketMQ.BrokerAddr; addr != "" {
		ensureRocketMQTopicsViaAdmin(ctx, cfg, addr, topics, log)
		return
	}
	if p != nil {
		// 无 broker_addr：依赖 broker 的 autoCreateTopicEnable，发送探针消息触发自动建主题。
		for _, t := range topics {
			m := &primitive.Message{Topic: t, Body: []byte(rocketmqProbeBody)}
			if _, err := p.SendSync(ctx, m); err != nil {
				log.Warnf("rocketmq probe-send to topic %q failed (topic may not auto-create): %v", t, err)
			} else {
				log.Infof("rocketmq topic ensured via probe: %s", t)
			}
		}
		return
	}
	log.Warnf("rocketmq: broker_addr not set and no producer available; cannot pre-create topics, consumer may fail to subscribe")
}

// ensureRocketMQTopicsViaAdmin 用 admin API 在指定 broker 上显式建主题。
func ensureRocketMQTopicsViaAdmin(ctx context.Context, cfg *config.MQConfig, brokerAddr string, topics []string, log Logger) {
	opts := []admin.AdminOption{admin.WithResolver(primitive.NewPassthroughResolver(cfg.RocketMQ.Endpoints))}
	if cfg.RocketMQ.AccessKey != "" {
		opts = append(opts, admin.WithCredentials(primitive.Credentials{
			AccessKey: cfg.RocketMQ.AccessKey,
			SecretKey: cfg.RocketMQ.SecretKey,
		}))
	}
	adm, err := admin.NewAdmin(opts...)
	if err != nil {
		log.Warnf("rocketmq admin init failed, skip explicit topic creation: %v", err)
		return
	}
	defer adm.Close()
	for _, t := range topics {
		if err := adm.CreateTopic(ctx,
			admin.WithTopicCreate(t),
			admin.WithBrokerAddrCreate(brokerAddr),
			admin.WithReadQueueNums(4),
			admin.WithWriteQueueNums(4),
		); err != nil {
			// 主题已存在等情况忽略；其它情况告警，不阻断启动
			log.Warnf("rocketmq create topic %q: %v", t, err)
		} else {
			log.Infof("rocketmq topic ensured: %s", t)
		}
	}
}

// RocketMQConsumer 基于 apache/rocketmq-client-go/v2 的 PushConsumer 实现。
// 处理失败返回 ConsumeRetryLater 触发 RocketMQ 服务端重试；成功返回 ConsumeSuccess。
type RocketMQConsumer struct {
	cfg           *config.MQConfig
	group         string
	c             rocketConsumer
	topics        map[string]string
	log           Logger
	progressStore ConsumerProgressStore // 可选：注入后启用 event_id 跨实例幂等去重（消费侧 Outbox/Dedup）
}

// NewRocketMQConsumer 创建 RocketMQ 消费者（PushConsumer）。
func NewRocketMQConsumer(cfg *config.MQConfig, group string, log Logger) (*RocketMQConsumer, error) {
	if len(cfg.RocketMQ.Endpoints) == 0 {
		return nil, fmt.Errorf("rocketmq: empty endpoints")
	}
	if log == nil {
		log = nopLogger{}
	}
	c, err := consumer.NewPushConsumer(
		consumer.WithNameServer(primitive.NamesrvAddr(cfg.RocketMQ.Endpoints)),
		consumer.WithGroupName(group),
	)
	if err != nil {
		return nil, fmt.Errorf("rocketmq new consumer: %w", err)
	}
	return &RocketMQConsumer{cfg: cfg, group: group, c: c, topics: topicMapOf(cfg), log: log}, nil
}

// WithProgressStore 注入消费进度/去重存储，启用 event_id 跨实例幂等去重（消费侧 Outbox/Dedup）。
// RocketMQ 由 broker 管理消费位点（offset），故本 store 仅用于 Seen 去重与去重标记，不接管 offset；
// 注入 SQLProgressStore（与业务库同实例）可获得跨节点 event_id 去重，与 Kafka 消费者 #3 能力对齐。
func (r *RocketMQConsumer) WithProgressStore(s ConsumerProgressStore) *RocketMQConsumer {
	r.progressStore = s
	return r
}

// Subscribe 订阅 topics 并启动消费；阻塞直到 ctx 取消后 Shutdown。
func (r *RocketMQConsumer) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	for _, topic := range topics {
		t := topic
		if err := r.c.Subscribe(t, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			return r.handleMessages(ctx, t, msgs, handler)
		}); err != nil {
			// topic 订阅失败（broker 不可达 / 路由不存在）：拉取失败计数，与 kafka reader fetch 错误同口径。
			recordConsumeFetchError(typeRocketMQ, t)
			return fmt.Errorf("rocketmq subscribe %s: %w", t, err)
		}
	}
	if err := r.c.Start(); err != nil {
		// Start 失败 = 无法连 namesrv（broker 不可用）：逐 topic 上报拉取失败，供统一告警。
		for _, t := range topics {
			recordConsumeFetchError(typeRocketMQ, t)
		}
		return fmt.Errorf("rocketmq start consumer: %w", err)
	}
	<-ctx.Done()
	return r.c.Shutdown()
}

// handleMessages 处理单批 RocketMQ 消息（从 Subscribe 回调中抽出，便于单测与复用）。
//   - 毒消息（unmarshal 失败）：返回 ConsumeRetryLater，交由 broker 重试并最终进入 %DLQ%，
//     而非 continue 后整批 ConsumeSuccess 丢弃（避免消息静默丢失，修复旧实现 bug）。
//   - 消费侧幂等去重（消费侧 Outbox/Dedup，与 Kafka / RabbitMQ 对齐）：跨节点/重投的同 eventID 直接 continue 跳过。
//   - 本地重试（对齐 Kafka / RabbitMQ 消费侧）：handler 失败先就地指数退避重试，让瞬时故障/抖动在本地自愈，
//     避免一上来就交给 broker %RETRY%（省一轮 broker 往返、降低 broker 重试压力）；本地重试耗尽再返回
//     ConsumeRetryLater，由 broker 服务端按 %RETRY% 延时重投、超次进 %DLQ%（RocketMQ 惯用兜底）。
//   - 处理成功：标记去重（持久化 eventID），便于后续重投（broker 重试/重启）跳过，避免重复业务处理。
func (r *RocketMQConsumer) handleMessages(ctx context.Context, t string, msgs []*primitive.MessageExt, handler MessageHandler) (consumer.ConsumeResult, error) {
	for _, me := range msgs {
		var msg event.Message
		if err := json.Unmarshal(me.Body, &msg); err != nil {
			r.log.Errorf("rocketmq unmarshal failed (topic=%s): %v", t, err)
			// 反序列化失败 → 交由 broker 重试直至 %DLQ%（非本地 DLQ），不在此计 dlq（避免 broker
			// 多次重投重复计数）；仅成功 / 去重跳过计入，保持计数器语义一致（见 13 §3.62）。
			return consumer.ConsumeRetryLater, err
		}
		if r.progressStore != nil && msg.EventID != "" {
			if seen, sErr := r.progressStore.Seen(ctx, msg.EventID); sErr == nil && seen {
				r.log.Infof("rocketmq duplicate event %s skipped", msg.EventID)
				recordConsumed(typeRocketMQ, t, resultDedupSkipped) // 重复 event_id 跳过，计入去重口径
				continue
			}
		}
		// 本地重试（对齐 Kafka / RabbitMQ 消费侧）：先就地指数退避重试，仅瞬时/抖动可通过本地重试自愈，
		// 避免一上来就交给 broker %RETRY%（省一轮 broker 往返、降低 broker 重试压力）；
		// 本地重试耗尽再返回 ConsumeRetryLater，由 broker 服务端按 %RETRY%/%DLQ% 兜底（RocketMQ 惯用）。
		rmax, rbase, rmaxDelay := consumerRetryOf(r.cfg)
		if err := retryProcess(ctx, &msg, handler, rmax, rbase, rmaxDelay); err != nil {
			r.log.Warnf("rocketmq handler failed (topic=%s), retry later by broker: %v", t, err)
			// 失败 → broker 重试（非本地 DLQ），不在此计 dlq（避免 broker 多次重投重复计数）。
			return consumer.ConsumeRetryLater, err
		}
		recordConsumed(typeRocketMQ, t, resultProcessed)
		if r.progressStore != nil && msg.EventID != "" {
			if err := r.progressStore.Commit(ctx, t, 0, 0, msg.EventID, false); err != nil {
				r.log.Warnf("rocketmq progress commit failed (event=%s): %v", msg.EventID, err)
			}
		}
	}
	return consumer.ConsumeSuccess, nil
}

// Close 关闭消费者。
func (r *RocketMQConsumer) Close() error {
	return r.c.Shutdown()
}
