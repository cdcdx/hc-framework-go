package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// rabbitExchange 所有事件发布的主题交换机（topic 类型，支持按 routing key 绑定）。
const rabbitExchange = "hc-events"

// errRabbitMarshal 标记「事件序列化失败」：此类错误无法补发，不落 DLQ（避免无效追加），
// 仅在 sendOne 中记录一次发送失败计数。
var errRabbitMarshal = errors.New("rabbitmq marshal")

// rabbitChannel 池中的一个 channel：各自持锁串行发布 + 独立的 publisher confirm 流。
// 单连接多 channel 可并发发布（amqp091-go 的 Connection 多路复用），从而提升吞吐；
// 每个 channel 串行发布保证其确认顺序与发布顺序一致（读取一个 Confirmation 即对应本条消息）。
type rabbitChannel struct {
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
	mu       sync.Mutex
}

// RabbitMQProducer 基于 rabbitmq/amqp091-go 的生产者实现（异步模型，对齐 Kafka / RocketMQ）。
//
// 异步发送模型：Send 仅入队即返回，不阻塞调用方（结算/下单/兑换等关键路径）；
// 后台 poolSize 个 worker 协程经 channel 池真正发布并等待 broker 确认（publisher confirm），
// 队列满时转入有界溢出池后台发送，溢出池也打满则降级到本地 DLQ 由 replay 协程补发
// （broker 恢复后自动补偿，不丢消息）。worker 仍带超时与 broker 确认等待，故「异步」指
// 「解耦调用方与 broker 往返 + 多 channel 并发确认」，而非「不保证送达」。
type RabbitMQProducer struct {
	producerBase
	cfg      *config.MQConfig
	conn     *amqp.Connection
	pool     chan *rabbitChannel // 空闲 channel 缓冲池，容量 = poolSize；worker 并发发布通道
	poolSize int
}

// NewRabbitMQProducer 创建 RabbitMQ 生产者（建立连接、预建 poolSize 个 channel 并各自开启 confirm，
// 启动后台 worker / replay 协程）。url 为空返回 error（由 factory 在 type=rabbitmq 且无 url 时返回 nil，保持 no-op 语义）。
func NewRabbitMQProducer(cfg *config.MQConfig, group string, log Logger) (*RabbitMQProducer, error) {
	if cfg.RabbitMQ.URL == "" {
		return nil, fmt.Errorf("rabbitmq: empty url")
	}
	if log == nil {
		log = nopLogger{}
	}
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	poolSize := rabbitPoolSizeOf(cfg)
	pool := make(chan *rabbitChannel, poolSize)
	for i := 0; i < poolSize; i++ {
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("rabbitmq channel %d: %w", i, err)
		}
		// 声明 topic 交换机（broker 级、幂等，每 channel 各自声明一次即可保证该 channel 可用）。
		if err := ch.ExchangeDeclare(rabbitExchange, "topic", true, false, false, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return nil, fmt.Errorf("rabbitmq exchange declare: %w", err)
		}
		// 开启发布确认（publisher confirm）：broker 落盘后异步回执，Send 会等待该回执，
		// 保证消息真正被 broker 接收（而非仅发出到网络层即返回）。须在 NotifyPublish 前调用。
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			conn.Close()
			return nil, fmt.Errorf("rabbitmq confirm mode: %w", err)
		}
		confirms := make(chan amqp.Confirmation, 1)
		ch.NotifyPublish(confirms)
		pool <- &rabbitChannel{ch: ch, confirms: confirms}
	}
	prod := &RabbitMQProducer{
		cfg:      cfg,
		conn:     conn,
		pool:     pool,
		poolSize: poolSize,
	}
	// 共享发送脚手架（队列 / 溢出池 / DLQ / 熔断 / 主题 / 关闭）统一由 producerBase 持有，
	// 仅协议相关的「真正发送一条」(sendOne) 与「DLQ 补发一条」(relay) 在此注入。
	prod.initBase(typeRabbitMQ, rabbitQueueSizeOf(cfg), rabbitOverflowWorkersOf(cfg),
		newDLQStore(typeRabbitMQ, cfg.RabbitMQ.DLQEnabled, cfg.RabbitMQ.DLQLocalPath, log),
		newProducerBreaker("rabbitmq-producer", log),
		topicMapOf(cfg),
		log,
	)
	prod.publish = prod.sendOne
	prod.relayFn = prod.relay
	// 启动 poolSize 个后台 worker：并发消费队列并经 channel 池发布 + 确认，把「等待 broker 确认」
	// 从调用方业务线程移走（对齐 Kafka / RocketMQ 异步生产者；原每 channel 串行确认语义保留，
	// 并发度上限仍 = poolSize）。关闭信号到达时 worker 先排空队列再退出。
	for i := 0; i < poolSize; i++ {
		prod.wg.Add(1)
		go prod.worker()
	}
	if cfg.RabbitMQ.DLQEnabled && cfg.RabbitMQ.DLQLocalPath != "" {
		prod.wg.Add(1)
		go prod.replayLoop()
	}
	return prod, nil
}

// acquire 从池取一个空闲 channel；池空（全部在途）时阻塞，受 ctx 取消约束以支援优雅关闭。
func (p *RabbitMQProducer) acquire(ctx context.Context) (*rabbitChannel, error) {
	select {
	case rc := <-p.pool:
		return rc, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("rabbitmq acquire channel: %w", ctx.Err())
	}
}

// release 将 channel 归还池中复用；nil 时安全跳过。
func (p *RabbitMQProducer) release(rc *rabbitChannel) {
	if rc == nil {
		return
	}
	p.pool <- rc
}

// recreate 在 channel 失效（发布错误 / 确认未返回 Ack）时重建同一池槽的 channel，避免带病复用污染连接池。
// 失败（连接级故障）则保留原（已失效）channel，使该槽后续发布仍失败直至连接恢复——与单 channel 版本一致，
// 不引入自动重连（重连属于更大范围改造）。调用方须持 rc.mu，确保重建期间无并发发布。
func (p *RabbitMQProducer) recreate(rc *rabbitChannel) {
	if rc.ch != nil {
		_ = rc.ch.Close()
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return
	}
	if err := ch.ExchangeDeclare(rabbitExchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return
	}
	confirms := make(chan amqp.Confirmation, 1)
	ch.NotifyPublish(confirms)
	rc.ch = ch
	rc.confirms = confirms
}

// Send 异步发布单条消息（routing key = 事件主题）。入队即返回，不阻塞调用方；
// 队列满时转入有界溢出池后台发送，溢出池也打满则降级到本地 DLQ（由 replay 补偿）。
// 两种方式都不阻塞业务关键路径；溢出 goroutine 受 sendSem 限流并被 wg 跟踪，避免无限增长。
// SendSync 同步发布单条消息：阻塞等待 broker 确认（publisher confirm）后返回 error。
// 失败不降级 DLQ，直接把投递结果返回调用方，便于关键链路（如支付成功事件）发送失败时立即决策（回滚/重试）。
// 仍受 send_timeout 确认等待超时上界约束，broker 不可达时返回 error 而非永久阻塞。
func (p *RabbitMQProducer) SendSync(ctx context.Context, msg *event.Message) error {
	topic := p.topicFor(msg)
	if err := p.publishOne(ctx, msg); err != nil {
		// marshal 失败无法补发，直接返回；其余发布/确认失败同样返回，由调用方决策。
		if !errors.Is(err, errRabbitMarshal) {
			recordSend(typeRabbitMQ, topic, err)
		}
		return err
	}
	recordSend(typeRabbitMQ, topic, nil)
	return nil
}

// worker 后台发送协程（共 poolSize 个）：并发消费队列，经 channel 池发布并等待 broker 确认。
// 关闭信号到达时先排空队列再退出，保证优雅关闭不丢队列内消息（仍在途的 confirm 由 Close 的超时兜底）。
func (p *RabbitMQProducer) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.closeCh:
			// 关闭排空：尽力发送；仅「熔断打开」仍发不出去的消息降级 DLQ（requeue 已无意义）。
			for {
				select {
				case msg := <-p.queue:
					p.drainMessage(context.Background(), msg)
				default:
					return
				}
			}
		case msg := <-p.queue:
			// 主路径：熔断打开则 requeue 等待 broker 恢复（避免降级 DLQ 的额外一跳）；
			// 真实失败已在 publish 内降级 DLQ，无需再处理。
			p.processMessage(context.Background(), msg)
		}
	}
}

// sendOne 在后台 worker / 溢出池中将单条消息发布到 broker。
// 发布经熔断器保护（对齐 Kafka / RocketMQ 生产者）：
//   - 熔断打开 / 半开名额耗尽（broker 大概率仍不可达、可能正在恢复）：返回该错误但不降级 DLQ，
//     交由调用方决定——主 worker requeue 等待恢复（消除恢复瞬间的「队列→DLQ」额外一跳），
//     溢出池 / 关闭排空则降级 DLQ（无「等待」语义）。
//   - 真实发布/确认失败（非 marshal）：降级到本地 DLQ（replay 协程在恢复后补发）。
//   - marshal 失败：无法补发，仅记录不落 DLQ。
func (p *RabbitMQProducer) sendOne(ctx context.Context, msg *event.Message) error {
	topic := p.topicFor(msg)
	if err := p.publishOne(ctx, msg); err != nil {
		// marshal 失败属于生产者侧序列化错误（非 broker 发送尝试），不计入「发送成功/失败」指标，
		// 与 Kafka / RocketMQ 异步 sendOne（marshal 返回早于 recordSend）及三队列 SendSync（marshal 直接返回不计数）口径一致。
		if errors.Is(err, errRabbitMarshal) {
			p.log.Errorf("rabbitmq marshal failed (event=%s): %v", msg.EventType, err)
			return nil
		}
		// 熔断打开：不降级 DLQ，返回错误交由调用方 requeue（主 worker）或 DLQ（溢出池/排空）。
		if isBreakerOpen(err) {
			return err
		}
		p.log.Warnf("rabbitmq send failed (event=%s topic=%s): %v", msg.EventType, topic, err)
		recordSend(typeRabbitMQ, topic, err)
		recordDLQ(typeRabbitMQ, topic)
		p.dlq.append(topic, msg, err)
		return nil
	}
	recordSend(typeRabbitMQ, topic, nil)
	return nil
}

// publishOne 在后台 worker / 溢出池中将单条消息发布到 broker。失败（非 marshal 错误）降级到本地 DLQ（replay 补偿）。
// 发布经熔断器保护：broker 连续不可达时熔断打开，快速失败（不走 10s confirm 超时），失败消息降级到 DLQ 由
// replay 协程在恢复后补发（对齐 Kafka / RocketMQ 生产者雪崩保护）。SendSync 亦经此路径，自动受熔断保护。
func (p *RabbitMQProducer) publishOne(ctx context.Context, msg *event.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("%w: %v", errRabbitMarshal, err)
	}
	topic := resolveTopic(p.topics, msg.EventType)
	_, err = p.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, p.publishConfirmed(ctx, msg, body, topic)
	})
	return err
}

// publishConfirmed 经 channel 池真正发布单条消息并等待 broker 确认（publisher confirm）。
// 失败（发布错误 / 未确认 / 确认超时 / 序列化失败）返回 error 由调用方决定降级处理；
// 不在此处落 DLQ / 记计数，以便 replay 协程复用本函数而不重复追加（对齐 RocketMQ）。
func (p *RabbitMQProducer) publishConfirmed(ctx context.Context, msg *event.Message, body []byte, topic string) error {
	rc, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	defer p.release(rc)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	pub := amqp.Publishing{
		ContentType:  "application/json",
		MessageId:    msg.EventID,
		Timestamp:    msg.Timestamp,
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}
	// 发布到 rabbitExchange，routing key = 事件映射出的主题名（与 Kafka / RocketMQ 同一套映射语义）。
	if err := rc.ch.PublishWithContext(ctx, rabbitExchange, topic, false, false, pub); err != nil {
		p.recreate(rc) // channel 已失效，重建以免污染连接池
		return fmt.Errorf("rabbitmq publish: %w", err)
	}
	// 等待本 channel 的 broker 确认回执（publisher confirm 模式）。Confirm 模式下 PublishWithContext
	// 仅表示已发往网络层，真正可靠性以 broker 回执为准。该 channel 串行发布（持 rc.mu 锁），
	// 确认顺序与发布顺序一致，故读取一个 Confirmation 即对应本条消息。
	// 若 broker 关闭 channel，confirms 随之关闭，会立即收到零值回执（Ack=false）并返回错误。
	// 落实确认等待超时：若调用方未给定带截止时间的 ctx（如 context.Background()），派生默认超时子 ctx，
	// 避免「发布已发出但 broker 确认回执丢失/极慢」时无限阻塞、冻结该 channel（其余 channel 不受影响）。
	confCtx, cancel := ctxWithTimeoutIfUnset(ctx, rabbitSendTimeoutOf(p.cfg))
	defer cancel()
	select {
	case conf := <-rc.confirms:
		if !conf.Ack {
			p.recreate(rc) // broker 拒绝（如路由不可达）：重建 channel，避免带病复用
			return fmt.Errorf("rabbitmq publish not acknowledged by broker (event=%s)", msg.EventType)
		}
		return nil
	case <-confCtx.Done():
		return fmt.Errorf("rabbitmq publish confirm wait timed out (event=%s): %w", msg.EventType, confCtx.Err())
	}
}

// relay 将一条 DLQ 消息补发到 broker（供 replay 协程调用）。成功返回 nil；
// 失败返回 error（保留在 DLQ 中待下次重试），不在本函数内重复追加 DLQ。
func (p *RabbitMQProducer) relay(_ context.Context, msg *event.Message) error {
	topic := p.topicFor(msg)
	if err := p.publishOne(context.Background(), msg); err != nil {
		return err
	}
	recordSend(typeRabbitMQ, topic, nil)
	return nil
}

// Close 关闭生产者：停止后台协程并尽力排空队列，最后关闭底层连接（幂等，重复调用安全）。
// 关闭全过程带 5s 超时兜底：broker 不可达时 confirm 等待可能长时间阻塞，超时后放弃
// 剩余发送直接返回，不拖慢进程优雅退出（未发出的消息已落 DLQ，待 broker 恢复后 replay）。
func (p *RabbitMQProducer) Close() error {
	return p.closeProducer(func() error {
		if p.pool != nil {
			close(p.pool)
			for rc := range p.pool {
				if rc.ch != nil {
					_ = rc.ch.Close()
				}
			}
		}
		return p.conn.Close()
	})
}

// RabbitMQConsumer 基于 rabbitmq/amqp091-go 的消费者实现（消费组 = 队列名）。
// 手动 Ack：处理成功才 Ack；处理失败（重试耗尽）Nack 且不 requeue（丢弃，生产建议接 DLQ 交换机，此处简化）。
type RabbitMQConsumer struct {
	cfg           *config.MQConfig
	group         string
	conn          *amqp.Connection
	ch            *amqp.Channel
	topics        map[string]string
	log           Logger
	progressStore ConsumerProgressStore // 可选：注入后启用 event_id 跨实例幂等去重（消费侧 Outbox/Dedup）
}

// NewRabbitMQConsumer 创建 RabbitMQ 消费者（建立连接、声明 topic 交换机）。
func NewRabbitMQConsumer(cfg *config.MQConfig, group string, log Logger) (*RabbitMQConsumer, error) {
	if cfg.RabbitMQ.URL == "" {
		return nil, fmt.Errorf("rabbitmq: empty url")
	}
	if log == nil {
		log = nopLogger{}
	}
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	if err := ch.ExchangeDeclare(rabbitExchange, "topic", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq exchange declare: %w", err)
	}
	return &RabbitMQConsumer{
		cfg:    cfg,
		group:  group,
		conn:   conn,
		ch:     ch,
		topics: topicMapOf(cfg),
		log:    log,
	}, nil
}

// WithProgressStore 注入消费进度/去重存储，启用 event_id 跨实例幂等去重（消费侧 Outbox/Dedup）。
// RabbitMQ 由 broker 管理投递位点（Ack），故本 store 仅用于 Seen 去重与去重标记，不接管 offset；
// 注入 SQLProgressStore（与业务库同实例）可获得跨节点 event_id 去重，与 Kafka 消费者 #3 能力对齐。
func (c *RabbitMQConsumer) WithProgressStore(s ConsumerProgressStore) *RabbitMQConsumer {
	c.progressStore = s
	return c
}

// defaultRabbitPrefetch RabbitMQ 未配置 prefetch 时的默认预取/并发上限。
const defaultRabbitPrefetch = 64

// defaultRabbitPoolSize RabbitMQ 生产者 channel 池默认大小（同时作为异步模型下后台 worker 数）。
// 单连接多 channel 并发发布以提升吞吐；每个 channel 仍串行发布（publisher confirm 要求确认顺序与
// 发布顺序一致），故并发度上限 = PoolSize。默认 8 在大多数场景足够，高吞吐可按需上调
// （受 broker 每连接 channel 上限约束，通常数千）。
const defaultRabbitPoolSize = 8

// defaultRabbitQueueSize 异步发送队列默认长度（Send 入队即返回），对齐 RocketMQ/Kafka。
const defaultRabbitQueueSize = 8192

// defaultRabbitOverflowWorkers 队列满时溢出发送的最大并发 goroutine 数（限流），默认 256。
const defaultRabbitOverflowWorkers = 256

// rabbitPoolSizeOf 返回 rabbitmq 生产者 channel 池大小（= 后台 worker 数）。<=0 时用默认值。
func rabbitPoolSizeOf(cfg *config.MQConfig) int {
	if cfg.RabbitMQ.PoolSize > 0 {
		return cfg.RabbitMQ.PoolSize
	}
	return defaultRabbitPoolSize
}

// rabbitQueueSizeOf 返回异步发送队列长度。<=0 时用默认 8192。
func rabbitQueueSizeOf(cfg *config.MQConfig) int {
	return maxInt(cfg.RabbitMQ.QueueSize, defaultRabbitQueueSize)
}

// rabbitOverflowWorkersOf 返回队列满时溢出发送的最大并发 goroutine 数。<=0 时用默认 256。
func rabbitOverflowWorkersOf(cfg *config.MQConfig) int {
	return maxInt(cfg.RabbitMQ.MaxOverflowWorkers, defaultRabbitOverflowWorkers)
}

// defaultRabbitSendTimeout 发布确认等待的默认超时上界：避免「发布已发出但 broker 确认
// 回执丢失/极慢」时持 mu 永久阻塞、冻结整个生产者。确认通常毫秒级返回，10s 仅为安全网。
const defaultRabbitSendTimeout = 10 * time.Second

// rabbitPrefetchOf 返回 rabbitmq 预取上限（同时用作有界并发上限）。<=0 时用默认值。
func rabbitPrefetchOf(cfg *config.MQConfig) int {
	if cfg.Consumer.Prefetch > 0 {
		return cfg.Consumer.Prefetch
	}
	return defaultRabbitPrefetch
}

// rabbitSendTimeoutOf 返回发布确认等待超时，未配置（<=0）时用默认值。
// 仅作确认等待的安全上界：broker 关闭 channel 会立即返回零值回执（Ack=false），不会真等到超时。
func rabbitSendTimeoutOf(cfg *config.MQConfig) time.Duration {
	return orDuration(cfg.RabbitMQ.SendTimeout, defaultRabbitSendTimeout)
}

// Subscribe 声明持久队列（名=group）并绑定 topics，启动消费；阻塞直到 ctx 取消。
//
// 并发与背压：先用 channel.Qos 设置预取上限（限制未 Ack 的在途消息数），再用同等大小的信号量
// 限制同时处理的 goroutine 数。二者对齐可在消息洪峰时提供稳定背压，避免 goroutine/内存无限增长。
// 关闭（ctx 取消）时等待在途处理完成，避免丢消息或悬挂 goroutine。
func (c *RabbitMQConsumer) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	// 死信处理：声明死信交换机与死信队列，使重试耗尽的消息（Nack 且 requeue=false）不再被直接丢弃，
	// 而是路由到死信队列保留，便于后续排查/补处理。死信按 "#" 绑定，接收主队列的所有死信。
	dlx := rabbitExchange + "-dlx"
	dlq := c.group + "-dlq"
	if err := c.ch.ExchangeDeclare(dlx, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("rabbitmq dlx declare: %w", err)
	}
	if _, err := c.ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("rabbitmq dlq declare: %w", err)
	}
	if err := c.ch.QueueBind(dlq, "#", dlx, false, nil); err != nil {
		return fmt.Errorf("rabbitmq dlq bind: %w", err)
	}

	// 主队列声明带 x-dead-letter-exchange 参数：Nack(false,false) 的消息自动路由到 dlx→dlq。
	// 若队列已存在但未带该参数，抢占式声明会返回 PRECONDITION_FAILED；此时回退为不带 DLX 参数
	// 重新声明，保证仍能消费（死信将直接丢弃，行为等价于旧版），避免破坏已存在的旧队列。
	qargs := amqp.Table{"x-dead-letter-exchange": dlx}
	q, err := c.ch.QueueDeclare(c.group, true, false, false, false, qargs)
	if err != nil {
		c.log.Warnf("rabbitmq queue declare with DLX failed (%v); fallback to declare without DLX (dead messages will be dropped instead of routed to DLQ)", err)
		q, err = c.ch.QueueDeclare(c.group, true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("rabbitmq queue declare: %w", err)
		}
	}
	for _, topic := range topics {
		if err := c.ch.QueueBind(q.Name, topic, rabbitExchange, false, nil); err != nil {
			return fmt.Errorf("rabbitmq queue bind: %w", err)
		}
	}
	prefetch := rabbitPrefetchOf(c.cfg)
	if err := c.ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("rabbitmq qos: %w", err)
	}
	deliveries, err := c.ch.Consume(q.Name, c.group, false, false, false, false, nil)
	if err != nil {
		// broker 不可达 / 队列不可用：拉取失败计数（与 kafka 的 reader fetch 错误同一口径），
		// 供 mq_consumer_fetch_errors_total 跨后端统一观测「broker 不可达」。
		recordConsumeFetchError(typeRabbitMQ, c.group)
		return fmt.Errorf("rabbitmq consume: %w", err)
	}

	sem := make(chan struct{}, prefetch)
	var wg sync.WaitGroup
	go func() {
		for d := range deliveries {
			d := d
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				_ = d.Nack(false, true) // 关闭中：重新入队，交由重启/其它实例处理
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				c.handleDelivery(ctx, d, handler)
			}()
		}
		// deliveries 通道关闭：正常出口是 ctx 取消（Subscribe 在 <-ctx.Done() 返回，连接随进程关闭）。
		// 若 ctx 未取消却到此，说明 broker 连接断开导致投递流中断（amqp 不自动重连，消费者静默停止），
		// 上报 fetch 错误便于告警「RabbitMQ 消费者因 broker 断连已失效」。
		if ctx.Err() == nil {
			recordConsumeFetchError(typeRabbitMQ, c.group)
			c.log.Errorf("rabbitmq deliveries channel closed unexpectedly (broker connection lost), consumer stopped")
		}
	}()

	<-ctx.Done()
	wg.Wait() // 等待在途处理完成后再返回，保证优雅关闭
	return nil
}

// handleDelivery 处理单条投递：反序列化 → 重试处理 → Ack/Nack。
//   - 毒消息（反序列化失败）：Nack 不 requeue，由 x-dead-letter-exchange 路由到死信队列避免反复投递；
//   - 关闭中断（ctx 取消导致重试提前返回）：Nack 且 requeue，避免误丢；
//   - 重试耗尽：Nack 不 requeue，由 x-dead-letter-exchange 路由到死信队列（若队列未配 DLX 则直接丢弃，等价旧版）。
func (c *RabbitMQConsumer) handleDelivery(ctx context.Context, d amqp.Delivery, handler MessageHandler) {
	var msg event.Message
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		c.log.Errorf("rabbitmq unmarshal failed: %v", err)
		recordConsumed(typeRabbitMQ, d.RoutingKey, resultDLQ) // 毒消息进死信队列，计入 dlq 口径
		_ = d.Nack(false, false)
		return
	}
	// 消费侧幂等去重（消费侧 Outbox/Dedup，与 Kafka #3 对齐）：跨节点/重投的同 event_id 直接跳过，
	// 仍 Ack（避免反复投递），但不重复执行业务逻辑。注入 SQLProgressStore 时生效；默认文件 store 不去重。
	if c.progressStore != nil && msg.EventID != "" {
		if seen, sErr := c.progressStore.Seen(ctx, msg.EventID); sErr == nil && seen {
			c.log.Infof("rabbitmq duplicate event %s skipped", msg.EventID)
			recordConsumed(typeRabbitMQ, d.RoutingKey, resultDedupSkipped) // 重复 event_id 跳过，计入去重口径
			_ = d.Ack(false)
			return
		}
	}
	rmax, rbase, rmaxDelay := consumerRetryOf(c.cfg)
	if err := retryProcess(ctx, &msg, handler, rmax, rbase, rmaxDelay); err != nil {
		if ctx.Err() != nil {
			_ = d.Nack(false, true) // 关闭中断，重新入队避免误丢
			return
		}
		c.log.Errorf("rabbitmq handler failed (topic=%s): %v", d.RoutingKey, err)
		recordConsumed(typeRabbitMQ, d.RoutingKey, resultDLQ) // 重试耗尽进死信队列，计入 dlq 口径
		_ = d.Nack(false, false)
		return
	}
	// 处理成功：先标记去重（持久化 event_id），再 Ack，避免「已 Ack 但去重未落库」导致重投重复处理。
	if c.progressStore != nil && msg.EventID != "" {
		if err := c.progressStore.Commit(ctx, d.RoutingKey, 0, 0, msg.EventID, false); err != nil {
			c.log.Warnf("rabbitmq progress commit failed (event=%s): %v", msg.EventID, err)
		}
	}
	recordConsumed(typeRabbitMQ, d.RoutingKey, resultProcessed)
	_ = d.Ack(false)
}

// Close 关闭 channel 与连接（关闭后 deliveries 信道关闭，消费 goroutine 自然退出）。
func (c *RabbitMQConsumer) Close() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
