package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker/v2"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// KafkaProducer 基于 segmentio/kafka-go 的生产者实现。
//
// 设计目标：
//   - 异步发送：业务调用 Send 仅入队即返回，不阻塞结算/下单等关键路径。
//   - 熔断保护：连续发送失败时熔断器打开，快速失败避免雪崩，半开态自动探测恢复。
//   - 本地 DLQ 降级：Broker 不可用（熔断打开 / 发送失败）时，消息以 JSONL 落盘到
//     DLQLocalPath，由后台 replay 协程在 Broker 恢复后自动补发，保证事件不丢。
type KafkaProducer struct {
	producerBase
	cfg    *config.KafkaConfig
	writer *kafka.Writer
}

// errProducerOverflow 表示队列满且溢出池也已打满，消息已降级到 DLQ。
// 三队列生产者（Kafka / RabbitMQ / RocketMQ）共用此哨兵，错误信息为通用描述（不绑定具体 MQ 类型），
// 避免在 RabbitMQ / RocketMQ 的降级日志里出现 "kafka producer" 字样。
var errProducerOverflow = errors.New("mq producer: queue full and overflow workers saturated")

// errBreakerOpen 表示熔断器打开（或处于半开探测名额耗尽），broker 大概率不可达但仍可能正在恢复。
// 与真实发送错误区分：调用方（loop）应保留消息等待恢复后直接发出，而非立即降级 DLQ——否则 broker
// 恢复瞬间本可直接发出的消息会多走「队列→loop→DLQ→replay」一跳，引入可达 replay 间隔的额外延迟。
var errBreakerOpen = errors.New("kafka producer: circuit breaker open")

// requeueBackoff 是熔断打开时 loop 把消息放回队列重试的退避间隔：避免 broker 持续不可达时
// loop 忙等打满 CPU，也给 broker 恢复、熔断器进入半开探测留出时间。远小于 breaker Timeout，
// 故 broker 恢复瞬间的感知延迟可忽略；恢复后消息经半开探测成功即直接发出（不再落 DLQ）。
const requeueBackoff = 200 * time.Millisecond

// NewKafkaProducer 创建 Kafka 生产者并启动后台发送/重放协程。
// log 为分级日志（熔断状态变更、DLQ 落盘/重放等），nil 时静默；brokers 为空返回 error。
func NewKafkaProducer(cfg *config.KafkaConfig, log Logger) (*KafkaProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: empty brokers")
	}
	if log == nil {
		log = nopLogger{}
	}

	pc := cfg.Producer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.Hash{}, // 按 Key 哈希分区，保证同一用户事件有序
		RequiredAcks: acksOf(pc.Acks),
		Compression:  compressionOf(pc.Compression),
		MaxAttempts:  maxInt(pc.MaxRetries+1, 1),
		WriteTimeout: orDuration(pc.DeliveryTimeout, 5*time.Second),
		BatchBytes:   int64(maxInt(pc.BatchSize, 1)),
		Async:        false, // 由本实现自行做异步队列 + 熔断，便于错误回收
	}

	// 队列容量：尊重配置值；未配置（<=0）时回退默认 8192，避免无缓冲 channel 阻塞 Send。
	// 注：此前 maxInt(QueueSize, 8192) 会把小容量配置（如测试用 1）静默覆盖为 8192，导致
	// 「满队列溢出」行为失效；此处改为仅对 <=0 兜底，正数配置真正生效。
	queueSize := pc.QueueSize
	if queueSize <= 0 {
		queueSize = 8192
	}
	// 溢出池并发上限：尊重配置值（含 0=显式禁用溢出池，队列满即降级 DLQ）；
	// 仅负数（未配置）回退默认 256。原 maxInt(上限,256) 会把 0 静默覆盖为 256，致配置失效。
	overflowWorkers := pc.MaxOverflowWorkers
	if overflowWorkers < 0 {
		overflowWorkers = 256
	}

	p := &KafkaProducer{
		cfg:    cfg,
		writer: writer,
	}
	// 共享发送脚手架（队列 / 溢出池 / DLQ / 熔断 / 主题 / 关闭）统一由 producerBase 持有，
	// 仅协议相关的「真正发送一条」(sendOne) 与「DLQ 补发一条」(relay) 在此注入。
	p.initBase(typeKafka, queueSize, overflowWorkers,
		newDLQStore(typeKafka, pc.DLQEnabled, pc.DLQLocalPath, log),
		gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
			Name:        "kafka-producer",
			MaxRequests: 5, // 半开态允许试探的请求数
			Interval:    30 * time.Second,
			Timeout:     30 * time.Second, // 打开后等待多久进入半开
			ReadyToTrip: func(c gobreaker.Counts) bool {
				return c.ConsecutiveFailures >= 5
			},
			IsSuccessful: func(err error) bool { return err == nil },
			OnStateChange: func(name string, from, to gobreaker.State) {
				log.Infof("kafka circuit breaker %s: %s -> %s", name, from, to)
			},
		}),
		// 统一用 eventTopicsMap 作为事件→主题的唯一来源，与 topicMapOf / EnsureTopics 保持一致，
		// 避免此处内联漏配新事件（如 CacheEvents）导致该事件被错误回落到 "events"。
		eventTopicsMap(cfg.Topics),
		log,
	)
	p.publish = p.sendOne
	p.relayFn = p.relay

	p.wg.Add(1)
	go p.loop()

	if pc.DLQEnabled && pc.DLQLocalPath != "" {
		p.wg.Add(1)
		go p.replayLoop()
	}

	return p, nil
}

// SendSync 同步发送单条消息：阻塞等待 broker 确认（WriteMessages 落盘回执）后返回 error。
// 与 Send 的异步入队不同，失败不降级 DLQ，直接把 error 返回调用方，便于关键链路（如支付成功事件）
// 在发送失败时立即决策（回滚/重试）。仍受 DeliveryTimeout 超时上界约束，broker 不可达时返回 error 而非永久阻塞。
func (p *KafkaProducer) SendSync(ctx context.Context, msg *event.Message) error {
	kmsg, err := p.toKafkaMessage(msg)
	if err != nil {
		return fmt.Errorf("kafka marshal: %w", err)
	}
	sendCtx, cancel := ctxWithTimeoutIfUnset(ctx, kafkaSendTimeoutOf(p.cfg))
	defer cancel()
	_, err = p.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, p.writer.WriteMessages(sendCtx, kmsg)
	})
	recordSend(typeKafka, kmsg.Topic, err)
	return err
}

// Close 关闭生产者：停止后台协程并尽力排空队列，最后关闭 writer。
// 关闭全过程带 5s 超时兜底：Broker 不可达时 writer.Close() 的 flush 与队列排空可能长时间
// 阻塞，超时后放弃剩余 flush 直接返回，避免拖慢进程优雅退出（未发出的消息已落 DLQ）。
func (p *KafkaProducer) Close() error {
	return p.closeProducer(func() error { return p.writer.Close() })
}

// loop 后台发送循环：从队列取消息发送，收到关闭信号时先排空再退出。
func (p *KafkaProducer) loop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.closeCh:
			// 排空剩余消息：无论熔断状态都尽力发送，失败（含熔断打开）则降级 DLQ，确保不丢。
			// 加预算上限避免 broker 不可达时 sendOne 逐个超时（5s × queueSize）导致 loop 永不退出。
			const drainBudget = 200
			drained := 0
			for {
				select {
				case msg := <-p.queue:
					if err := p.sendOne(context.Background(), msg); err != nil {
						p.fallbackToDLQ(msg, err)
					}
					drained++
					if drained >= drainBudget {
						// 预算耗尽：剩余消息直接落 DLQ（sendOne 逐个超时说明 broker 已不可达，
						// 剩余消息几乎必然同样超时，不如一批落 DLQ 由 replay 补发）。
						p.drainToDLQ()
						return
					}
				default:
					return
				}
			}
		case msg := <-p.queue:
			// 仅「熔断打开」时保留消息在队列中等待 broker 恢复后直接发出，避免降级 DLQ 的额外一跳；
			// 真实发送失败已在 publish 内降级 DLQ、marshal 错误不可重试直接丢弃，二者均不再 requeue
			// （避免无效重试 / 无限 requeue 死循环）。
			p.processMessage(context.Background(), msg)
		}
	}
}

// kafkaSendTimeoutOf 返回 Kafka 发送超时上界（复用生产者 DeliveryTimeout 配置），未配置时用默认 5s。
// 与 RocketMQ / RabbitMQ 的 send_timeout 对齐：调用方传 context.Background()（loop / 溢出池 / replay）
// 时仍保证写入有上界，避免 broker 不可达 / 极慢时 WriteMessages 无限挂起。
func kafkaSendTimeoutOf(cfg *config.KafkaConfig) time.Duration {
	return orDuration(cfg.Producer.DeliveryTimeout, 5*time.Second)
}

// sendOne 发送单条消息（经熔断器保护）。
//   - 真实发送错误（含 broker 不可达但熔断仍关闭）：降级到 DLQ 并返回 nil。
//   - 熔断器打开 / 半开探测名额耗尽（broker 大概率不可达、可能正在恢复）：返回 errBreakerOpen，
//     不在此降级 DLQ，交由调用方决定（loop 保留等待恢复、溢出池 / 关闭排空则降级），
//     以此消除 broker 恢复瞬间「队列→loop→DLQ」的额外一跳。
func (p *KafkaProducer) sendOne(ctx context.Context, msg *event.Message) error {
	kmsg, err := p.toKafkaMessage(msg)
	if err != nil {
		p.log.Errorf("kafka marshal message failed: %v", err)
		return err
	}

	// 落实统一发送超时上界：调用方多传 context.Background()（loop / 溢出池 / replay），
	// 以 DeliveryTimeout 派生子 ctx，避免 broker 不可达 / 极慢时 WriteMessages 无限挂起，
	// 与 RocketMQ / RabbitMQ 的 send_timeout 语义对齐。已带 deadline 的 ctx 原样透传。
	sendCtx, cancel := ctxWithTimeoutIfUnset(ctx, kafkaSendTimeoutOf(p.cfg))
	defer cancel()

	_, err = p.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, p.writer.WriteMessages(sendCtx, kmsg)
	})
	recordSend(typeKafka, kmsg.Topic, err)
	if err != nil {
		// 熔断打开 / 半开名额耗尽：broker 大概率仍不可达，但可能正在恢复——不降级 DLQ，
		// 交由调用方保留消息等待恢复后直接发出（消除恢复瞬间的额外一跳）。
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return errBreakerOpen
		}
		p.log.Warnf("kafka send failed (event=%s topic=%s): %v", msg.EventType, kmsg.Topic, err)
		recordDLQ(typeKafka, kmsg.Topic)
		p.dlq.append(kmsg.Topic, msg, err)
	}
	return nil
}

// drainToDLQ 将队列中剩余消息全部降级到本地 DLQ（关闭排空预算耗尽时使用）。
// 直接批量落盘，不逐个 sendOne，避免在 broker 不可达时逐条超时阻塞关闭流程。
func (p *KafkaProducer) drainToDLQ() {
	n := 0
	for {
		select {
		case msg := <-p.queue:
			recordDLQ(typeKafka, p.topicFor(msg))
			p.dlq.append(p.topicFor(msg), msg, errProducerOverflow)
			n++
		default:
			if n > 0 {
				p.log.Warnf("kafka producer drainToDLQ: %d messages flushed to DLQ during shutdown", n)
			}
			return
		}
	}
}

// relay 将一条 DLQ 消息补发到 broker（供 replay 协程调用）。含熔断保护；成功返回 nil。
func (p *KafkaProducer) relay(_ context.Context, msg *event.Message) error {
	kmsg, err := p.toKafkaMessage(msg)
	if err != nil {
		return err
	}
	_, err = p.breaker.Execute(func() (struct{}, error) {
		return struct{}{}, p.writer.WriteMessages(context.Background(), kmsg)
	})
	if err == nil {
		recordSend(typeKafka, kmsg.Topic, nil) // DLQ 补发成功计入发送成功
	}
	return err
}

// toKafkaMessage 将统一事件转换为 kafka-go 消息（含分区 Key 与 Header）。
func (p *KafkaProducer) toKafkaMessage(msg *event.Message) (kafka.Message, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("marshal event: %w", err)
	}
	headers := make([]kafka.Header, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return kafka.Message{
		Topic:   p.topicFor(msg),
		Key:     []byte(msg.Key),
		Value:   body,
		Headers: headers,
		Time:    msg.Timestamp,
	}, nil
}

// acksOf 将配置字符串映射为 kafka.RequiredAcks。
func acksOf(s string) kafka.RequiredAcks {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "all", "-1":
		return kafka.RequireAll
	case "1":
		return kafka.RequireOne
	case "0":
		return kafka.RequireNone
	default:
		return kafka.RequireAll
	}
}

// compressionOf 将配置字符串映射为 kafka.Compression。
func compressionOf(s string) kafka.Compression {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return kafka.Compression(0) // 未压缩（零值）
	}
}

// maxInt 返回两整数较大者。
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// orDuration 返回 d；d<=0 时返回 def。
func orDuration(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// EnsureTopics 在启动时幂等地确保消费 / 生产所需的 topic 存在。
//
// 根因：若 broker 未开启 auto.create.topics，订阅 / 发送不存在的 topic 会让 kafka-go 的
// FetchMessage 返回 UnknownServerError（错误串 "[-1] Unknown: an unexpected server error
// occurred"），消费循环因此反复失败且无法通过重试自愈。这里主动建好 topic 以消掉该根因。
// 已存在的 topic 会被 CreateTopics 报 TopicAlreadyExists，忽略即可（幂等）。
//
// 分区数 / 副本因子传 -1 表示沿用 broker 默认值，避免单 broker 环境下误设 >1 副本触发
// InvalidReplicationFactor。本函数 best-effort：建 topic 失败仅告警，不阻断进程启动。
func EnsureTopics(cfg *config.KafkaConfig, log Logger) {
	if len(cfg.Brokers) == 0 {
		return
	}
	if log == nil {
		log = nopLogger{}
	}

	// 收集所有被引用的 topic（事件类型 -> topic 映射），去重后批量创建
	raw := []string{
		cfg.Topics.UserEvents,
		cfg.Topics.IdleEvents,
		cfg.Topics.TaskEvents,
		cfg.Topics.ShopEvents,
		cfg.Topics.UserPoints,
		cfg.Topics.CacheEvents,
	}
	seen := make(map[string]struct{}, len(raw))
	topics := make([]kafka.TopicConfig, 0, len(raw))
	dedup := make([]string, 0, len(raw))
	for _, name := range raw {
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		dedup = append(dedup, name)
		topics = append(topics, kafka.TopicConfig{
			Topic:             name,
			NumPartitions:     -1, // 沿用 broker 默认
			ReplicationFactor: -1, // 沿用 broker 默认
		})
	}
	if len(topics) == 0 {
		return
	}

	client := &kafka.Client{Addr: kafka.TCP(cfg.Brokers...)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 探测 broker 实际支持的 API 版本（尤其 FETCH），用于定位 kafka-go 与 broker 的
	// 协议 / 版本协商不兼容（常见表现即消费循环返回 [-1] Unknown）。
	probeBrokerAPIVersions(client, log)

	log.Infof("kafka ensuring topics exist: %v", dedup)
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: topics})
	if err != nil {
		log.Warnf("kafka ensure topics failed (topics may need manual creation): %v", err)
		return
	}
	for topic, terr := range resp.Errors {
		if terr == nil {
			continue
		}
		// TopicAlreadyExists 视为成功（幂等），其余错误告警但不致命
		if errors.Is(terr, kafka.TopicAlreadyExists) {
			continue
		}
		log.Warnf("kafka create topic %q failed: %v", topic, terr)
	}
}

// probeBrokerAPIVersions 打印 broker 支持的 API 版本范围，重点关注 FETCH。
// 若 broker 返回的 FETCH 最大版本高于 kafka-go 实际能正确处理的版本（常见于 KRaft 模式
// 的 Kafka 3.x 误报版本号），消费循环的 FetchMessage 就会返回 UnknownServerError([-1]
// Unknown)。该探测提供定位证据，不直接修复，但能确认是否为版本协商不兼容。
func probeBrokerAPIVersions(client *kafka.Client, log Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.ApiVersions(ctx, &kafka.ApiVersionsRequest{Addr: client.Addr})
	if err != nil {
		log.Warnf("kafka broker ApiVersions probe failed: %v", err)
		return
	}
	for _, k := range resp.ApiKeys {
		if k.ApiName == "Fetch" {
			log.Infof("kafka broker supports FETCH API v%d..v%d", k.MinVersion, k.MaxVersion)
		}
	}
	log.Infof("kafka broker ApiVersions probe ok (supported api keys=%d)", len(resp.ApiKeys))
}

// isBreakerOpen 判断错误是否由熔断器「打开 / 半开探测名额耗尽」导致（broker 大概率仍不可达、可能正在恢复）。
// 三个生产者统一用此判断区分「应保留消息等待恢复（requeue）」与「真实发送失败（降级 DLQ）」：
//   - Kafka 的 sendOne 在熔断打开时返回 errBreakerOpen 哨兵；
//   - RabbitMQ / RocketMQ 的 publishOne/sendOne 在熔断打开时返回 gobreaker.ErrOpenState / ErrTooManyRequests。
func isBreakerOpen(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errBreakerOpen) ||
		errors.Is(err, gobreaker.ErrOpenState) ||
		errors.Is(err, gobreaker.ErrTooManyRequests)
}

// newProducerBreaker 构造生产者通用熔断器（RabbitMQ / RocketMQ 复用，与 Kafka 同参）。
// marshal 等「非 broker 故障」错误在调用方（publishOne/sendOne）已排除在 breaker 之外，
// 故 IsSuccessful 采用 err==nil；broker 不可达/确认超时会累积连续失败并在达到阈值后打开，
// 快速失败避免雪崩，半开态自动探测恢复（对齐 Kafka 生产者）。
func newProducerBreaker(name string, log Logger) *gobreaker.CircuitBreaker[struct{}] {
	return gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		Name:        name,
		MaxRequests: 5,
		Interval:    30 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= 5
		},
		IsSuccessful: func(err error) bool { return err == nil },
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Infof("mq circuit breaker %s: %s -> %s", name, from, to)
		},
	})
}
