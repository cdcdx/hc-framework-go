package mq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
)

// Logger 带级别的消费侧日志，避免把错误统一用 INFO 级别打印而淹没真实告警。
// *zap.SugaredLogger 天然满足该接口（具备 Infof/Warnf/Errorf 方法）。
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// nopLogger 空实现（NewKafkaConsumer 的 log 为 nil 时使用，避免 nil 判空散落各处）。
type nopLogger struct{}

func (nopLogger) Infof(string, ...interface{})  {}
func (nopLogger) Warnf(string, ...interface{})  {}
func (nopLogger) Errorf(string, ...interface{}) {}

// KafkaConsumer 基于 segmentio/kafka-go 的消费者实现（无消费组 / 直连分区）。
//
// 为什么不用消费组（GroupID）：
//
//	kafka-go v0.4.51 的 group coordinator 协议与 Kafka 4.0 的 KRaft 模式不兼容，
//	只要带 GroupID，FetchMessage 就会在 coordinator 握手阶段返回
//	UNKNOWN_SERVER_ERROR(-1) 且无法自愈。参考项目 high-concurrency-framework-go
//	之所以能用消费组，是因为它跑在 ZooKeeper 模式的 broker 上，而本项目是 KRaft 4.0。
//
// 因此这里采用「无消费组 + 按分区直读」：
//   - 对每个 topic 的每个 partition 创建一个不带 GroupID 的 Reader，直连分区读取，
//     完全不经过 group coordinator，绕开 KRaft 的 -1 Unknown。
//   - offset 由本地 offsetStore 持久化（默认 ./data/kafka-offsets.json），重启后从
//     已处理位置续读，弥补了无消费组「跨重启 offset 不持久」的缺陷。
//   - 代价：失去了消费组的自动再均衡与多实例水平扩展能力（多实例需自行分配 partition）。
//
// 保留的本项目能力：Worker 池并发处理、指数退避重试、本地文件 DLQ、关闭超时兜底。
type KafkaConsumer struct {
	cfg     *config.KafkaConfig
	groupID string // 仅作记录，本实现不使用消费组

	log     Logger
	closeCh chan struct{}
	wg      sync.WaitGroup
	dlqMu   sync.Mutex
	// dlqBacklog 按 topic 记录本地 DLQ 当前存量（供 mq_consumer_dlq_backlog gauge）。
	// 消费者 DLQ 无自动 replay（存档语义）：进程内只随 appendDLQ 增长，启动时由 initDLQBacklog
	// 扫描已有文件行数初始化，反映磁盘真实存量。读写均在 dlqMu 保护下（与 appendDLQ 落盘同锁）。
	dlqBacklog map[string]int64

	// partitionFn 分区查询实现（见 13 §6.5 #4 / §3.57）。默认指向 c.partitions（真实 dial broker）；
	// 测试可覆盖以模拟分区数变化，无需真实 Kafka。
	partitionFn func(ctx context.Context, topic string) ([]int, error)

	// progressStore 消费进度/去重存储（消费侧 Outbox/Dedup）。nil 时使用默认文件存储
	// （订阅时按 offset_store_path 构造）。注入 SQLProgressStore（与业务库同实例）可获得
	// 跨分区 event_id 幂等去重 + offset 与业务写原子提交的能力。
	progressStore ConsumerProgressStore

	closeOnce sync.Once
}

// consumerDLQRecord 消费失败落盘记录。
type consumerDLQRecord struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Error     string    `json:"error"`
	TS        time.Time `json:"ts"`
}

// readerMeta 记录每个分区 reader 的归属（topic/partition），供消费 lag / offset 指标抓取
// （见 13 §6.5 / §3.59）按 topic/partition 维度暴露到 Prometheus，而不依赖 reader 自身记忆。
type readerMeta struct {
	r         *kafka.Reader
	topic     string
	partition int
}

// fetchErrEscalateAfter 连续 fetch 失败达到该次数后，日志从 Warn 升级为 Error，
// 让持续故障（如 topic 不存在、broker 长期不可用）更醒目，同时配合指数退避避免刷屏。
const fetchErrEscalateAfter = 5

// defaultKafkaWorkerPoolSize 消费并发 worker 下限（worker_pool_size 未配置时使用）。
// 单 worker 会锁死消费吞吐（每条 handler 完成前不取下一消息），8 是兼顾下游压力的保守起点，
// 高 ingest 场景应在配置中显式上调（32~64）。
const defaultKafkaWorkerPoolSize = 8

// NewKafkaConsumer 创建 Kafka 消费者。brokers 为空返回 error。
// groupID 在本实现中不再使用（消费组在 KRaft 4.0 下不可用），仅作记录保留。
// log 为 nil 时静默（nopLogger），调用方可不传日志。
func NewKafkaConsumer(cfg *config.KafkaConfig, log Logger) (*KafkaConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: empty brokers")
	}
	if log == nil {
		log = nopLogger{}
	}
	// 无消费组模式下部分遗留字段已失效（死配置，见 13 §6.5 #5 / §3.55）：启动时告警提示，
	// 避免运维误以为这些配置生效（如误以为仍走消费组 auto-commit）。非致命，不阻断启动。
	for _, w := range cfg.Consumer.CheckDeadConfig() {
		log.Warnf("kafka config: %s", w)
	}
	// 消费模式显式声明（见 13 §6.5 / §3.60「配置收敛」）：
	//   - "direct"（默认）：无消费组 + 按分区直读，规避 kafka-go v0.4.51 与 Kafka 4.0 KRaft
	//     group coordinator 不兼容导致的 -1 Unknown。
	//   - "group"：消费组模式——当前 kafka-go v0.4.51 在 KRaft 4.0 下不可用（coordinator 握手
	//     返回 UNKNOWN_SERVER_ERROR(-1) 且无法自愈），故显式配置为 group 直接启动报错（P0 待升级客户端）。
	//   空值回落为 "direct"；未知值告警并回落 direct。
	switch strings.ToLower(cfg.Consumer.Mode) {
	case "", "direct":
		// 默认无消费组直读模式，继续
	case "group":
		return nil, fmt.Errorf("kafka: consumer.mode=group is unsupported under Kafka 4.0 KRaft with kafka-go v0.4.51 (group coordinator returns UNKNOWN_SERVER_ERROR(-1)); use 'direct' (no-group per-partition read).")
	default:
		log.Warnf("kafka: unknown consumer.mode=%q, falling back to 'direct' (no-group per-partition read)", cfg.Consumer.Mode)
	}
	c := &KafkaConsumer{
		cfg:     cfg,
		groupID: cfg.Consumer.GroupID,
		log:     log,
		closeCh: make(chan struct{}),
	}
	c.partitionFn = c.partitions // 默认真实查询；测试可覆盖（见 13 §6.5 #4 / §3.57）
	return c, nil
}

// WithProgressStore 注入消费进度/去重存储（消费侧 Outbox/Dedup）。
// 传入 SQLProgressStore（与业务库同实例）可获得跨分区 event_id 幂等去重，并让 offset 推进
// 与业务写落在同一 DB 事务（由 handler 在事务内提交），达到消费侧 exactly-once；
// 不调用则使用默认文件存储（offset_store_path）。
func (c *KafkaConsumer) WithProgressStore(s ConsumerProgressStore) *KafkaConsumer {
	c.progressStore = s
	return c
}

// Subscribe 订阅主题并阻塞消费，直到 ctx 取消或 Close 被调用。
// handler 为业务处理函数（如更新任务进度）。
//
// 采用「无消费组 + 按分区直读」：对每个 topic 的每个 partition 建一个独立 Reader，
// 不经过 group coordinator，兼容 Kafka 4.0 KRaft。offset 持久化到本地 store，
// 重启后从已处理位置续读。
func (c *KafkaConsumer) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	if len(topics) == 0 {
		return fmt.Errorf("kafka: empty topics")
	}
	cc := c.cfg.Consumer

	// 防御：若通过结构体字面量构造（未走 NewKafkaConsumer），partitionFn 可能为 nil，回落真实实现。
	if c.partitionFn == nil {
		c.partitionFn = c.partitions
	}

	if cc.DLQLocalPath != "" {
		if err := os.MkdirAll(cc.DLQLocalPath, 0o755); err != nil {
			return fmt.Errorf("kafka: mkdir consumer dlq: %w", err)
		}
		// 扫描已有 DLQ 文件行数，初始化存量 gauge（跨重启反映磁盘真实存量，区别于会重置的 counter）。
		c.initDLQBacklog(cc.DLQLocalPath, cc.DLQSuffix)
	}

	// 消费进度/去重存储：注入则优先使用（如 SQLProgressStore，与业务库同实例），否则默认文件存储。
	var store ConsumerProgressStore
	if c.progressStore != nil {
		store = c.progressStore
	} else {
		// offset 持久化路径（默认 ./data/kafka-offsets.json）。
		// 多实例（InstanceCount>1）时按实例区分文件名（见 perInstanceOffsetPath），
		// 避免多个实例写同一文件互相覆盖导致 offset 损坏（13 §6.5 #2 根因）。
		offsetPath := orStr(cc.OffsetStorePath, "./data/kafka-offsets.json")
		offsetPath = c.instanceOffsetPath(offsetPath)
		// 反直觉告警（13 §6.5 #6）：start_from_latest=true 仅当 offset 文件不存在时生效，
		// 文件已存在则续读已处理位置，改 true 不能跳过积压。
		c.warnStartFromLatestIfOffsetExists(offsetPath)
		if dir := filepath.Dir(offsetPath); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("kafka: mkdir offset store: %w", err)
			}
		}
		store = newFileProgressStore(offsetPath, cc.OffsetFlushInterval)
	}

	// worker 池：以信号量限制并发处理数。未配置（<=0）时用默认 8（原先为 1，单分区/低并发下消费
	// 吞吐被严重锁死；8 是兼顾 DB/下游压力的保守下界，高 ingest 场景建议显式上调至 32~64）。
	poolSize := maxInt(cc.WorkerPoolSize, defaultKafkaWorkerPoolSize)
	sem := make(chan struct{}, poolSize)

	// 关闭信号：ctx 取消或 Close() 均触发 reader 关闭以解除 FetchMessage 阻塞
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-c.closeCh:
			cancel()
		case <-fetchCtx.Done():
		}
	}()

	// 与参考实现对齐：FirstOffset（默认从头消费）/ LastOffset（跳过积压）二选一。
	startOffset := kafka.FirstOffset
	if cc.StartFromLatest {
		startOffset = kafka.LastOffset
	}

	// 对每个 topic 的每个 partition 建一个独立 Reader（无 GroupID，直连分区）。
	// 韧性策略（见 13 §6.5 #7 / §3.54）：非严格模式（默认）下，单个 topic 查询 partition 失败
	// 时仅告警并跳过、继续订阅其余就绪 topic，仅当【全部】 topic 都失败时才返回错误；
	// 严格模式（partition_fetch_strict=true）保留旧行为——任一失败即整体启动失败。
	var readers []*kafka.Reader
	var readerMetas []readerMeta // 与 readers 一一对应，供 lag/offset 指标抓取（见 §3.59）
	var fetchErrs []error

	// 线程安全的 reader 集合：初始循环与「分区扩容热感知」协程都可能新增 reader（见 13 §6.5 #4 / §3.57）。
	// closeReaders 在 wg.Wait() 之后执行，此时热感知协程已退出、无并发追加，slice 读取安全。
	subMu := sync.Mutex{}
	active := make(map[string]bool) // "topic/part" -> 已建 reader
	partKey := func(t string, p int) string { return offsetKey(t, p) }

	// spawnReader 创建单个 (topic, partition) 的直连 Reader 并启动读取协程；已存在则幂等跳过。
	spawnReader := func(topic string, p int) {
		k := partKey(topic, p)
		subMu.Lock()
		if active[k] {
			subMu.Unlock()
			return
		}
		active[k] = true
		subMu.Unlock()

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     c.cfg.Brokers,
			Topic:       topic,
			Partition:   p,
			MinBytes:    orInt(cc.MinBytes, 1024),
			MaxBytes:    orInt(cc.MaxBytes, 10*1024*1024),
			MaxWait:     orDuration(cc.BatchTriggerInterval, time.Second),
			StartOffset: startOffset,
		})
		// 若已记录过进度，则从此处续读（覆盖默认 StartOffset）
		if off, ok, _ := store.Get(fetchCtx, topic, p); ok {
			_ = r.SetOffset(off)
		}
		subMu.Lock()
		readers = append(readers, r)
		readerMetas = append(readerMetas, readerMeta{r: r, topic: topic, partition: p})
		subMu.Unlock()
		c.wg.Add(1)
		go c.readerLoop(fetchCtx, r, store, sem, handler)
	}

	for _, topic := range topics {
		parts, err := c.partitionFn(fetchCtx, topic)
		if err != nil {
			if cc.PartitionFetchStrict {
				cancel()
				c.closeReaders(readers)
				return fmt.Errorf("kafka: read partitions for topic %q: %w", topic, err)
			}
			// 韧性模式：跳过该 topic，继续订阅其余 topic（重启或分区扩容热感知后再生效）。
			c.log.Warnf("kafka consumer skip topic %q: read partitions failed (set mq.kafka.consumer.partition_fetch_strict=true to fail fast): %v", topic, err)
			fetchErrs = append(fetchErrs, fmt.Errorf("%s: %w", topic, err))
			continue
		}
		// 无消费组模式多实例分区分配（替代消费组再均衡，见 13 §6.5 #1 / §3.53）：
		// InstanceCount>1 时仅保留本实例应消费的分区子集，多个实例各持互不重叠分区，实现水平扩展。
		// InstanceID 越界时退化为全量消费并告警，避免「分区取模恒不成立 → 订阅 0 分区 → 静默不消费」。
		parts = c.assignPartitions(parts)
		for _, p := range parts {
			spawnReader(topic, p)
		}
	}

	// 韧性模式下若【全部】 topic 都查询失败（零 reader、无任何分区可消费），仍应返回错误：
	// 避免「订阅静默成功却什么都不消费」的隐蔽故障（broker 全不可达 / 全部 topic 未创建）。
	if len(readers) == 0 {
		cancel()
		if len(fetchErrs) > 0 {
			return fmt.Errorf("kafka: all %d topic(s) failed partition fetch, nothing to consume: %w", len(fetchErrs), errors.Join(fetchErrs...))
		}
		// 无 fetchErrs 但仍零 reader：多实例分区分配后本实例恰好未分到任何分区
		// （instance_count > 分区总数时可能发生），属配置问题，明确报错而非静默空转。
		return fmt.Errorf("kafka: no partitions assigned to this instance (check partition count vs instance_count), nothing to consume")
	}

	// 分区扩容热感知（见 13 §6.5 #4 / §3.57）：定期重查各 topic 分区数，发现新增分区即动态建 reader，
	// 无需重启即可消费新分区。partition_refresh_interval<=0 时禁用（保持旧行为）。
	// topic 查询瞬时失败仅告警、跳过本轮（下个周期再试），不中断消费。
	if refresh := cc.PartitionRefreshInterval; refresh > 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ticker := time.NewTicker(refresh)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for _, topic := range topics {
						parts, err := c.partitionFn(fetchCtx, topic)
						if err != nil {
							c.log.Warnf("kafka consumer partition refresh failed (topic=%q): %v", topic, err)
							continue
						}
						for _, p := range c.assignPartitions(parts) {
							if !active[partKey(topic, p)] { // 二次判定，spawnReader 内部亦幂等
								c.log.Infof("kafka consumer new partition detected (topic=%s partition=%d), spawning reader", topic, p)
								spawnReader(topic, p)
							}
						}
					}
				case <-fetchCtx.Done():
					return
				}
			}
		}()
	} else {
		c.log.Infof("kafka consumer partition auto-refresh disabled (partition_refresh_interval<=0)")
	}

	// 多连接开销告警（见 13 §6.5 #8）：每个 (topic, partition) 一个 Reader 一个 broker TCP 连接，
	// 分区多时连接数=分区数。超过阈值时提示考虑合并分区或迁移消费组；connection_warn_threshold<=0 不告警。
	if thr := cc.ConnectionWarnThreshold; thr > 0 && len(readers) > thr {
		c.log.Warnf("kafka consumer has %d readers (one broker TCP connection each); high connection count may pressure the broker. Consider consolidating partitions or migrating to consumer group", len(readers))
	}

	// 消费 lag 可观测（见 13 §6.5 / §3.59）：后台周期读取各 reader 的 Stats().Lag / Offset，
	// 经 ConsumerMetrics 暴露到 Prometheus（mq_consumer_lag / mq_consumer_offset），用于观测消费滞后。
	// 未注入指标实现（如未启用 Prometheus）时这些调用为 no-op，不影响消费。抓取间隔由
	// lag_scrape_interval 控制，<=0 用默认 15s。纳入 wg：fetchCtx.Done 触发后本协程先结束，
	// closeReaders 才开始关闭 reader，避免读取已关闭的 reader。
	lagInterval := cc.LagScrapeInterval
	if lagInterval <= 0 {
		lagInterval = 15 * time.Second
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(lagInterval)
		defer ticker.Stop()
		// 启动即首抓一次，避免首个 lag_scrape_interval 窗口内指标空白（仪表盘/告警无数据）。
		// 抓前复制快照，避免与热感知协程的并发 append 竞争 readerMetas。
		subMu.Lock()
		initMetas := make([]readerMeta, len(readerMetas))
		copy(initMetas, readerMetas)
		subMu.Unlock()
		scrapeConsumerLag(initMetas)
		for {
			select {
			case <-ticker.C:
				// 复制快照，避免与热感知协程的并发 append 竞争 readerMetas。
				subMu.Lock()
				metas := make([]readerMeta, len(readerMetas))
				copy(metas, readerMetas)
				subMu.Unlock()
				scrapeConsumerLag(metas)
			case <-fetchCtx.Done():
				return
			}
		}
	}()

	// 后台周期落盘：仅文件等「依赖周期落盘」的实现需要；SQL 实现每条 Commit 即落库，无需此协程。
	// 在 readers 创建后再启动，确保分区查询失败等提前返回路径无需清理该协程。
	if iv := store.FlushInterval(); iv > 0 {
		flushStop := make(chan struct{})
		defer close(flushStop)
		go func() {
			ticker := time.NewTicker(iv)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if err := store.Flush(); err != nil {
						c.log.Warnf("kafka consumer offset flush failed: %v", err)
					}
			case <-flushStop:
				if err := store.Flush(); err != nil {
					c.log.Warnf("kafka consumer offset flush on shutdown failed: %v", err)
				}
				return
				}
			}
		}()
	}
	defer store.Close()

	commitMode := "concurrent"
	if cc.OrderedCommit {
		commitMode = "ordered(per-partition-serial)"
	}
	c.log.Infof("kafka consumer subscribed (no-group direct) topics=%v readers=%d workers=%d commit=%s progressStore=%T%s",
		topics, len(readers), poolSize, commitMode, store, c.instanceDesc())

	// 等待退出（ctx 取消 / Close）
	<-fetchCtx.Done()

	// drain：等待在途处理完成（handler 尊重 ctx，会随关闭信号提前退出重试循环）。
	c.wg.Wait()
	// 关闭所有 reader（带超时兜底，避免 broker 不可达时 Close 挂起拖住关闭流程）。
	c.closeReaders(readers)
	return nil
}

// partitions 查询某 topic 的 partition 列表（仅用于直连分区消费，不经过 coordinator）。
// 遍历所有配置的 broker，任一可达即返回，避免「只连 Brokers[0] 时首节点宕机即整体失败」的可用性短板。
func (c *KafkaConsumer) partitions(ctx context.Context, topic string) ([]int, error) {
	var lastErr error
	for _, b := range c.cfg.Brokers {
		conn, err := kafka.DialContext(ctx, "tcp", b)
		if err != nil {
			lastErr = err
			continue
		}
		ps, err := conn.ReadPartitions(topic)
		conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		out := make([]int, len(ps))
		for i, p := range ps {
			out[i] = p.ID
		}
		return out, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("kafka: no brokers configured")
	}
	return nil, lastErr
}

// readerLoop 持续从单个 (topic, partition) 的 Reader 读取消息，分发到 worker 池处理。
// ctx 取消（Close 或父 ctx 结束）时退出。
func (c *KafkaConsumer) readerLoop(ctx context.Context, r *kafka.Reader, store ConsumerProgressStore, sem chan struct{}, handler MessageHandler) {
	defer c.wg.Done()
	cc := c.cfg.Consumer
	var fetchFails int // 连续 fetch 失败计数；成功拉取后归零
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			// ctx 取消 / reader 关闭属于正常退出
			if ctx.Err() != nil {
				return
			}
			// fetch 错误多为 broker 瞬时错误，退避重试可恢复。
			fetchFails++
			if fetchFails >= fetchErrEscalateAfter {
				c.log.Errorf("kafka consumer fetch error (consecutive=%d): %v", fetchFails, err)
			} else {
				c.log.Warnf("kafka consumer fetch error (consecutive=%d): %v", fetchFails, err)
			}
			recordConsumeFetchError(typeKafka, r.Config().Topic)
			base := orDuration(cc.RetryBaseDelay, time.Second)
			maxBackoff := orDuration(cc.RetryMaxDelay, 16*time.Second)
			backoff := base
			for i := 1; i < fetchFails && backoff < maxBackoff; i++ {
				backoff *= 2
			}
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
			continue
		}

		// 成功拉取到消息，重置连续失败计数（退避随之回到最小值）
		fetchFails = 0

		// 顺序提交模式：本分区串行「处理完再推进 offset」，消除「已处理未提交」崩溃丢消息窗口，
		// 并保证同分区严格有序（代价：单分区并发=1；跨分区仍并行）。
		if cc.OrderedCommit {
			c.handle(ctx, store, m, handler)
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// 退出前尽力处理这条已取到的消息
			c.handle(ctx, store, m, handler)
			return
		}

		c.wg.Add(1)
		go func(msg kafka.Message) {
			defer c.wg.Done()
			defer func() { <-sem }()
			c.handle(ctx, store, msg, handler)
		}(m)
	}
}

// handle 处理单条消息：先反序列化拿 event_id 做幂等去重，再调用 handler（含重试/DLQ），
// 最后推进 offset。无论成功、去重跳过还是落入 DLQ，都推进 offset（DLQ 已持久化，不会丢）。
// 无消费组时 offset 由 ConsumerProgressStore 管理，而非 group coordinator。
func (c *KafkaConsumer) handle(ctx context.Context, store ConsumerProgressStore, m kafka.Message, handler MessageHandler) {
	// 反序列化（先拿到 event_id 用于幂等去重）
	var msg event.Message
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		c.log.Warnf("kafka consumer unmarshal failed (topic=%s offset=%d): %v", m.Topic, m.Offset, err)
		c.appendDLQ(m, err)
		recordConsumed(typeKafka, m.Topic, resultDLQ) // 毒消息落 DLQ，计入 dlq 口径
		c.commitOffset(ctx, store, m, "")             // 毒消息也推进 offset，避免卡住分区
		return
	}

	// 幂等去重（消费侧 Outbox/Dedup）：跨分区/重启重投的同 event_id 直接跳过，offset 仍推进。
	// 这是「跨分区业务一致性」的关键——同 event_id 无论在哪个分区重投，都只生效一次，
	// 与 producer 侧 event_dedup / points_outbox 的「恰好一次」语义对称。
	if msg.EventID != "" {
		if seen, sErr := store.Seen(ctx, msg.EventID); sErr == nil && seen {
			c.commitOffset(ctx, store, m, msg.EventID)
			recordConsumed(typeKafka, m.Topic, resultDedupSkipped) // 重复 event_id 跳过，计入去重口径
			return
		}
	}

	dlqed := c.process(ctx, m, &msg, handler)
	c.commitOffset(ctx, store, m, msg.EventID)
	if dlqed {
		recordConsumed(typeKafka, m.Topic, resultDLQ)
	} else {
		recordConsumed(typeKafka, m.Topic, resultProcessed)
	}
}

// commitOffset 推进 (topic, partition) 的 offset。
//   - ordered_commit 模式：sync=true，每条消息同步落盘 offset，将崩溃重放窗口缩到最小；
//     （配合单分区串行，等价于「处理完即持久化提交」，是 per-partition 最强的可达语义）
//   - 并发模式：sync=false，依赖后台周期落盘（吞吐优先）。
//
// 两种模式均「只增不减」，避免乱序完成导致的 offset 回退重放。
func (c *KafkaConsumer) commitOffset(ctx context.Context, store ConsumerProgressStore, m kafka.Message, eventID string) {
	if err := store.Commit(ctx, m.Topic, m.Partition, m.Offset+1, eventID, c.cfg.Consumer.OrderedCommit); err != nil {
		c.log.Warnf("kafka consumer offset commit failed (topic=%s partition=%d offset=%d): %v",
			m.Topic, m.Partition, m.Offset+1, err)
	}
}

// process 调用 handler 并用指数退避重试；重试耗尽落 DLQ。msg 已由 handle 反序列化好。
// 返回是否落入 DLQ（供 handle 上报消费计数口径：dlq vs processed）。
func (c *KafkaConsumer) process(ctx context.Context, m kafka.Message, msg *event.Message, handler MessageHandler) bool {
	cc := c.cfg.Consumer
	delay := orDuration(cc.RetryBaseDelay, time.Second)
	maxDelay := orDuration(cc.RetryMaxDelay, 16*time.Second)
	var lastErr error
	for attempt := 0; attempt <= maxInt(cc.RetryMax, 0); attempt++ {
		if attempt > 0 {
			sleepWithContext(ctx, delay)
			if ctx.Err() != nil {
				lastErr = ctx.Err()
				goto dlq
			}
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
		err := callHandlerSafe(ctx, msg, handler)
		if err != nil {
			lastErr = err
			if errors.Is(err, errHandlerPanic) {
				goto dlq // handler panic 不可重试：直接落 DLQ，避免进程崩溃（见 13 §3.39）
			}
			continue
		}
		return false // 成功
	}

dlq:
	c.log.Warnf("kafka consumer handler exhausted retries (event=%s topic=%s): %v", msg.EventType, m.Topic, lastErr)
	c.appendDLQ(m, lastErr)
	return true
}

// appendDLQ 将消费失败的消息追加到本地 DLQ 文件（按 topic 分文件，JSONL）。
func (c *KafkaConsumer) appendDLQ(m kafka.Message, cause error) {
	cc := c.cfg.Consumer
	if cc.DLQLocalPath == "" {
		return
	}
	errStr := ""
	if cause != nil {
		errStr = cause.Error()
	}
	rec := consumerDLQRecord{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       string(m.Key),
		Value:     string(m.Value),
		Error:     errStr,
		TS:        time.Now(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		c.log.Errorf("kafka consumer dlq marshal failed: %v", err)
		return
	}

	suffix := cc.DLQSuffix
	if suffix == "" {
		suffix = ".dlq"
	}
	fp := filepath.Join(cc.DLQLocalPath, m.Topic+suffix+".jsonl")

	c.dlqMu.Lock()
	defer c.dlqMu.Unlock()
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		c.log.Errorf("kafka consumer dlq open failed: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		c.log.Errorf("kafka consumer dlq write failed: %v", err)
		return
	}
	// 落盘成功：存量 +1 并上报 gauge。消费者 DLQ 无自动 replay（存档语义），此值只增，
	// 人工清理文件后由重启扫描归零。仍在 dlqMu 锁内（与写文件同锁），并发安全。
	if c.dlqBacklog == nil {
		c.dlqBacklog = make(map[string]int64)
	}
	c.dlqBacklog[m.Topic]++
	setConsumerDLQBacklog(typeKafka, m.Topic, c.dlqBacklog[m.Topic])
}

// initDLQBacklog 扫描 DLQ 目录下已有的 "<topic><suffix>.jsonl" 文件，按 topic 统计行数，
// 初始化 dlqBacklog 与 mq_consumer_dlq_backlog gauge，使存量在进程重启后仍反映磁盘真实值
// （消费者 DLQ 无自动 replay，属存档语义；counter 会随进程重置，故用 gauge + 启动扫描补齐）。
func (c *KafkaConsumer) initDLQBacklog(dir, suffix string) {
	if suffix == "" {
		suffix = ".dlq"
	}
	fileSuffix := suffix + ".jsonl"
	files, err := filepath.Glob(filepath.Join(dir, "*"+fileSuffix))
	if err != nil {
		c.log.Warnf("kafka consumer dlq backlog glob failed: %v", err)
		return
	}
	c.dlqMu.Lock()
	defer c.dlqMu.Unlock()
	if c.dlqBacklog == nil {
		c.dlqBacklog = make(map[string]int64)
	}
	for _, fp := range files {
		base := filepath.Base(fp)
		topic := strings.TrimSuffix(base, fileSuffix)
		if topic == "" || topic == base { // 命名不符预期（无 topic 前缀），跳过
			continue
		}
		n, cErr := countJSONLLines(fp)
		if cErr != nil {
			c.log.Warnf("kafka consumer dlq backlog scan %q failed: %v", fp, cErr)
			continue
		}
		c.dlqBacklog[topic] = n
		setConsumerDLQBacklog(typeKafka, topic, n)
	}
}

// countJSONLLines 统计文件中非空行数（DLQ JSONL 每行一条记录）。缓冲上限复用 maxDLQLineBytes，
// 兼容较大的消息行，避免超长行导致扫描失败误计。
func countJSONLLines(fp string) (int64, error) {
	f, err := os.Open(fp)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxDLQLineBytes)
	var n int64
	for scanner.Scan() {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		n++
	}
	return n, scanner.Err()
}

// closeReaders 关闭所有分区 Reader，单个 Close 带超时兜底，避免 broker 不可达时
// 阻塞关闭流程。
func (c *KafkaConsumer) closeReaders(readers []*kafka.Reader) {
	closeTimeout := orDuration(c.cfg.Consumer.CloseTimeout, 10*time.Second)
	var wg sync.WaitGroup
	for _, r := range readers {
		wg.Add(1)
		go func(rd *kafka.Reader) {
			defer wg.Done()
			errc := make(chan error, 1)
			go func() { errc <- rd.Close() }()
			select {
			case <-errc:
			case <-time.After(closeTimeout):
				c.log.Warnf("kafka consumer reader close timed out after %s", closeTimeout)
			}
		}(r)
	}
	wg.Wait()
}

// scrapeConsumerLag 读取各 (topic, partition) reader 的 Stats().Lag / Offset 并上报消费 lag /
// offset 指标（见 13 §6.5 / §3.59）。抽出为独立函数便于单测——kafka.Reader.Stats() 是无连接读取
// 内部统计，不依赖真实 broker。调用于 Subscribe 的后台抓取协程（周期性）与可观测性测试。
func scrapeConsumerLag(metas []readerMeta) {
	for _, m := range metas {
		st := m.r.Stats()
		// kafka-go 在刚 SetOffset / 分区重分配等瞬时窗口会报负 lag，而 lag 在语义与告警上
		// 应为非负（落后消息数）。钳到 0，避免负 lag 误导看板与导致告警误判。
		setConsumerLag(typeKafka, m.topic, m.partition, clampLag(st.Lag))
		setConsumerOffset(typeKafka, m.topic, m.partition, st.Offset)
	}
}

// clampLag 将瞬时窗口可能产生的负 lag 钳到 0（lag 在语义与告警上应为非负）。抽出为纯函数便于单测。
func clampLag(lag int64) int64 {
	if lag < 0 {
		return 0
	}
	return lag
}

// Close 关闭消费者：通知 Subscribe 退出（reader 在 Subscribe 返回时关闭）。
func (c *KafkaConsumer) Close() error {
	c.closeOnce.Do(func() { close(c.closeCh) })
	return nil
}

// orInt 返回 d 非零值，否则返回默认值 def。用于配置缺省。
func orInt(d, def int) int {
	if d == 0 {
		return def
	}
	return d
}

// orStr 返回 s 非空值，否则返回默认值 def。用于配置缺省。
func orStr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// assignPartitions 无消费组模式下的多实例分区分配（替代消费组再均衡，见 13 §6.5 #1 / §3.53）。
// InstanceCount<=1 时不裁剪，返回全量分区（向后兼容单/双实例部署）。
// InstanceCount>1 时仅保留 partition % InstanceCount == InstanceID 的分区，使 N 个实例各持
// 互不重叠的分区子集，消除「无消费组多实例重复消费」。
// 防御兜底（越界配置已由 config.Validate 在启动/热更新期拦截，见 13 §6.5 #1 本轮补充）：
// InstanceID 越界（<0 或 >=InstanceCount）时退化为全量消费并告警，避免「取模恒不成立 → 订阅 0 分区 → 静默不消费」。
func (c *KafkaConsumer) assignPartitions(parts []int) []int {
	ic, id := c.cfg.Consumer.InstanceCount, c.cfg.Consumer.InstanceID
	if ic <= 1 {
		return parts
	}
	if id < 0 || id >= ic {
		c.log.Warnf("kafka consumer instance_id=%d out of range [0,%d), falling back to consuming ALL partitions (check mq.kafka.consumer.instance_id/instance_count)",
			id, ic)
		return parts
	}
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if p%ic == id {
			out = append(out, p)
		}
	}
	c.log.Infof("kafka consumer partition assignment: instance %d/%d consumes %d/%d partitions",
		id, ic, len(out), len(parts))
	return out
}

// instanceDesc 返回订阅日志中的实例描述后缀（单实例为空）。
func (c *KafkaConsumer) instanceDesc() string {
	ic, id := c.cfg.Consumer.InstanceCount, c.cfg.Consumer.InstanceID
	if ic <= 1 {
		return ""
	}
	return fmt.Sprintf(" instance=%d/%d", id, ic)
}

// instanceOffsetPath 多实例下把 offset 文件路径按实例区分（见 13 §6.5 #2 根因）。
// 在文件名（扩展名之前）追加 .<InstanceID>，例如 ./data/kafka-offsets.json -> ./data/kafka-offsets.2.json，
// 避免多个实例写同一文件互相覆盖导致 offset 损坏。单实例（InstanceCount<=1）原样返回。
func (c *KafkaConsumer) instanceOffsetPath(path string) string {
	ic, id := c.cfg.Consumer.InstanceCount, c.cfg.Consumer.InstanceID
	if ic <= 1 {
		return path
	}
	if id < 0 || id >= ic {
		return path // 越界时沿用原路径（config.Validate 已拦截越界配置，此处仅作兜底，与 assignPartitions 保持一致）
	}
	dir, file := filepath.Split(path)
	ext := filepath.Ext(file)
	base := file[:len(file)-len(ext)]
	return filepath.Join(dir, fmt.Sprintf("%s.%d%s", base, id, ext))
}

// warnStartFromLatestIfOffsetExists 反直觉告警（见 13 §6.5 #6）：`start_from_latest=true`
// 仅当本地【无 offset 文件】时生效——首次启动从最新位置消费、跳过积压；一旦 offset 文件
// 已存在，`SetOffset` 会优先从已处理位置续读，此时改 `start_from_latest: true` **并不能**
// 「跳过积压」。若确需跳过积压，须先删除对应 offset 文件（或重置）再重启。
// 仅文件存储模式适用（注入 SQLProgressStore 时无本地 offset 文件，语义不同，不在此告警）。
// 注意：offsetPath 须已应用 instanceOffsetPath（多实例按实例区分文件名），以免误判其它实例的文件。
func (c *KafkaConsumer) warnStartFromLatestIfOffsetExists(offsetPath string) {
	if !c.cfg.Consumer.StartFromLatest {
		return
	}
	if _, err := os.Stat(offsetPath); err == nil {
		c.log.Warnf("kafka: start_from_latest=true has NO effect: offset file %q already exists, resume from processed offset (not skipping backlog). To skip backlog, delete the offset file and restart", offsetPath)
	}
}
