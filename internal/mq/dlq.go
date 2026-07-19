package mq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/event"
)

// dlqRecord 本地 DLQ 落盘记录（保留原始消息与失败原因，便于 replay 与排查）。
// Kafka / RocketMQ / RabbitMQ 生产者共用同一结构，保证 DLQ 文件格式跨实现一致、可独立 replay。
type dlqRecord struct {
	Message *event.Message `json:"message"`
	Error   string         `json:"error"`
	Ts      time.Time      `json:"ts"`
}

// relayFunc 将一条 DLQ 消息补发到 broker；返回 nil 视为补发成功、可移除该行，
// 返回 error 表示该条仍失败（保留在 DLQ 中待下次重试）。
type relayFunc func(ctx context.Context, msg *event.Message) error

// DLQ 健壮性默认参数（防磁盘无限增长 / OOM / 单次 replay 长时间占用）。
const (
	// defaultDLQMaxBytes 单个 topic DLQ 文件的字节上限：broker 长期不可达时避免落盘无限增长撑爆磁盘。
	// 超限则丢弃新消息（记 dropped 计数），保留较早消息（更可能仍有补发价值）。默认 256MB。
	defaultDLQMaxBytes int64 = 256 << 20
	// defaultReplayBatchMax 单次 replay 每个 DLQ 文件最多补发的条数：避免一次 tick 补发海量消息
	// 长时间占用 replay 协程 / 猛打 broker；未处理的行保留到下一轮。默认 1000。
	defaultReplayBatchMax = 1000
	// maxDLQLineBytes bufio.Scanner 单行缓冲上限（防超大消息行导致扫描失败）。默认 8MB。
	maxDLQLineBytes = 8 << 20
	// dlqReplayingSuffix replay 时的快照文件后缀：先原子 rename 主文件到该快照再慢慢补发，
	// 使并发 append 立即在新主文件继续追加、不被 relay（等 broker）阻塞，也避免读改写竞争丢消息。
	dlqReplayingSuffix = ".replaying"
	// dlqCorruptSuffix 扫描损坏（如超长行）时的保留后缀，供人工排查，避免直接丢弃。
	dlqCorruptSuffix = ".corrupt"
)

// dlqStore 共享的本地文件 DLQ（按 topic 分文件、JSONL）。Kafka / RocketMQ / RabbitMQ 生产者复用，
// 统一「broker 不可达时落盘、恢复后由 replay 协程补发」的降级语义，避免各适配器重复实现。
//
// 语义：落 DLQ 不等于「丢弃」——它会被 replay 协程在 broker 恢复后自动补发；真正会丢消息的
// 是内存缓冲满（memory）、总线禁用（nop）、以及 DLQ 文件达到大小上限后无法再落盘的消息，
// 均由 recordDropped 另行计数。
//
// 并发模型：append（发送协程，多并发）与 replay（replayLoop 单协程）可能同时访问同一 topic 文件。
// 二者对主文件的所有读写均以 mu 串行化；replay 采用「rename 快照 + 流式补发 + 失败行写回」以尽量
// 缩短持锁时间（rename 是原子、瞬时操作，真正耗时的 relay 在快照文件上进行、不持锁、不阻塞 append）。
type dlqStore struct {
	enabled        bool
	path           string
	mqType         string // 指标标签：kafka|rabbitmq|rocketmq（用于超限丢弃计数）
	maxBytes       int64  // 单文件字节上限（<=0 关闭该保护）
	replayBatchMax int    // 单次 replay 每文件最多补发条数（<=0 关闭批量上限）
	mu             sync.Mutex
	backlog        atomic.Int64 // 当前 DLQ 待补发的积压消息数（append +1 / replay 成功 -1），供 SetDLQBacklog gauge。
	log            Logger
}

// newDLQStore 创建本地 DLQ 存储。enabled && path 非空时确保目录存在（幂等）。
// mqType 用于超限丢弃时的指标标签（kafka|rabbitmq|rocketmq）。
func newDLQStore(mqType string, enabled bool, path string, log Logger) *dlqStore {
	if log == nil {
		log = nopLogger{}
	}
	if enabled && path != "" {
		if err := os.MkdirAll(path, 0o755); err != nil {
			log.Warnf("dlq mkdir %q failed: %v", path, err)
		}
	}
	return &dlqStore{
		enabled:        enabled,
		path:           path,
		mqType:         mqType,
		maxBytes:       defaultDLQMaxBytes,
		replayBatchMax: defaultReplayBatchMax,
		log:            log,
	}
}

// pathFor 返回某主题的 DLQ 文件路径。
func (d *dlqStore) pathFor(topic string) string {
	return filepath.Join(d.path, topic+".dlq.jsonl")
}

// append 将失败消息追加到本地 DLQ 文件（按 topic 分文件，JSONL）。未启用或路径为空时安全跳过。
// 文件达到 maxBytes 上限时丢弃本条（记 dropped 计数并告警），避免 broker 长期不可达时撑爆磁盘。
func (d *dlqStore) append(topic string, msg *event.Message, sendErr error) {
	if !d.enabled || d.path == "" {
		return
	}
	rec := dlqRecord{Message: msg, Error: sendErr.Error(), Ts: time.Now()}
	raw, err := json.Marshal(rec)
	if err != nil {
		d.log.Errorf("dlq marshal failed: %v", err)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	fp := d.pathFor(topic)
	// 单文件大小上限保护：预估追加后是否超限（含换行符），超限则丢弃本条并计数。
	if d.maxBytes > 0 {
		if fi, statErr := os.Stat(fp); statErr == nil && fi.Size()+int64(len(raw))+1 > d.maxBytes {
			recordDropped(d.mqType, topic)
			d.log.Warnf("dlq file %q would exceed max %d bytes; dropping message (event=%s) — broker down too long?", fp, d.maxBytes, msg.EventType)
			return
		}
	}
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		d.log.Errorf("dlq open failed: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		d.log.Errorf("dlq write failed: %v", err)
		return
	}
	// 落盘成功：积压 +1 并上报，供 mq_dlq_backlog gauge 反映「待补发消息数」。
	n := d.backlog.Add(1)
	recordDLQBacklog(d.mqType, topic, n)
}

// replayAll 重放所有 DLQ 文件：先恢复上次进程崩溃遗留的快照，再逐文件补发。
// relay 为实际补发函数（含熔断等保护），返回 nil 视为成功。
func (d *dlqStore) replayAll(relay relayFunc) {
	if !d.enabled || d.path == "" {
		return
	}
	d.recoverLeftovers()
	files, err := filepath.Glob(filepath.Join(d.path, "*.dlq.jsonl"))
	if err != nil || len(files) == 0 {
		return
	}
	for _, fp := range files {
		// 由文件名反解 topic（pathFor = "<topic>.dlq.jsonl"），供 backlog gauge 按 topic 维度上报。
		topic := strings.TrimSuffix(filepath.Base(fp), ".dlq.jsonl")
		d.replayFile(fp, topic, relay)
	}
}

// recoverLeftovers 把上次进程异常退出遗留的 .replaying 快照内容并回对应主文件，避免消息滞留不补发。
func (d *dlqStore) recoverLeftovers() {
	leftovers, err := filepath.Glob(filepath.Join(d.path, "*.dlq.jsonl"+dlqReplayingSuffix))
	if err != nil {
		return
	}
	for _, tmp := range leftovers {
		fp := strings.TrimSuffix(tmp, dlqReplayingSuffix)
		data, err := os.ReadFile(tmp)
		if err != nil {
			continue
		}
		d.mu.Lock()
		f, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = f.Write(data)
			_ = f.Close()
		}
		d.mu.Unlock()
		_ = os.Remove(tmp)
		d.log.Warnf("dlq recovered leftover snapshot %s back to %s (previous crash?)", tmp, fp)
	}
}

// replayFile 重放单个 DLQ 文件：原子 rename 到快照后逐行补发，成功则移除；失败 / 超批的行写回主文件。
// rename 在持锁下瞬时完成，随后耗时的 relay 在快照文件上进行、不持锁，故不阻塞并发 append。
func (d *dlqStore) replayFile(fp, topic string, relay relayFunc) {
	tmp := fp + dlqReplayingSuffix
	d.mu.Lock()
	if _, err := os.Stat(fp); err != nil {
		d.mu.Unlock()
		return // 文件已不存在
	}
	if err := os.Rename(fp, tmp); err != nil {
		d.mu.Unlock()
		d.log.Errorf("dlq rename %s failed: %v", fp, err)
		return
	}
	d.mu.Unlock()
	d.drainSnapshot(tmp, fp, topic, relay)
}

// drainSnapshot 流式补发快照文件 tmp：逐行 relay，成功即丢弃；失败 / 超批 / 损坏行收集后写回主文件 fp。
func (d *dlqStore) drainSnapshot(tmp, fp, topic string, relay relayFunc) {
	f, err := os.Open(tmp)
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxDLQLineBytes)

	var failed [][]byte
	recovered := 0
	stop := false // 一旦 broker 不可用 / 达到批量上限，后续行全部保留，不再尝试（避免无效重放刷屏）
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		cp := append([]byte(nil), line...) // scanner 复用底层 buffer，须复制后留存
		if stop || (d.replayBatchMax > 0 && recovered >= d.replayBatchMax) {
			failed = append(failed, cp)
			continue
		}
		var rec dlqRecord
		if err := json.Unmarshal(cp, &rec); err != nil || rec.Message == nil {
			failed = append(failed, cp) // 损坏行保留，不丢失
			continue
		}
		if err := relay(context.Background(), rec.Message); err != nil {
			failed = append(failed, cp)
			stop = true // broker 仍不可用：本轮后续行全部保留
			continue
		}
		recovered++
		// 补发成功：积压 -1 并上报，gauge 回落反映「待补发消息数」减少。
		n := d.backlog.Add(-1)
		recordDLQBacklog(d.mqType, topic, n)
	}
	scanErr := scanner.Err()
	_ = f.Close()

	if scanErr != nil {
		// 扫描损坏（如超长行）：保留整个快照为 .corrupt 供排查，避免丢失；已读到的失败行也写回主文件。
		d.log.Errorf("dlq scan %s failed: %v; keeping snapshot as %s", tmp, scanErr, fp+dlqCorruptSuffix)
		if len(failed) > 0 {
			d.writeBack(fp, failed)
		}
		_ = os.Rename(tmp, fp+dlqCorruptSuffix)
		return
	}

	if len(failed) > 0 {
		d.writeBack(fp, failed)
	}
	_ = os.Remove(tmp)
	if recovered > 0 {
		d.log.Infof("dlq replayed %d messages from %s", recovered, fp)
	}
}

// writeBack 将未成功补发的行追加回主文件（持锁，与并发 append 合并；DLQ 为尽力补发，行序不严格）。
func (d *dlqStore) writeBack(fp string, lines [][]byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		d.log.Errorf("dlq writeback open failed: %v", err)
		return
	}
	defer f.Close()
	for _, l := range lines {
		if _, err := f.Write(append(l, '\n')); err != nil {
			d.log.Errorf("dlq writeback write failed: %v", err)
			return
		}
	}
}
