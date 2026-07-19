package mq

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ConsumerProgressStore 消费进度（offset）与幂等去重的抽象存储。
//
// 为什么抽象：无消费组模式下 offset 不再由 Kafka coordinator 管理，而是「消费侧自管」。
// 默认的 FileProgressStore 把 (topic, partition) → 下一条待消费 offset 写到本地 JSON 文件，
// 重启续读；但它与业务库不共享事务，且依赖周期落盘（ordered_commit 模式改为每条同步落盘）。
// 对「消费侧业务一致性」要求更高的场景，可注入 SQLProgressStore（与业务库同实例），使 offset 与
// 业务写入可落在同一 DB 事务（由 handler 通过事务提交），并获得 event_id 跨分区幂等去重——
// 这正是「消费侧 Outbox / Dedup」模式，等价于 producer 侧 points_outbox 在消费端的镜像。
//
// 注意：Kafka 不保证跨分区顺序/原子提交，本抽象也无法改变这一点。它提供的是
//   - 分区内严格、可持久化的 offset 推进（ordered_commit 下每条同步落盘）；
//   - event_id 维度的幂等去重（同 event_id 跨分区/重启重投只生效一次）。
//
// 真正的「跨分区原子提交」在 Kafka 模型下不可达，需把相关事件路由到同一分区（相同 key）再配合本机制。
type ConsumerProgressStore interface {
	// Get 返回 (topic, partition) 的「下一条待消费 offset」；未记录过返回 (0, false, nil)。
	Get(ctx context.Context, topic string, partition int) (int64, bool, error)
	// Commit 记录 (topic, partition) 已处理到的 offset（= 下一条待消费位置）。仅增不减，避免乱序回退。
	// eventID 非空时一并写入幂等去重（消费侧 Dedup）。sync=true 要求落盘后再返回
	// （ordered_commit 模式用，缩小崩溃重放窗口）；false 可由实现异步/周期落盘（并发模式用）。
	Commit(ctx context.Context, topic string, partition int, offset int64, eventID string, sync bool) error
	// Seen 返回 eventID 是否已处理（幂等去重查询）。eventID 为空时返回 false。
	Seen(ctx context.Context, eventID string) (bool, error)
	// FlushInterval 返回需后台周期落盘的间隔；>0 表示实现依赖周期落盘（FileProgressStore=5s），
	// 0 表示自身已持久化（SQLProgressStore 每条 Commit 即落库）。Subscribe 据此决定是否起落盘协程。
	FlushInterval() time.Duration
	// Flush 立即落盘（周期落盘实现用；SQL 实现为 no-op）。
	Flush() error
	// Close 释放资源（如有后台落盘则做最后一次 Flush）。
	Close() error
}

// ─────────────────────────────────────────────────────────────────────────────
// FileProgressStore：本地 JSON 文件实现（默认），兼容旧 offsetStore 行为。
// ─────────────────────────────────────────────────────────────────────────────

// offsetStore 将每个 (topic, partition) 的「下一条待消费 offset」持久化到本地 JSON 文件，
// 使无消费组的消费者在重启后能从已处理位置续读。
type offsetStore struct {
	mu            sync.Mutex
	m             map[string]int64
	path          string
	flushInterval time.Duration // 0 表示使用默认 5s
}

func newOffsetStore(path string, flushInterval time.Duration) *offsetStore {
	s := &offsetStore{m: map[string]int64{}, path: path, flushInterval: flushInterval}
	s.load()
	return s
}

// newFileProgressStore 别名：构造默认的文件进度存储（向后兼容旧 newOffsetStore 语义）。
// flushInterval<=0 时使用默认 5s 落盘间隔（见 13 §6.5 #3 / §3.58）。
func newFileProgressStore(path string, flushInterval time.Duration) *offsetStore {
	return newOffsetStore(path, flushInterval)
}

func offsetKey(topic string, part int) string { return topic + "/" + strconv.Itoa(part) }

func (s *offsetStore) load() {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return // 首次运行无文件，从 StartOffset 开始
	}
	_ = json.Unmarshal(data, &s.m)
}

func (s *offsetStore) get(topic string, part int) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[offsetKey(topic, part)]
	return v, ok
}

// set 记录 (topic, partition) 的「下一条待消费 offset」。
// 采用「只增不减」语义：worker 池并发处理同一分区的多条消息时，完成顺序可能与 offset 顺序
// 不一致，若直接覆盖会出现较大 offset 先提交、较小 offset 后提交把进度「回退」，导致重启后
// 该区间消息被重复消费。只增不减可消除这种回退，保证 offset 单调推进。
//
// 已知局限（与「无消费组 + 并发处理」设计一致）：进程在「消息已处理完、尚未 set」的极小窗口
// 崩溃时，仍可能丢失该消息（offset 已推进到更大值）。彻底消除需同分区顺序提交（牺牲并发），
// 本项目事件处理多为幂等，且 handler 失败已落 DLQ，故默认采用只增不减这一低风险修复；
// 对不丢消息要求更高的场景可开启 mq.kafka.consumer.ordered_commit=true（单分区串行提交，
// 且每条消息同步落盘 offset，将崩溃重放窗口缩到最小）。
func (s *offsetStore) set(topic string, part int, off int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := offsetKey(topic, part)
	if old, ok := s.m[k]; !ok || off > old {
		s.m[k] = off
	}
}

// flush 原子写盘（先写临时文件再 rename），避免进程崩溃损坏文件。
func (s *offsetStore) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(s.m)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// ── ConsumerProgressStore 接口实现 ──

func (s *offsetStore) Get(_ context.Context, topic string, partition int) (int64, bool, error) {
	v, ok := s.get(topic, partition)
	return v, ok, nil
}

func (s *offsetStore) Commit(_ context.Context, topic string, partition int, offset int64, _ string, sync bool) error {
	s.set(topic, partition, offset)
	if sync {
		return s.flush()
	}
	return nil
}

// Seen 文件存储不做跨分区幂等去重（无全局 event_id 索引）；去重由 SQLProgressStore 提供。
func (s *offsetStore) Seen(_ context.Context, _ string) (bool, error) { return false, nil }

func (s *offsetStore) FlushInterval() time.Duration {
	if s.flushInterval > 0 {
		return s.flushInterval
	}
	return 5 * time.Second
}

func (s *offsetStore) Flush() error { return s.flush() }

func (s *offsetStore) Close() error { return s.flush() }

// ─────────────────────────────────────────────────────────────────────────────
// SQLProgressStore：gorm 实现（消费侧 Outbox / Dedup），与业务库同实例时获得强一致。
// ─────────────────────────────────────────────────────────────────────────────

// consumerProgressRow 消费进度表：每个 (topic, partition) 一行，记录下一条待消费 offset。
type consumerProgressRow struct {
	Topic     string    `gorm:"primaryKey;column:topic;type:varchar(191)"`
	Partition int       `gorm:"primaryKey;column:partition"`
	Offset    int64     `gorm:"column:offset;not null;default:0"`
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (consumerProgressRow) TableName() string { return "kafka_consumer_progress" }

// consumerDedupRow 消费侧幂等去重表：event_id 唯一，重复消息（跨分区/重启重投）只生效一次。
// 镜像 producer 侧 event_dedup / points_outbox 的「恰好一次」语义。
type consumerDedupRow struct {
	ID        int64     `gorm:"primaryKey;autoIncrement"`
	EventID   string    `gorm:"column:event_id;uniqueIndex;type:varchar(128);not null"`
	Topic     string    `gorm:"column:topic;type:varchar(191);index"`
	Partition int       `gorm:"column:partition"`
	Offset    int64     `gorm:"column:offset;not null;default:0"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (consumerDedupRow) TableName() string { return "kafka_consumer_dedup" }

// SQLProgressStore 基于 gorm 的消费进度/去重存储。建议传入与业务写同一实例的 *gorm.DB，
// 这样 handler 可在同一 DB 事务内提交业务数据并（经本 store）推进 offset，达到消费侧 exactly-once。
type SQLProgressStore struct {
	db *gorm.DB
}

// NewSQLProgressStore 构造 SQL 进度存储并自动建表（幂等）。
func NewSQLProgressStore(db *gorm.DB) (*SQLProgressStore, error) {
	if err := db.AutoMigrate(&consumerProgressRow{}, &consumerDedupRow{}); err != nil {
		return nil, err
	}
	return &SQLProgressStore{db: db}, nil
}

func (s *SQLProgressStore) Get(_ context.Context, topic string, partition int) (int64, bool, error) {
	var row consumerProgressRow
	err := s.db.Where("topic = ? AND partition = ?", topic, partition).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return row.Offset, true, nil
}

func (s *SQLProgressStore) Commit(ctx context.Context, topic string, partition int, offset int64, eventID string, _ bool) error {
	// 单事务内完成「offset 只增不减 upsert」+「event_id 去重插入」，保证两者原子。
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row consumerProgressRow
		err := tx.Where("topic = ? AND partition = ?", topic, partition).First(&row).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if errors.Is(err, gorm.ErrRecordNotFound) || offset > row.Offset {
			if err := tx.Save(&consumerProgressRow{Topic: topic, Partition: partition, Offset: offset}).Error; err != nil {
				return err
			}
		}
		if eventID != "" {
			// 已存在（唯一冲突）则忽略——这正是「恰好一次」去重语义。
			d := consumerDedupRow{EventID: eventID, Topic: topic, Partition: partition, Offset: offset}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&d).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLProgressStore) Seen(_ context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	var cnt int64
	if err := s.db.Model(&consumerDedupRow{}).Where("event_id = ?", eventID).Count(&cnt).Error; err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func (s *SQLProgressStore) FlushInterval() time.Duration { return 0 } // 每条 Commit 即落库
func (s *SQLProgressStore) Flush() error                 { return nil }
func (s *SQLProgressStore) Close() error                 { return nil }
