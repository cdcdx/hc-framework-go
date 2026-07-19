package common

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/mq"
	"github.com/cdcdx/hc-framework-go/internal/repository"
)

// 默认积分异步投递 worker 池并发度；可由 NewPointsOutboxApplier 的 maxConcurrency 覆盖。
// 该上限同时是「并发占用的 userDB 连接数」上限，须明显小于 userDB 连接池容量，
// 为 Status / 查询等读路径留出余量（§3.48 第3项）。
const (
	defaultOutboxAsyncConcurrency = 64
	// 单条积分写（Claim + UpdatePoints + MarkDone）的最长耗时，超时即放弃本 worker（交 relay 兜底）。
	outboxWriteTimeout = 5 * time.Second
)

// PointsOutboxApplier 积分调整的可靠投递器（Transactional Outbox 实现）。
//
// 竞争模型：有界异步快路径（ApplyAsync）+ Kafka 消费者（ApplyPointsAdjust）+ 后台 relay（RelayLoop）
// 三者均通过 Claim(event_id) 原子争用，保证对 userDB 余额「恰好一次」生效：
//   - businessDB 事务提交时写入 pending outbox 记录（与库存/订单/流水同提交）；
//   - 任一经 Claim 成功者负责调用 userRepo.UpdatePoints，成功后 MarkDone；
//   - 失败则 Requeue 回 pending，由 relay / 消费者重试，从而保证最终一致。
//
// 因此 Redeem / settleSession / Claim 在 businessDB 提交后不再直接写 userDB，也不因 userDB
// 暂时不可用而失败，根除「库存已扣、积分未加」的跨库不一致。
type PointsOutboxApplier struct {
	outboxRepo *repository.PointsOutboxRepository
	userRepo   repository.UserRepository
	publisher  mq.Producer
	// sem 限制并发 userDB 写（即 tryApply 中的 UpdatePoints）的 goroutine 数，
	// 防止积分投递在高峰把 userDB 连接池占满、饿死读路径（Status 等）。§3.48 第3项。
	sem chan struct{}
}

func NewPointsOutboxApplier(outboxRepo *repository.PointsOutboxRepository, userRepo repository.UserRepository, publisher mq.Producer, maxConcurrency int) *PointsOutboxApplier {
	if maxConcurrency <= 0 {
		maxConcurrency = defaultOutboxAsyncConcurrency
	}
	return &PointsOutboxApplier{
		outboxRepo: outboxRepo,
		userRepo:   userRepo,
		publisher:  publisher,
		sem:        make(chan struct{}, maxConcurrency),
	}
}

// AppendOutboxInTx 在业务事务 tx 内追加一条 outbox 记录（与库存/订单/积分流水同提交）。
// tx 已携带调用方 ctx（来自 businessDB.Transaction），直接使用可保留链路追踪与超时信息。
func (a *PointsOutboxApplier) AppendOutboxInTx(tx *gorm.DB, rec *model.PointsOutbox) error {
	return tx.Create(rec).Error
}

// ApplyAsync 业务事务提交后调用。
//
// 原本在请求关键路径内「同步」执行 tryApply（Claim + UpdatePoints + MarkDone），每个
// settle/redeem/claim 请求都会串行占用一条 userDB 连接，是 §3.48 压测 p99 长尾的贡献因素之一。
// 现改为「有界 goroutine 异步」：把写余额的工作派发到 sem 限流的 worker 池，请求在入队后
// 立即返回，不再持有 userDB 连接。outbox 记录已在 businessDB 事务内持久化，即便本批异步
// worker 尚未执行，也会由 Kafka 消费者 + 后台 relay 兜底应用，保证最终一致（§3.48 第3项）。
//
// 关键：worker 内使用 context.Background() 派生的「带超时」ctx，而非请求 ctx——请求可能在
// 写入完成前已返回并取消其 ctx，沿用会导致 UpdatePoints 中途被取消（连接被异常归还、余额未更新）。
func (a *PointsOutboxApplier) ApplyAsync(ctx context.Context, rec *model.PointsOutbox) {
	if rec == nil {
		return
	}
	// 池有空闲：派发异步 worker 立即尝试同步快路径（即时一致）。
	select {
	case a.sem <- struct{}{}:
		go func() {
			defer func() { <-a.sem }()
			wctx, cancel := context.WithTimeout(context.Background(), outboxWriteTimeout)
			defer cancel()
			if a.tryApply(wctx, rec.EventID, rec.UserID, rec.Delta) {
				return
			}
			a.publish(wctx, rec)
		}()
	default:
		// 池满：绝不阻塞请求。outbox 已持久化，直接发布 Kafka 事件由消费者异步应用；
		// 即便 MQ 禁用/不可用，relay 也会在下一周期补应用，最终一致不受影响。
		a.publish(ctx, rec)
	}
}

// tryApply 争用并应用一次积分调整。返回是否成功生效（含已被其他竞争者处理）。
func (a *PointsOutboxApplier) tryApply(ctx context.Context, eventID, userID string, delta int64) bool {
	claimed, err := a.outboxRepo.Claim(ctx, eventID)
	if err != nil || !claimed {
		return false
	}
	if err := a.userRepo.UpdatePoints(ctx, userID, delta); err != nil {
		// 余额更新失败：释放回 pending，交由 relay / 消费者重试，保证最终一致。
		_ = a.outboxRepo.Requeue(ctx, eventID)
		return false
	}
	_ = a.outboxRepo.MarkDone(ctx, eventID)
	return true
}

func (a *PointsOutboxApplier) publish(ctx context.Context, rec *model.PointsOutbox) {
	if a.publisher == nil {
		return
	}
	_ = a.publisher.Send(ctx, &event.Message{
		EventType: event.EventUserPointsAdjust,
		Key:       rec.UserID,
		EventID:   rec.EventID,
		Timestamp: time.Now(),
		Payload: event.UserPointsAdjustPayload{
			UserID:  rec.UserID,
			EventID: rec.EventID,
			Delta:   rec.Delta,
			RefType: rec.RefType,
			RefID:   rec.RefID,
		},
	})
}

// ApplyPointsAdjust Kafka 消费者处理函数：幂等应用积分调整（event_id 保证恰好一次）。
func (a *PointsOutboxApplier) ApplyPointsAdjust(ctx context.Context, msg *event.Message) error {
	var p event.UserPointsAdjustPayload
	if err := DecodePayload(msg.Payload, &p); err != nil {
		return err
	}
	a.tryApply(ctx, p.EventID, p.UserID, p.Delta)
	return nil // 幂等：无论本次是否生效都不重试消息（outbox 由 relay 兜底）
}

// RelayLoop 后台补偿：周期性扫描 pending 超时记录并重试，覆盖「无 Kafka」「发布前崩溃」
// 「同步路径 userDB 暂不可用」等场景，是最终一致性的兜底保障。
func (a *PointsOutboxApplier) RelayLoop(ctx context.Context, interval, grace time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if grace <= 0 {
		grace = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.relayOnce(ctx, grace)
		}
	}
}

func (a *PointsOutboxApplier) relayOnce(ctx context.Context, grace time.Duration) {
	rows, err := a.outboxRepo.PendingOlderThan(ctx, time.Now().Add(-grace), 200)
	if err != nil {
		return
	}
	for i := range rows {
		a.tryApply(ctx, rows[i].EventID, rows[i].UserID, rows[i].Delta)
	}
}
