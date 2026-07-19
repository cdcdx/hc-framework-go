package common

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/mq"
	"github.com/cdcdx/hc-framework-go/internal/repository"
)

// noopUserRepo 实现 repository.UserRepository 的空操作桩，供测试嵌入复用。
type noopUserRepo struct{}

func (noopUserRepo) Create(ctx context.Context, u *model.User) error { return nil }
func (noopUserRepo) FindByID(ctx context.Context, id string) (*model.User, error) {
	return nil, nil
}
func (noopUserRepo) FindByEmail(ctx context.Context, e string) (*model.User, error) {
	return nil, nil
}
func (noopUserRepo) FindByGoogleID(ctx context.Context, g string) (*model.User, error) {
	return nil, nil
}
func (noopUserRepo) Update(ctx context.Context, u *model.User) error { return nil }
func (noopUserRepo) UpdatePoints(ctx context.Context, userID string, amount int64) error {
	return nil
}
func (noopUserRepo) UpdatePassword(ctx context.Context, userID, hash string, changedAt interface{}) error {
	return nil
}
func (noopUserRepo) AutoMigrate() error      { return nil }
func (noopUserRepo) Close() error            { return nil }
func (noopUserRepo) SQLDB() (*sql.DB, error) { return nil, nil }

// countingUserRepo 统计 UpdatePoints 调用次数，其余方法委托 noopUserRepo。
type countingUserRepo struct {
	noopUserRepo
	mu sync.Mutex
	n  int
}

func (c *countingUserRepo) UpdatePoints(ctx context.Context, userID string, amount int64) error {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return nil
}

// count 安全读取当前计数（线程安全，消除 DATA RACE）。
func (c *countingUserRepo) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// failingUserRepo 模拟 userDB 暂不可用（UpdatePoints 始终失败）。
type failingUserRepo struct {
	noopUserRepo
}

func (failingUserRepo) UpdatePoints(ctx context.Context, userID string, amount int64) error {
	return context.DeadlineExceeded
}

func newOutboxTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := gdb.AutoMigrate(&model.PointsOutbox{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return gdb
}

func TestPointsOutboxClaimExactlyOnce(t *testing.T) {
	gdb := newOutboxTestDB(t)
	repo := repository.NewPointsOutboxRepository(gdb)
	ur := &countingUserRepo{}
	a := NewPointsOutboxApplier(repo, ur, nil, 0)

	// 事务内写入 pending 记录
	tx := gdb.Begin()
	if err := a.AppendOutboxInTx(tx, &model.PointsOutbox{
		UserID: "u1", Delta: 10, RefType: "idle", RefID: "1",
		EventID: "pts:idle:1", Status: model.OutboxStatusPending,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit: %v", err)
	}

	// 同步快路径应用一次
	if !a.tryApply(context.Background(), "pts:idle:1", "u1", 10) {
		t.Fatal("expected first apply to succeed")
	}
	// 重复 apply（模拟消费者 / relay 重投）必须幂等，不得二次入账
	if a.tryApply(context.Background(), "pts:idle:1", "u1", 10) {
		t.Fatal("second apply must be idempotent (no-op)")
	}
	if ur.count() != 1 {
		t.Fatalf("expected UpdatePoints called exactly once, got %d", ur.count())
	}

	// 记录应已置为 done
	var rec model.PointsOutbox
	if err := gdb.Where("event_id = ?", "pts:idle:1").First(&rec).Error; err != nil {
		t.Fatalf("query: %v", err)
	}
	if rec.Status != model.OutboxStatusDone {
		t.Fatalf("expected done, got %s", rec.Status)
	}
}

func TestPointsOutboxRequeueOnUserDBFailure(t *testing.T) {
	gdb := newOutboxTestDB(t)
	repo := repository.NewPointsOutboxRepository(gdb)
	// failingUserRepo 让 UpdatePoints 返回错误，验证回退 pending 且可经 relay 重试。
	failing := failingUserRepo{}
	a := NewPointsOutboxApplier(repo, failing, nil, 0)

	if err := a.AppendOutboxInTx(gdb, &model.PointsOutbox{
		UserID: "u2", Delta: 5, RefType: "task", RefID: "2",
		EventID: "pts:task:u2:2", Status: model.OutboxStatusPending,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// 第一次应用失败 → 回退 pending，不应视为成功
	if a.tryApply(context.Background(), "pts:task:u2:2", "u2", 5) {
		t.Fatal("apply should fail when userDB errors")
	}
	// relay 扫描应仍能捡起 pending 记录
	rows, err := repo.PendingOlderThan(context.Background(), time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 pending row, got %d", len(rows))
	}
}

// TestPointsOutboxConsumedMarksDone 端到端验证「事件经 MQ 消费后落库成功」：
// 业务事务写入 pending outbox 记录后，经内存 MQ 发布 user.points.adjust 事件；消费者（与 rocketmq
// 消费者走完全相同的 ApplyPointsAdjust 入口）收到事件后 Claim 并应用余额，最终把 outbox 置为 done。
// 此路径即 rocketmq 配置下的「落库」链路——修复前 rocketmq 消费者从不启动，该路径是死的。
func TestPointsOutboxConsumedMarksDone(t *testing.T) {
	gdb := newOutboxTestDB(t)
	repo := repository.NewPointsOutboxRepository(gdb)
	ur := &countingUserRepo{}

	// 内存 MQ（与生产/消费同一套接口；rocketmq 消费者调用的正是相同的 handler 路径）
	cfg := &config.MQConfig{
		Type: "memory",
		Kafka: config.KafkaConfig{Topics: config.KafkaTopicsConfig{
			UserPoints: "user-points-verify-" + t.Name(),
		}},
	}
	producer, err := mq.NewMemoryProducer(cfg, nil)
	if err != nil {
		t.Fatalf("memory producer: %v", err)
	}
	consumer, err := mq.NewMemoryConsumer(cfg, "verify-group", nil)
	if err != nil {
		t.Fatalf("memory consumer: %v", err)
	}
	a := NewPointsOutboxApplier(repo, ur, producer, 0)

	// 业务事务提交：写入 pending 记录（与库存/订单/流水同提交，即「落库」第一步）
	if err := a.AppendOutboxInTx(gdb, &model.PointsOutbox{
		UserID: "u3", Delta: 7, RefType: "shop", RefID: "3",
		EventID: "pts:shop:u3:3", Status: model.OutboxStatusPending,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// 启动消费者：事件到来 → ApplyPointsAdjust（与 rocketmq 消费者同一入口）
	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = consumer.Subscribe(bgCtx, []string{cfg.Kafka.Topics.UserPoints}, func(ctx context.Context, msg *event.Message) error {
			return a.ApplyPointsAdjust(ctx, msg)
		})
	}()
	// 等待消费者完成订阅（内存 broker 无订阅者时会丢弃消息），避免发布早于频道创建
	time.Sleep(100 * time.Millisecond)

	// 经 MQ 发布事件（模拟业务提交后的 publisher.Send）
	if err := producer.Send(context.Background(), &event.Message{
		EventType: event.EventUserPointsAdjust,
		Key:       "u3",
		EventID:   "pts:shop:u3:3",
		Timestamp: time.Now(),
		Payload: event.UserPointsAdjustPayload{
			UserID: "u3", EventID: "pts:shop:u3:3", Delta: 7, RefType: "shop", RefID: "3",
		},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// 等待消费者处理并落库：outbox 置 done 且余额更新恰好一次
	deadline := time.Now().Add(2 * time.Second)
	for {
		var rec model.PointsOutbox
		if err := gdb.Where("event_id = ?", "pts:shop:u3:3").First(&rec).Error; err == nil && rec.Status == model.OutboxStatusDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("outbox not marked done within timeout (MQ consumer path failed to land)")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ur.count() != 1 {
		t.Fatalf("expected UpdatePoints called exactly once, got %d", ur.count())
	}
}

// spyProducer 记录 Send 调用，其余方法空实现，用于验证 ApplyAsync 的池满降级分支。
type spyProducer struct {
	mu    sync.Mutex
	sends int
}

func (s *spyProducer) Send(ctx context.Context, msg *event.Message) error {
	s.mu.Lock()
	s.sends++
	s.mu.Unlock()
	return nil
}
func (s *spyProducer) SendSync(ctx context.Context, msg *event.Message) error     { return nil }
func (s *spyProducer) SendBatch(ctx context.Context, msgs []*event.Message) error { return nil }
func (s *spyProducer) Close() error                                               { return nil }

// TestPointsOutboxApplyAsyncOffRequestPath 验证 §3.48 第3项：ApplyAsync 把写余额工作派发到
// 有界 worker 池后「立即返回」，不再阻塞请求关键路径；余额最终由后台 worker 恰一次入账且 outbox 置 done。
func TestPointsOutboxApplyAsyncOffRequestPath(t *testing.T) {
	gdb := newOutboxTestDB(t)
	repo := repository.NewPointsOutboxRepository(gdb)
	ur := &countingUserRepo{}
	a := NewPointsOutboxApplier(repo, ur, nil, 1) // 并发度=1，确定性观察后台 worker

	// 业务事务提交：写入 pending 记录（与真实链路一致）
	if err := a.AppendOutboxInTx(gdb, &model.PointsOutbox{
		UserID: "u9", Delta: 3, RefType: "idle", RefID: "9",
		EventID: "pts:idle:9", Status: model.OutboxStatusPending,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// ApplyAsync 应立即返回，此刻余额尚未被后台 worker 同步更新（证明已移出请求关键路径）。
	a.ApplyAsync(context.Background(), &model.PointsOutbox{
		EventID: "pts:idle:9", UserID: "u9", Delta: 3, RefType: "idle", RefID: "9",
	})
	if ur.count() != 0 {
		t.Fatalf("ApplyAsync must not apply synchronously (off request path), got n=%d", ur.count())
	}

	// 后台 worker 应在短时间内完成；余额恰好一次入账，outbox 置 done（最终一致）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		var rec model.PointsOutbox
		_ = gdb.Where("event_id = ?", "pts:idle:9").First(&rec).Error
		if ur.count() >= 1 && rec.Status == model.OutboxStatusDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("async apply not completed: n=%d status=%s", ur.count(), rec.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ur.count() != 1 {
		t.Fatalf("expected exactly-once async apply, got n=%d", ur.count())
	}
}

// TestPointsOutboxApplyAsyncFallbackWhenPoolFull 验证池满时不阻塞请求、降级为发布 Kafka 事件
// （由消费者异步应用），且不直接调用 UpdatePoints；outbox 已持久化，relay 也会兜底，最终一致不受影响。
func TestPointsOutboxApplyAsyncFallbackWhenPoolFull(t *testing.T) {
	gdb := newOutboxTestDB(t)
	repo := repository.NewPointsOutboxRepository(gdb)
	ur := &countingUserRepo{}
	sp := &spyProducer{}
	a := NewPointsOutboxApplier(repo, ur, sp, 1)

	// 预先占满信号量（并发度=1），强制 ApplyAsync 走 default 分支（池满降级）。
	a.sem <- struct{}{}
	defer func() { <-a.sem }()

	a.ApplyAsync(context.Background(), &model.PointsOutbox{
		EventID: "pts:idle:fb", UserID: "ufb", Delta: 1, RefType: "idle", RefID: "fb",
	})

	if sp.sends != 1 {
		t.Fatalf("expected fallback publish on pool-full, got sends=%d", sp.sends)
	}
	if ur.count() != 0 {
		t.Fatalf("pool-full fallback must not call UpdatePoints synchronously, got n=%d", ur.count())
	}
}
