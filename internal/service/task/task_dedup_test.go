package task

import (
	"context"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// newTestTaskService 构造一个仅依赖内存 SQLite 的 TaskService，
// 用于验证事件驱动进度更新的幂等性（ApplyEventProgress 只用到 businessDB）。
func newTestTaskService(t *testing.T) (*TaskService, *gorm.DB) {
	t.Helper()
	// 每个测试独立的具名内存库（cache=shared 让同库多连接共享，避免连接池看到不同库）
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if sqlDB, dErr := gdb.DB(); dErr == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := gdb.AutoMigrate(&model.Task{}, &model.UserTaskProgress{}, &model.EventDedup{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// 播种测试任务（与 SeedTasks 中的挂机/兑换 key 一致）
	tasks := []model.Task{
		{TaskType: "daily", TaskKey: model.TaskKeyDailyIdle30, TaskName: "挂机满30分钟", TargetValue: 30, IsActive: true},
		{TaskType: "weekly", TaskKey: model.TaskKeyWeeklyIdle300, TaskName: "累计挂机300分钟", TargetValue: 300, IsActive: true},
		{TaskType: "daily", TaskKey: model.TaskKeyDailyRedeem1, TaskName: "完成1次兑换", TargetValue: 1, IsActive: true},
		{TaskType: "weekly", TaskKey: model.TaskKeyWeeklyRedeem3, TaskName: "累计兑换3次", TargetValue: 3, IsActive: true},
		{TaskType: "achievement", TaskKey: model.TaskKeyAchieveRedeem100, TaskName: "兑换100次", TargetValue: 100, IsActive: true},
	}
	if err := gdb.Create(&tasks).Error; err != nil {
		t.Fatalf("seed tasks: %v", err)
	}
	rw := db.NewRWDBFromGORM(gdb)
	return &TaskService{businessDB: rw}, gdb
}

// progressOf 读取某 task_key 在指定 user 当前周期的累计进度（不存在返回 0）。
func progressOf(t *testing.T, gdb *gorm.DB, userID, taskKey string) int {
	t.Helper()
	var task model.Task
	if err := gdb.Where("task_key = ?", taskKey).First(&task).Error; err != nil {
		t.Fatalf("find task %s: %v", taskKey, err)
	}
	// 复用生产逻辑的周期计算需要 repository 包，这里直接按类型推导测试期望不便，
	// 故直接取该 task 的唯一进度行（测试内每类只播种一个用户一个周期）。
	var p model.UserTaskProgress
	err := gdb.Where("user_id = ? AND task_id = ?", userID, task.ID).First(&p).Error
	if err == gorm.ErrRecordNotFound {
		return 0
	}
	if err != nil {
		t.Fatalf("find progress %s: %v", taskKey, err)
	}
	return p.CurrentProgress
}

// TestApplyEventProgress_Idempotent 验证：同一 event_id 重复消费只累加一次。
func TestApplyEventProgress_Idempotent(t *testing.T) {
	svc, gdb := newTestTaskService(t)
	ctx := context.Background()

	msg := &event.Message{
		EventType: event.EventIdleSettled,
		EventID:   "idle:1001",
		Payload: event.IdleSettledPayload{
			UserID:          "u1",
			IdleRecordID:    1001,
			DurationSeconds: 1800, // 30 分钟
		},
	}

	// 连续消费 3 次（模拟 at-least-once 重投）
	for i := 0; i < 3; i++ {
		if err := svc.ApplyEventProgress(ctx, msg); err != nil {
			t.Fatalf("apply #%d: %v", i, err)
		}
	}

	if got := progressOf(t, gdb, "u1", "daily_idle_30"); got != 30 {
		t.Fatalf("daily_idle_30 progress = %d, want 30 (idempotent)", got)
	}
	if got := progressOf(t, gdb, "u1", "weekly_idle_300"); got != 30 {
		t.Fatalf("weekly_idle_300 progress = %d, want 30 (idempotent)", got)
	}

	// event_dedup 应只有一行
	var cnt int64
	gdb.Model(&model.EventDedup{}).Count(&cnt)
	if cnt != 1 {
		t.Fatalf("event_dedup rows = %d, want 1", cnt)
	}
}

// TestApplyEventProgress_DistinctEvents 验证：不同 event_id 各自累加。
func TestApplyEventProgress_DistinctEvents(t *testing.T) {
	svc, gdb := newTestTaskService(t)
	ctx := context.Background()

	// 两次不同订单的兑换事件
	for _, orderID := range []int64{1, 2} {
		msg := &event.Message{
			EventType: event.EventShopRedeemed,
			EventID:   "",
			Payload: event.ShopRedeemedPayload{
				UserID:  "u2",
				OrderID: orderID,
			},
		}
		if err := svc.ApplyEventProgress(ctx, msg); err != nil {
			t.Fatalf("apply order %d: %v", orderID, err)
		}
	}

	if got := progressOf(t, gdb, "u2", "weekly_redeem_3"); got != 2 {
		t.Fatalf("weekly_redeem_3 progress = %d, want 2", got)
	}

	// 再重放订单 1（event_id 派生自 OrderID，应被去重）
	msg := &event.Message{
		EventType: event.EventShopRedeemed,
		Payload:   event.ShopRedeemedPayload{UserID: "u2", OrderID: 1},
	}
	if err := svc.ApplyEventProgress(ctx, msg); err != nil {
		t.Fatalf("replay order 1: %v", err)
	}
	if got := progressOf(t, gdb, "u2", "weekly_redeem_3"); got != 2 {
		t.Fatalf("weekly_redeem_3 after replay = %d, want 2 (dedup by order id)", got)
	}
}

// TestCleanupDedup 验证：清理只删除 retention 之前的记录。
func TestCleanupDedup(t *testing.T) {
	svc, gdb := newTestTaskService(t)
	ctx := context.Background()

	// 造两条记录：一条“旧”（10 天前），一条“新”（刚才）
	old := &model.EventDedup{EventID: "old:1", Source: "test", CreatedAt: time.Now().Add(-10 * 24 * time.Hour)}
	fresh := &model.EventDedup{EventID: "new:1", Source: "test", CreatedAt: time.Now()}
	if err := gdb.Create(old).Error; err != nil {
		t.Fatalf("create old: %v", err)
	}
	if err := gdb.Create(fresh).Error; err != nil {
		t.Fatalf("create fresh: %v", err)
	}

	n, err := svc.CleanupDedup(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if n != 1 {
		t.Fatalf("cleaned = %d, want 1", n)
	}

	var cnt int64
	gdb.Model(&model.EventDedup{}).Count(&cnt)
	if cnt != 1 {
		t.Fatalf("remaining rows = %d, want 1", cnt)
	}
}
