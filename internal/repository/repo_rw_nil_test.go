package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// assertRWError 断言 nil-rw 场景下方法返回「rw not initialized」错误而非 panic。
func assertRWError(t *testing.T, err error, method string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "rw not initialized") {
		t.Errorf("%s on nil rw: got %v, want rw-not-initialized error", method, err)
	}
}

// TestIdleRepo_NilRW 验证 idle 仓储所有 DB 路径方法在 rw 为 nil 时返回明确 error 而非 panic。
func TestIdleRepo_NilRW(t *testing.T) {
	r := NewIdleRepository(nil)
	ctx := context.Background()
	var err error

	err = r.Create(ctx, &model.IdleRecord{})
	assertRWError(t, err, "Create")
	_, err = r.FindActive(ctx, "u")
	assertRWError(t, err, "FindActive")
	_, err = r.FindActiveByDevice(ctx, "u", "d")
	assertRWError(t, err, "FindActiveByDevice")
	_, err = r.FindByID(ctx, 1)
	assertRWError(t, err, "FindByID")
	_, err = r.FindActiveAll(ctx, "u")
	assertRWError(t, err, "FindActiveAll")
	_, err = r.CountActive(ctx, "u")
	assertRWError(t, err, "CountActive")
	err = r.Update(ctx, &model.IdleRecord{})
	assertRWError(t, err, "Update")
	_, err = r.FindAllActive(ctx)
	assertRWError(t, err, "FindAllActive")
	_, err = r.FindStaleActive(ctx, time.Now())
	assertRWError(t, err, "FindStaleActive")
	_, err = r.BatchUpdateHeartbeat(ctx, nil)
	assertRWError(t, err, "BatchUpdateHeartbeat")
	_, err = r.markSettled(ctx, 1, "settled", time.Now(), 0, 0)
	assertRWError(t, err, "markSettled")
	err = r.UpsertDailyPoints(ctx, "u", "2026-01-01", 1)
	assertRWError(t, err, "UpsertDailyPoints")
	_, err = r.BackfillDailyPoints(ctx, "2026-01-01")
	assertRWError(t, err, "BackfillDailyPoints")
	_, err = r.FindRecords(ctx, "u", 0, 10)
	assertRWError(t, err, "FindRecords")
	err = r.AutoMigrate()
	assertRWError(t, err, "AutoMigrate")
}

// TestTaskRepo_NilRW 验证 task 仓储所有 DB 路径方法在 rw 为 nil 时返回明确 error 而非 panic。
func TestTaskRepo_NilRW(t *testing.T) {
	r := NewTaskRepository(nil)
	ctx := context.Background()
	var err error

	_, err = r.FindAll(ctx)
	assertRWError(t, err, "FindAll")
	_, err = r.FindByID(ctx, 1)
	assertRWError(t, err, "FindByID")
	_, err = r.FindProgress(ctx, "u", 1, "p")
	assertRWError(t, err, "FindProgress")
	_, err = r.FindUserProgress(ctx, "u", "p")
	assertRWError(t, err, "FindUserProgress")
	err = r.UpsertProgress(ctx, &model.UserTaskProgress{})
	assertRWError(t, err, "UpsertProgress")
	_, err = r.FindByKeys(ctx, []string{"k"})
	assertRWError(t, err, "FindByKeys")
	err = r.IncrProgress(ctx, "u", &model.Task{}, "p", 1)
	assertRWError(t, err, "IncrProgress")
	err = r.ClaimReward(ctx, "u", 1, "p")
	assertRWError(t, err, "ClaimReward")
	err = r.SeedTasks()
	assertRWError(t, err, "SeedTasks")
	_, err = r.ResetProgressByType(ctx, "daily", "p")
	assertRWError(t, err, "ResetProgressByType")
	err = r.AutoMigrate()
	assertRWError(t, err, "AutoMigrate")
}

// TestShopRepo_NilRW 验证 shop 仓储所有 DB 路径方法在 rw 为 nil 时返回明确 error 而非 panic。
func TestShopRepo_NilRW(t *testing.T) {
	r := NewShopRepository(nil)
	ctx := context.Background()
	var err error

	_, err = r.FindItems(ctx, 0, 10, "")
	assertRWError(t, err, "FindItems")
	_, err = r.FindItemByID(ctx, 1)
	assertRWError(t, err, "FindItemByID")
	_, err = r.TotalStock(ctx, 1)
	assertRWError(t, err, "TotalStock")
	err = r.DeductStock(ctx, 1, 1, "u")
	assertRWError(t, err, "DeductStock")
	err = r.CreateStockBuckets(ctx, 1, 1)
	assertRWError(t, err, "CreateStockBuckets")
	err = r.BackfillStockBuckets(ctx)
	assertRWError(t, err, "BackfillStockBuckets")
	err = r.CreateOrder(ctx, &model.RedeemOrder{})
	assertRWError(t, err, "CreateOrder")
	_, err = r.FindOrders(ctx, "u", 0, 10)
	assertRWError(t, err, "FindOrders")
	_, err = r.FindOrderByID(ctx, 1)
	assertRWError(t, err, "FindOrderByID")
	err = r.CreateTransaction(ctx, &model.PointsTransaction{})
	assertRWError(t, err, "CreateTransaction")
	_, err = r.FindActivityByID(ctx, 1)
	assertRWError(t, err, "FindActivityByID")
	_, err = r.FindActivities(ctx, false, 10)
	assertRWError(t, err, "FindActivities")
	_, err = r.ListWarmupCandidates(ctx, time.Now(), time.Hour)
	assertRWError(t, err, "ListWarmupCandidates")
	_, err = r.TransitionStaleStatuses(ctx, time.Now())
	assertRWError(t, err, "TransitionStaleStatuses")
	err = r.CreateActivity(ctx, &model.ShopFlashActivity{})
	assertRWError(t, err, "CreateActivity")
	err = r.UpdateActivity(ctx, &model.ShopFlashActivity{})
	assertRWError(t, err, "UpdateActivity")
	_, err = r.AcquireFlashQuota(ctx, 1)
	assertRWError(t, err, "AcquireFlashQuota")
	err = r.ReleaseFlashQuota(ctx, 1)
	assertRWError(t, err, "ReleaseFlashQuota")
	_, err = r.CountUserFlashOrders(ctx, 1, "u")
	assertRWError(t, err, "CountUserFlashOrders")
	err = r.Transaction(ctx, func(tx *gorm.DB) error { return nil })
	assertRWError(t, err, "Transaction")
	err = r.SeedItems()
	assertRWError(t, err, "SeedItems")
	err = r.AutoMigrate()
	assertRWError(t, err, "AutoMigrate")
}
