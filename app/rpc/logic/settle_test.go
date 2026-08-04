package logic

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/config"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/cdcdx/hc-framework-go/common/model"
)

// newTestSvc 构建最小 ServiceContext：sqlite in-memory + 必要表结构，不触发 logx.SetUp 副作用。
func newTestSvc(t *testing.T) *svc.ServiceContext {
	t.Helper()
	db, err := gormx.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(
		&model.User{},
		&model.IdleRecord{},
		&model.IdleDailyPoints{},
		&model.Task{},
		&model.UserTaskProgress{},
	); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	c := config.Config{}
	c.Idle.MaxActiveDevices = 5
	c.Idle.PointsPerMinute = 10
	c.Idle.DailyPointsLimit = 1000
	mgr, err := jwt.NewManager(
		"HS256",
		"test-key-for-unit-testing-only-32b",
		"", "",
		"test",
		1*time.Hour, 24*time.Hour,
	)
	if err != nil {
		t.Fatalf("new jwt manager: %v", err)
	}
	return &svc.ServiceContext{Config: c, Db: db, JwtMgr: mgr}
}

func seedUser(t *testing.T, svcCtx *svc.ServiceContext, userID string, balance int64) {
	t.Helper()
	u := model.User{
		UserID:        userID,
		Username:      userID,
		Email:         userID + "@x.com",
		PointsBalance: balance,
		Status:        "active",
	}
	if err := svcCtx.Db.Create(&u).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}
}

func TestAccumulateDaily_FirstInsert(t *testing.T) {
	svcCtx := newTestSvc(t)
	if err := svc.AccumulateDaily(context.Background(), svcCtx, "u1", "2026-08-04", 100); err != nil {
		t.Fatalf("accumulateDaily first: %v", err)
	}
	var dp model.IdleDailyPoints
	if err := svcCtx.Db.Where("user_id=? AND day=?", "u1", "2026-08-04").First(&dp).Error; err != nil {
		t.Fatalf("query daily: %v", err)
	}
	if dp.Total != 100 {
		t.Errorf("Total = %d, want 100", dp.Total)
	}
}

func TestAccumulateDaily_IncrementWithinLimit(t *testing.T) {
	svcCtx := newTestSvc(t)
	_ = svc.AccumulateDaily(context.Background(), svcCtx, "u1", "2026-08-04", 100)
	if err := svc.AccumulateDaily(context.Background(), svcCtx, "u1", "2026-08-04", 200); err != nil {
		t.Fatalf("second accumulate: %v", err)
	}
	var dp model.IdleDailyPoints
	_ = svcCtx.Db.Where("user_id=? AND day=?", "u1", "2026-08-04").First(&dp)
	if dp.Total != 300 {
		t.Errorf("Total = %d, want 300", dp.Total)
	}
}

func TestAccumulateDaily_ExceedsDailyLimit(t *testing.T) {
	svcCtx := newTestSvc(t)
	svcCtx.Config.Idle.DailyPointsLimit = 250
	if err := svc.AccumulateDaily(context.Background(), svcCtx, "u1", "2026-08-04", 100); err != nil {
		t.Fatalf("first accumulate: %v", err)
	}
	// 累计 100+200 = 300 > 250 => 拦截
	err := svc.AccumulateDaily(context.Background(), svcCtx, "u1", "2026-08-04", 200)
	if err == nil {
		t.Fatal("expected CodeDailyPointsLimit, got nil")
	}
	if errorx.Code(err) != errorx.CodeDailyPointsLimit {
		t.Errorf("code = %d, want %d", errorx.Code(err), errorx.CodeDailyPointsLimit)
	}
	// 被拦截后不应累加
	var dp model.IdleDailyPoints
	_ = svcCtx.Db.Where("user_id=? AND day=?", "u1", "2026-08-04").First(&dp)
	if dp.Total != 100 {
		t.Errorf("Total = %d, want 100 (unchanged after reject)", dp.Total)
	}
}

func TestAccumulateDaily_SingleInsertExceedsLimit(t *testing.T) {
	svcCtx := newTestSvc(t)
	svcCtx.Config.Idle.DailyPointsLimit = 50
	err := svc.AccumulateDaily(context.Background(), svcCtx, "u1", "2026-08-04", 100)
	if err == nil {
		t.Fatal("expected CodeDailyPointsLimit for single over-limit insert")
	}
	if errorx.Code(err) != errorx.CodeDailyPointsLimit {
		t.Errorf("code = %d, want %d", errorx.Code(err), errorx.CodeDailyPointsLimit)
	}
}

func TestSettle_CalculatesPointsAndPersists(t *testing.T) {
	svcCtx := newTestSvc(t)
	seedUser(t, svcCtx, "u1", 0)

	start := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	rec := &model.IdleRecord{UserID: "u1", DeviceID: "d1", StartTime: start, Status: model.IdleStatusActive}
	if err := svcCtx.Db.Create(rec).Error; err != nil {
		t.Fatalf("create rec: %v", err)
	}

	// 结算到 start+30min => 30 * 10 = 300 分
	until := start.Add(30 * time.Minute)
	got, err := settle(context.Background(), svcCtx, rec, until, model.IdleStatusCompleted)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got != 300 {
		t.Errorf("points = %d, want 300", got)
	}

	// 用户积分已入账
	var u model.User
	_ = svcCtx.Db.Where("user_id=?", "u1").First(&u)
	if u.PointsBalance != 300 {
		t.Errorf("balance = %d, want 300", u.PointsBalance)
	}
	// 每日汇总已记录
	var dp model.IdleDailyPoints
	_ = svcCtx.Db.Where("user_id=? AND day=?", "u1", "2026-08-04").First(&dp)
	if dp.Total != 300 {
		t.Errorf("daily total = %d, want 300", dp.Total)
	}
	// 记录已更新
	var rec2 model.IdleRecord
	_ = svcCtx.Db.Where("id=?", rec.ID).First(&rec2)
	if rec2.PointsEarned != 300 {
		t.Errorf("points_earned = %d, want 300", rec2.PointsEarned)
	}
	if rec2.Status != model.IdleStatusCompleted {
		t.Errorf("status = %q, want completed", rec2.Status)
	}
	if rec2.DurationSeconds != 1800 {
		t.Errorf("duration = %d, want 1800", rec2.DurationSeconds)
	}
}

func TestSettle_NegativeDurationClampedToZero(t *testing.T) {
	svcCtx := newTestSvc(t)
	seedUser(t, svcCtx, "u1", 0)

	start := time.Date(2026, 8, 4, 1, 0, 0, 0, time.UTC)
	rec := &model.IdleRecord{UserID: "u1", DeviceID: "d1", StartTime: start, Status: model.IdleStatusActive}
	_ = svcCtx.Db.Create(rec)

	// until 早于 start（异常时钟）→ durSec<0 → 0 分
	got, err := settle(context.Background(), svcCtx, rec, start.Add(-10*time.Minute), model.IdleStatusCompleted)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got != 0 {
		t.Errorf("points = %d, want 0", got)
	}
}

func TestSettle_BlockedByDailyLimit(t *testing.T) {
	svcCtx := newTestSvc(t)
	svcCtx.Config.Idle.DailyPointsLimit = 100
	seedUser(t, svcCtx, "u1", 0)

	start := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	rec := &model.IdleRecord{UserID: "u1", DeviceID: "d1", StartTime: start, Status: model.IdleStatusActive}
	_ = svcCtx.Db.Create(rec)

	// 30min => 300 > 100 上限，应拦截，积分不入账
	until := start.Add(30 * time.Minute)
	_, err := settle(context.Background(), svcCtx, rec, until, model.IdleStatusCompleted)
	if err == nil {
		t.Fatal("expected settle blocked by daily limit")
	}
	if errorx.Code(err) != errorx.CodeDailyPointsLimit {
		t.Errorf("code = %d, want %d", errorx.Code(err), errorx.CodeDailyPointsLimit)
	}
	var u model.User
	_ = svcCtx.Db.Where("user_id=?", "u1").First(&u)
	if u.PointsBalance != 0 {
		t.Errorf("balance = %d, want 0 (no credit on blocked settle)", u.PointsBalance)
	}
}
