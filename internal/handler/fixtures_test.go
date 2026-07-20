package handler

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	"github.com/cdcdx/hc-framework-go/internal/service/idle"
	"github.com/cdcdx/hc-framework-go/internal/service/shop"
	"github.com/cdcdx/hc-framework-go/internal/service/task"
	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

// ───────────── 内存假实现：让 LogService 后台写循环不 panic ─────────────

type fakeLogRepo struct{ n int }

func (f *fakeLogRepo) Create(_ context.Context, _ *model.AuditLog) error { f.n++; return nil }
func (f *fakeLogRepo) FindByUser(_ context.Context, _ string, _ int64, _ int) ([]model.AuditLog, error) {
	return nil, nil
}
func (f *fakeLogRepo) FindByType(_ context.Context, _ string, _, _ time.Time, _ int) ([]model.AuditLog, error) {
	return nil, nil
}
func (f *fakeLogRepo) CountByType(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeLogRepo) CountByTypeAndResult(_ context.Context, _, _ string, _, _ time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeLogRepo) Close() error            { return nil }
func (f *fakeLogRepo) SQLDB() (*sql.DB, error) { return nil, nil }

type fakeMonitorRepo struct{ n int }

func (f *fakeMonitorRepo) Record(_ context.Context, _ *model.MonitorMetric) error { f.n++; return nil }
func (f *fakeMonitorRepo) CountByType(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeMonitorRepo) SumByType(_ context.Context, _ string, _, _ time.Time) (float64, error) {
	return 0, nil
}
func (f *fakeMonitorRepo) FindByTimeRange(_ context.Context, _, _ time.Time, _ int) ([]model.MonitorMetric, error) {
	return nil, nil
}
func (f *fakeMonitorRepo) Close() error            { return nil }
func (f *fakeMonitorRepo) SQLDB() (*sql.DB, error) { return nil, nil }

// ───────────── 内存 SQLite 工具 ─────────────

func sanitizeName(s string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return re.ReplaceAllString(s, "_")
}

func openTestDB(t *testing.T, suffix string, models ...interface{}) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:mem_%s_%s?mode=memory&cache=shared", sanitizeName(t.Name()), suffix)
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if sqlDB, err := gdb.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := gdb.AutoMigrate(models...); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return gdb
}

func seedUser(t *testing.T, gdb *gorm.DB, id string, points int64) {
	t.Helper()
	u := &model.User{UserID: id, Username: id, Email: id + "@x.com", PointsBalance: points, Status: "active"}
	if err := gdb.Create(u).Error; err != nil {
		t.Fatalf("seed user %s: %v", id, err)
	}
}

// newTestShop 用内存 SQLite 搭建 ShopService（无缓存/无 MQ/无积分 Outbox）。
func newTestShop(t *testing.T) (*shop.ShopService, *gorm.DB) {
	t.Helper()
	gdb := openTestDB(t, "shop", &model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.User{}, &model.PointsOutbox{}, &model.ShopItemStockBucket{})
	businessDB := db.CreateSingleRWDB(gdb)
	userRepo := repository.NewUserRepository(gdb)
	logSvc := common.NewLogService(&fakeLogRepo{}, &fakeMonitorRepo{})
	t.Cleanup(func() { logSvc.Close() })

	svc := shop.NewShopService(&config.Config{}, userRepo, businessDB, logSvc, nil, nil, nil)
	if err := svc.SeedItems(); err != nil {
		t.Fatalf("seed items: %v", err)
	}
	return svc, gdb
}

// newTestTask 用内存 SQLite 搭建 TaskService。
func newTestTask(t *testing.T) (*task.TaskService, *gorm.DB) {
	t.Helper()
	gdb := openTestDB(t, "task", &model.Task{}, &model.UserTaskProgress{}, &model.EventDedup{}, &model.User{}, &model.PointsTransaction{}, &model.PointsOutbox{})
	businessDB := db.CreateSingleRWDB(gdb)
	userRepo := repository.NewUserRepository(gdb)
	logSvc := common.NewLogService(&fakeLogRepo{}, &fakeMonitorRepo{})
	t.Cleanup(func() { logSvc.Close() })

	svc := task.NewTaskService(&config.Config{}, userRepo, businessDB, logSvc, nil, nil)
	if err := svc.SeedTasks(); err != nil {
		t.Fatalf("seed tasks: %v", err)
	}
	return svc, gdb
}

// newTestIdle 用内存 SQLite 搭建 IdleService（无缓存/无 MQ）。
func newTestIdle(t *testing.T) (*idle.IdleService, *gorm.DB) {
	t.Helper()
	gdb := openTestDB(t, "idle", &model.IdleRecord{}, &model.User{})
	businessDB := db.CreateSingleRWDB(gdb)
	userRepo := repository.NewUserRepository(gdb)
	logSvc := common.NewLogService(&fakeLogRepo{}, &fakeMonitorRepo{})
	t.Cleanup(func() { logSvc.Close() })

	svc := idle.NewIdleService(&config.Config{}, userRepo, businessDB, logSvc, nil, nil, nil)
	return svc, gdb
}

// authedEngine 构造一个注入固定 user_id 的 gin 引擎，用于隔离 handler 逻辑（绕过真实 JWT）。
func authedEngine(userID string, register func(r *gin.Engine)) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		if userID != "" {
			c.Set("user_id", userID)
		}
		c.Next()
	})
	register(r)
	return r
}
