package idle

import (
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"

	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// buildFallbackServiceWithDB 构造「Redis 不可用（L2=nil）」的 IdleService，用于集成验证降级路径：
//   - persistEnabled=true：启用心跳批量落库（不逐次 UPDATE，改经 hbFlusher 聚合按需 flush）。
//   - persistEnabled=false：保持旧「逐次 UPDATE」兜底（向后兼容）。
//
// L2=nil → IdleRepository.heartbeatRedisEnabled()=false → TouchHeartbeat 走降级分支。
func buildFallbackServiceWithDB(t *testing.T, cfg *config.Config, persistEnabled bool) *IdleService {
	t.Helper()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := gdb.AutoMigrate(&model.IdleRecord{}, &model.PointsTransaction{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rw := db.NewRWDBFromGORM(gdb)
	// L2=nil → heartbeatRedisEnabled()=false → 降级路径（不写 Redis 心跳标记，找回源 DB）。
	// cacheMgr 非 nil（带 strategy）使 FindActiveByDevice 经三级缓存直查 DB 而不 panic。
	mgr := newTestCacheManager(nil)
	repo := repository.NewIdleRepositoryWithCache(rw, mgr, cfg.Idle.ActiveSetShards, persistEnabled)
	svc := &IdleService{
		cfg:        cfg,
		idleRepo:   repo,
		userRepo:   noopUserRepo{},
		shopRepo:   repository.NewShopRepositoryWithCache(rw, mgr),
		businessDB: rw,
		cacheMgr:   mgr,
		logSvc:     common.NewLogService(noopLogRepo{}, noopMonitorRepo{}),
	}
	if persistEnabled {
		svc.hbFlusher = NewHeartbeatFlusher(repo, cfg.Idle.HeartbeatPersistIntervalOrDefault(), zap.NewNop())
	}
	return svc
}

// TestHeartbeatFlushWithDBFallback 集成验证：Redis 不可用（降级路径）下，高频心跳不逐次写 DB，
// 但经 hbFlusher 聚合后按需批量落库 last_heartbeat_at（Record 仍生效）。
func TestHeartbeatFlushWithDBFallback(t *testing.T) {
	cfg := baseIdleCfg()
	cfg.Idle.HeartbeatPersistInterval = time.Second
	svc := buildFallbackServiceWithDB(t, cfg, true)

	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	insertActiveRecord(t, svc, "uFB", "dFB", start)

	// 模拟高频心跳：多次调用 Heartbeat（降级路径 TouchHeartbeat 应 return nil，无逐次 UPDATE）。
	for i := 0; i < 3; i++ {
		if err := svc.Heartbeat(context.Background(), "uFB", "dFB"); err != nil {
			t.Fatalf("Heartbeat (fallback) #%d err: %v", i, err)
		}
	}

	// flush 前：last_heartbeat_at 仍为初始 start（证明降级路径未逐次 UPDATE DB last_heartbeat_at）。
	before := queryIdleRecord(t, svc, "uFB", "dFB")
	if before.LastHeartbeatAt == nil || !before.LastHeartbeatAt.Equal(start) {
		t.Fatalf("before flush, last_heartbeat_at should remain %v, got %v", start, before.LastHeartbeatAt)
	}

	// 按需批量落库：聚合心跳时间写入 DB（Record 仍生效，避免丢失近似心跳）。
	n, err := svc.FlushHeartbeats(context.Background())
	if err != nil {
		t.Fatalf("FlushHeartbeats err: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row updated by flush, got %d", n)
	}
	after := queryIdleRecord(t, svc, "uFB", "dFB")
	if after.LastHeartbeatAt == nil {
		t.Fatal("flush should persist last_heartbeat_at (Record still effective under fallback)")
	}
	if !after.LastHeartbeatAt.After(start) {
		t.Fatalf("last_heartbeat_at should be advanced by flush, got %v (start=%v)", after.LastHeartbeatAt, start)
	}
	if time.Since(*after.LastHeartbeatAt) > 5*time.Second {
		t.Fatalf("last_heartbeat_at should be recent (approx now), got %v", after.LastHeartbeatAt)
	}

	// 二次 flush：pending 已清空 → 不再有 UPDATE（无冗余 DB 写）。
	n2, err := svc.FlushHeartbeats(context.Background())
	if err != nil {
		t.Fatalf("second FlushHeartbeats err: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second flush should update 0 rows, got %d", n2)
	}
}

// TestHeartbeatFallbackPersistDisabledKeepsOncePerBeat 对比组：降级路径且未启用批量落库时，
// 保持旧「逐次 UPDATE」兜底（每次心跳都写 DB last_heartbeat_at），确认改动向后兼容。
func TestHeartbeatFallbackPersistDisabledKeepsOncePerBeat(t *testing.T) {
	cfg := baseIdleCfg()
	svc := buildFallbackServiceWithDB(t, cfg, false)

	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	insertActiveRecord(t, svc, "uFD", "dFD", start)

	// 未启用批量落库：每次心跳应逐次 UPDATE（旧兜底行为）。
	if err := svc.Heartbeat(context.Background(), "uFD", "dFD"); err != nil {
		t.Fatalf("Heartbeat (fallback, persist disabled) err: %v", err)
	}
	after := queryIdleRecord(t, svc, "uFD", "dFD")
	if after.LastHeartbeatAt == nil {
		t.Fatal("persist disabled fallback should UPDATE last_heartbeat_at each beat")
	}
	if !after.LastHeartbeatAt.After(start) {
		t.Fatalf("last_heartbeat_at should advance after single beat, got %v (start=%v)", after.LastHeartbeatAt, start)
	}
}
