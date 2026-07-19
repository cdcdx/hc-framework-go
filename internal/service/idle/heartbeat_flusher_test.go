package idle

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// newFlusherRepo 构造启用批量落库的仓库（每 Pod 聚合 + 定时 flush 的真实落库路径）。
func newFlusherRepo(t *testing.T, rw *db.RWDB) *repository.IdleRepository {
	t.Helper()
	mgr := newTestCacheManager(&mockL2{})
	return repository.NewIdleRepositoryWithCache(rw, mgr, 1, true)
}

// TestHeartbeatFlusherBatch 验证心跳聚合落库：内存聚合去重 + 批量 UPDATE last_heartbeat_at。
func TestHeartbeatFlusherBatch(t *testing.T) {
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := gdb.AutoMigrate(&model.IdleRecord{}, &model.PointsTransaction{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rw := db.NewRWDBFromGORM(gdb)
	repo := newFlusherRepo(t, rw)
	flusher := NewHeartbeatFlusher(repo, time.Second, zap.NewNop())

	svc := &IdleService{businessDB: rw}
	insertActiveRecord(t, svc, "uF1", "dF1", time.Now())
	insertActiveRecord(t, svc, "uF2", "dF2", time.Now())

	// 同一会话多次心跳 → 聚合去重，保留最新值
	t1 := time.Now().Add(-3 * time.Minute).Truncate(time.Second)
	t2 := time.Now().Add(-1 * time.Minute).Truncate(time.Second)
	flusher.Record("uF1", "dF1", t1)
	flusher.Record("uF1", "dF1", t2) // 覆盖 t1
	flusher.Record("uF2", "dF2", t1)

	// 手动 flush（不依赖 ticker）
	n, err := flusher.Flush(context.Background())
	if err != nil {
		t.Fatalf("flush err: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows updated, got %d", n)
	}

	// uF1 应更新为最新 t2（去重生效）；uF2 应为 t1。用 Unix 比较规避时区/精度差异。
	rec1 := queryIdleRecord(t, svc, "uF1", "dF1")
	if rec1.LastHeartbeatAt == nil || rec1.LastHeartbeatAt.Unix() != t2.Unix() {
		t.Fatalf("uF1 last_heartbeat_at should be %v, got %v", t2, rec1.LastHeartbeatAt)
	}
	rec2 := queryIdleRecord(t, svc, "uF2", "dF2")
	if rec2.LastHeartbeatAt == nil || rec2.LastHeartbeatAt.Unix() != t1.Unix() {
		t.Fatalf("uF2 last_heartbeat_at should be %v, got %v", t1, rec2.LastHeartbeatAt)
	}

	// 二次 flush：pending 已清空 → 不应再更新任何行（返回 0，避免无意义 DB 写）
	n2, err := flusher.Flush(context.Background())
	if err != nil {
		t.Fatalf("second flush err: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second flush should update 0 rows, got %d", n2)
	}
}

// TestHeartbeatFlusherSkipsSettled 验证已结算（status!=active）会话不被批量 UPDATE 覆盖。
func TestHeartbeatFlusherSkipsSettled(t *testing.T) {
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := gdb.AutoMigrate(&model.IdleRecord{}, &model.PointsTransaction{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rw := db.NewRWDBFromGORM(gdb)
	repo := newFlusherRepo(t, rw)
	flusher := NewHeartbeatFlusher(repo, time.Second, zap.NewNop())

	svc := &IdleService{businessDB: rw}
	insertActiveRecord(t, svc, "uS", "dS", time.Now())
	// 先手动结算该会话（status=completed），last_heartbeat_at 保持旧值
	start := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
	if err := rw.Write(context.Background()).Model(&model.IdleRecord{}).
		Where("user_id = ? AND device_id = ?", "uS", "dS").
		Updates(map[string]interface{}{"status": "completed", "end_time": start, "last_heartbeat_at": start}).Error; err != nil {
		t.Fatalf("manual settle: %v", err)
	}

	// 聚合一条该会话的「新心跳」并 flush：应被 WHERE status='active' 跳过
	hb := time.Now().Truncate(time.Second)
	flusher.Record("uS", "dS", hb)
	n, err := flusher.Flush(context.Background())
	if err != nil {
		t.Fatalf("flush err: %v", err)
	}
	if n != 0 {
		t.Fatalf("settled session should NOT be updated, got %d", n)
	}
	rec := queryIdleRecord(t, svc, "uS", "dS")
	if rec.LastHeartbeatAt == nil || rec.LastHeartbeatAt.Unix() != start.Unix() {
		t.Fatalf("settled session last_heartbeat_at should remain %v, got %v", start, rec.LastHeartbeatAt)
	}
}

// TestHeartbeatFlusherStartStop 验证定时 flush 由 ticker 驱动，且 Stop 排空剩余心跳。
func TestHeartbeatFlusherStartStop(t *testing.T) {
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := gdb.AutoMigrate(&model.IdleRecord{}, &model.PointsTransaction{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rw := db.NewRWDBFromGORM(gdb)
	repo := newFlusherRepo(t, rw)
	flusher := NewHeartbeatFlusher(repo, 50*time.Millisecond, zap.NewNop())

	svc := &IdleService{businessDB: rw}
	insertActiveRecord(t, svc, "uT", "dT", time.Now())

	// 记录心跳但尚未 flush
	hb := time.Now().Truncate(time.Second)
	flusher.Record("uT", "dT", hb)

	flusher.Start(context.Background())
	// 等待 ticker 至少触发一次 flush
	time.Sleep(200 * time.Millisecond)
	flusher.Stop(context.Background())

	rec := queryIdleRecord(t, svc, "uT", "dT")
	if rec.LastHeartbeatAt == nil || rec.LastHeartbeatAt.Unix() != hb.Unix() {
		t.Fatalf("ticker flush should persist heartbeat %v, got %v", hb, rec.LastHeartbeatAt)
	}
}
