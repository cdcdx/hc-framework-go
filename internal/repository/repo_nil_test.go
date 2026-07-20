package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// 这些测试验证 GORM 三个 repo 的写路径在「依赖 db 为 nil」或「入参为 nil 指针」时
// 返回明确 error 而非 panic（13 §3.40 ①）。无需真实 DB：
//   - db 未初始化：守卫在触达 gorm 前返回；
//   - 入参 nil 记录：守卫在触达 gorm 前返回；
//   - 批量全 nil 元素：过滤后为空，直接返回 nil（不触达 gorm）。
// 注：零值 gorm.DB{} 的方法本身会 panic（内部 Config 为 nil），故混合批量（含非 nil 元素）
// 需真实 DB 才能验证，此处不覆盖；all-nil 批量已证明过滤后不触达 db。

func assertDBError(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "db not initialized") {
		t.Fatalf("got %v, want db-not-initialized error", err)
	}
}

func assertNilArgError(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("got %v, want nil-arg error", err)
	}
}

func TestLogRepo_NilGuards(t *testing.T) {
	ctx := context.Background()

	nilDB := &LogRepository{db: nil}
	assertDBError(t, nilDB.Create(ctx, &model.AuditLog{}))
	// 读路径同样需要 nil-db 守卫，否则触达 gorm 会 panic（13 §3.40 ①）
	_, err := nilDB.CountByType(ctx, "login", time.Time{}, time.Time{})
	assertDBError(t, err)
	_, err = nilDB.CountByTypeAndResult(ctx, "login", "success", time.Time{}, time.Time{})
	assertDBError(t, err)
	_, err = nilDB.FindByType(ctx, "login", time.Time{}, time.Time{}, 10)
	assertDBError(t, err)
	_, err = nilDB.FindByUser(ctx, "u1", 0, 10)
	assertDBError(t, err)

	fake := &LogRepository{db: &gorm.DB{}}
	assertNilArgError(t, fake.Create(ctx, nil))
	if err := fake.CreateBatch(ctx, []*model.AuditLog{nil, nil}); err != nil {
		t.Fatalf("CreateBatch all-nil: got %v, want nil", err)
	}
}

func TestMonitorRepo_NilGuards(t *testing.T) {
	ctx := context.Background()

	nilDB := &MonitorRepository{db: nil}
	assertDBError(t, nilDB.Record(ctx, &model.MonitorMetric{}))
	_, err := nilDB.CountByType(ctx, "login_count", time.Time{}, time.Time{})
	assertDBError(t, err)
	_, err = nilDB.SumByType(ctx, "login_count", time.Time{}, time.Time{})
	assertDBError(t, err)
	_, err = nilDB.FindByTimeRange(ctx, time.Time{}, time.Time{}, 10)
	assertDBError(t, err)

	fake := &MonitorRepository{db: &gorm.DB{}}
	assertNilArgError(t, fake.Record(ctx, nil))
	if err := fake.RecordBatch(ctx, []*model.MonitorMetric{nil, nil}); err != nil {
		t.Fatalf("RecordBatch all-nil: got %v, want nil", err)
	}
}

// TestRepo_NilCloseAndSQLDB 验证 db 为 nil 时 Close/SQLDB 返回明确结果而非 panic。
// Close 属释放资源语义：无连接可释放，应安全返回 nil（不触发 gorm panic）；
// SQLDB 契约为「无 *sql.DB 时返回 error」，故应返回 db-not-initialized 而非触达 r.db.DB() panic。
func TestRepo_NilCloseAndSQLDB(t *testing.T) {
	nilLog := &LogRepository{db: nil}
	if err := nilLog.Close(); err != nil {
		t.Fatalf("Log Close on nil db: got %v, want nil", err)
	}
	if _, err := nilLog.SQLDB(); err == nil || !strings.Contains(err.Error(), "db not initialized") {
		t.Fatalf("Log SQLDB on nil db: got %v, want db-not-initialized error", err)
	}

	nilMonitor := &MonitorRepository{db: nil}
	if err := nilMonitor.Close(); err != nil {
		t.Fatalf("Monitor Close on nil db: got %v, want nil", err)
	}
	if _, err := nilMonitor.SQLDB(); err == nil || !strings.Contains(err.Error(), "db not initialized") {
		t.Fatalf("Monitor SQLDB on nil db: got %v, want db-not-initialized error", err)
	}

	nilUser := &gormUserRepository{db: nil}
	if err := nilUser.Close(); err != nil {
		t.Fatalf("User Close on nil db: got %v, want nil", err)
	}
	if _, err := nilUser.SQLDB(); err == nil || !strings.Contains(err.Error(), "db not initialized") {
		t.Fatalf("User SQLDB on nil db: got %v, want db-not-initialized error", err)
	}
	if err := nilUser.AutoMigrate(); err == nil || !strings.Contains(err.Error(), "db not initialized") {
		t.Fatalf("User AutoMigrate on nil db: got %v, want db-not-initialized error", err)
	}
}

// TestEventDedup_DeleteBeforeNilRW 验证 rw 为 nil 时 DeleteBefore 安全返回（该仓储文档明确 rw 可 nil）。
func TestEventDedup_DeleteBeforeNilRW(t *testing.T) {
	repo := NewEventDedupRepository(nil)
	n, err := repo.DeleteBefore(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("DeleteBefore on nil rw: got %v, want nil", err)
	}
	if n != 0 {
		t.Fatalf("DeleteBefore on nil rw: deleted %d, want 0", n)
	}
}

// TestPointsOutbox_NilDB 验证 *gorm.DB 仓储在 db 为 nil 时各方法返回明确 error 而非 panic。
func TestPointsOutbox_NilDB(t *testing.T) {
	repo := &PointsOutboxRepository{db: nil}
	assertDBError(t, repo.AutoMigrate())
	if claimed, err := repo.Claim(context.Background(), "evt"); err == nil || !strings.Contains(err.Error(), "db not initialized") || claimed {
		t.Fatalf("Claim on nil db: claimed=%v err=%v, want false + db-not-initialized error", claimed, err)
	}
	assertDBError(t, repo.MarkDone(context.Background(), "evt"))
	assertDBError(t, repo.Requeue(context.Background(), "evt"))
	if rows, err := repo.PendingOlderThan(context.Background(), time.Now(), 10); err == nil || !strings.Contains(err.Error(), "db not initialized") || rows != nil {
		t.Fatalf("PendingOlderThan on nil db: rows=%v err=%v, want nil + db-not-initialized error", rows, err)
	}
}
