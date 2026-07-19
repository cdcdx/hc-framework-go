package repository

import (
	"context"
	"strings"
	"testing"

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

func TestLoginRepo_NilGuards(t *testing.T) {
	ctx := context.Background()

	nilDB := &LoginRepository{db: nil}
	assertDBError(t, nilDB.Create(ctx, &model.LoginRecord{}))

	fake := &LoginRepository{db: &gorm.DB{}}
	assertNilArgError(t, fake.Create(ctx, nil))
	if err := fake.CreateBatch(ctx, []*model.LoginRecord{nil, nil}); err != nil {
		t.Fatalf("CreateBatch all-nil: got %v, want nil", err)
	}
}

func TestLogRepo_NilGuards(t *testing.T) {
	ctx := context.Background()

	nilDB := &LogRepository{db: nil}
	assertDBError(t, nilDB.Create(ctx, &model.AuditLog{}))

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

	fake := &MonitorRepository{db: &gorm.DB{}}
	assertNilArgError(t, fake.Record(ctx, nil))
	if err := fake.RecordBatch(ctx, []*model.MonitorMetric{nil, nil}); err != nil {
		t.Fatalf("RecordBatch all-nil: got %v, want nil", err)
	}
}
